pub mod blob;

use std::fs;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use rusqlite::{Connection, DatabaseName, OpenFlags};
use tokio::sync::{
    mpsc::{channel, Receiver},
    RwLock,
};

use crate::{
    cache::replacer::{DataStoreReplacer, ReplacerValue},
    error::ParpulseResult,
    storage_reader::StorageReaderStream,
};

use self::blob::{SqliteBlob, SqliteBlobReader};

use super::DataStoreCache;

const SQLITE_CACHE_TABLE_NAME: &str = "parpulse_cache";
const SQLITE_CACHE_COLUMN_NAME: &str = "content";
const SQLITE_BLOB_CHANNEL_CAPACITY: usize = 5;
const SQLITE_BLOB_READER_DEFAULT_BUFFER_SIZE: usize = 1024;

// TODO(Yuanxin):
// Get the data size in advance from S3 so that we don't need to set a constant
// value here. We can set the size of each blob to be the exact size of the S3 object.
const SQLITE_MAX_BLOB_SIZE: usize = 512 * 1024 * 1024; // 512 MB

pub type SqliteStoreReplacerKey = String;
pub struct SqliteStoreReplacerValue {
    pub(crate) row_id: i64,
    pub(crate) size: usize,
}

impl SqliteStoreReplacerValue {
    pub fn new(row_id: i64, size: usize) -> Self {
        Self { row_id, size }
    }
}

impl ReplacerValue for SqliteStoreReplacerValue {
    type Value = i64;

    fn into_value(self) -> Self::Value {
        self.row_id
    }

    fn as_value(&self) -> &Self::Value {
        &self.row_id
    }

    fn size(&self) -> usize {
        self.size
    }
}

pub struct SqliteStoreCache<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>>
{
    replacer: RwLock<R>,
    sqlite_base_path: String,
}

impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> SqliteStoreCache<R> {
    pub fn new(replacer: R, sqlite_base_path: String) -> ParpulseResult<Self> {
        let db = Connection::open(&sqlite_base_path)?;
        let create_table_stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} ({} BLOB);",
            SQLITE_CACHE_TABLE_NAME, SQLITE_CACHE_COLUMN_NAME
        );
        db.execute_batch(&create_table_stmt)?;

        Ok(Self {
            replacer: RwLock::new(replacer),
            sqlite_base_path,
        })
    }
}

impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> Drop
    for SqliteStoreCache<R>
{
    fn drop(&mut self) {
        // FIXME(Yuanxin): close sqlite connection before removing the db files?
        // self.db.close().expect("close sqlite connection failed");
        fs::remove_file(self.sqlite_base_path.clone()).expect("remove sqlite db files failed");
    }
}

#[async_trait]
impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> DataStoreCache
    for SqliteStoreCache<R>
{
    async fn get_data_from_cache(
        &mut self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        let mut replacer = self.replacer.write().await;
        if let Some(replacer_value) = replacer.get(&remote_location) {
            let (tx, rx) = channel(SQLITE_BLOB_CHANNEL_CAPACITY);
            let row_id = *replacer_value.as_value();
            let sqlite_base_path = self.sqlite_base_path.clone();

            tokio::spawn(async move {
                let db =
                    Connection::open_with_flags(sqlite_base_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                        .unwrap();
                let mut blob_reader =
                    SqliteBlobReader::new(&db, row_id, SQLITE_BLOB_READER_DEFAULT_BUFFER_SIZE)
                        .unwrap();
                while let Some(result) = blob_reader.next() {
                    match result {
                        Ok(bytes_read) => {
                            let buffer = blob_reader.buffer();
                            let bytes = Bytes::copy_from_slice(&buffer[..bytes_read]);
                            tx.send(Ok(bytes)).await.unwrap()
                        }
                        Err(err) => tx.send(Err(err)).await.unwrap(),
                    }
                }
            });
            Ok(Some(rx))
        } else {
            Ok(None)
        }
    }

    async fn put_data_to_cache(
        &mut self,
        remote_location: String,
        mut data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        let mut replacer = self.replacer.write().await;
        let sqlite_base_path = self.sqlite_base_path.clone();
        let db = Connection::open(sqlite_base_path)?;
        let insert_blob_stmt = format!(
            "INSERT INTO {} ({}) VALUES (ZEROBLOB({}))",
            SQLITE_CACHE_TABLE_NAME, SQLITE_CACHE_COLUMN_NAME, SQLITE_MAX_BLOB_SIZE
        );
        db.execute(&insert_blob_stmt, [])?;
        let blob_key = db.last_insert_rowid();
        let mut blob = SqliteBlob::new(db.blob_open(
            DatabaseName::Main,
            SQLITE_CACHE_TABLE_NAME,
            SQLITE_CACHE_COLUMN_NAME,
            blob_key,
            false,
        )?);

        let mut size = 0;
        while let Some(data) = data_stream.next().await {
            let data = data?;
            blob.write_at(&data, size)?;
            size += data.len();
        }
        replacer.put(
            remote_location,
            SqliteStoreReplacerValue::new(blob_key, size),
        );
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::cache::replacer::lru::LruReplacer;

    use super::*;

    #[tokio::test]
    async fn test_sqlite_store_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let sqlite_base_path = tmp.path().to_owned().join(Path::new("sqlite_test.db"));
        let replacer = LruReplacer::new(1024);
        let mut cache =
            SqliteStoreCache::new(replacer, sqlite_base_path.to_str().unwrap().to_string())
                .expect("create sqlite store cache failed");

        let data = Bytes::from("hello world".as_bytes());
        let remote_location = "tests-parquet/userdata1.parquet".to_string();
        let data_stream = futures::stream::iter(vec![Ok(data.clone())]).boxed();
        let bytes_written = cache
            .put_data_to_cache(remote_location.clone(), data_stream)
            .await
            .expect("put data to cache failed");
        assert_eq!(bytes_written, data.len());

        let mut rx = cache
            .get_data_from_cache(remote_location.clone())
            .await
            .expect("get data from cache failed")
            .expect("data not found in cache");
        let data = rx
            .recv()
            .await
            .expect("read data from cache failed")
            .unwrap();
        assert_eq!(data, Bytes::from("hello world".as_bytes()));
    }
}
