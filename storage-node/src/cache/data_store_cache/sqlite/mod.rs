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
    buffer_size: usize,
}

impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> SqliteStoreCache<R> {
    pub fn new(
        replacer: R,
        sqlite_base_path: String,
        buffer_size: Option<usize>,
    ) -> ParpulseResult<Self> {
        let db = Connection::open(&sqlite_base_path)?;
        let create_table_stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} ({} BLOB);",
            SQLITE_CACHE_TABLE_NAME, SQLITE_CACHE_COLUMN_NAME
        );
        let buffer_size = buffer_size.unwrap_or(SQLITE_BLOB_READER_DEFAULT_BUFFER_SIZE);
        db.execute_batch(&create_table_stmt)?;

        Ok(Self {
            replacer: RwLock::new(replacer),
            sqlite_base_path,
            buffer_size,
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
            let buffer_size = self.buffer_size;

            tokio::spawn(async move {
                let db =
                    Connection::open_with_flags(sqlite_base_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                        .unwrap();
                let mut blob_reader = SqliteBlobReader::new(&db, row_id, buffer_size).unwrap();
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
        data_size: Option<usize>,
        mut data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        let mut replacer = self.replacer.write().await;
        let sqlite_base_path = self.sqlite_base_path.clone();
        let db = Connection::open(sqlite_base_path)?;
        let blob_size = data_size.unwrap_or(SQLITE_MAX_BLOB_SIZE);
        let insert_blob_stmt = format!(
            "INSERT INTO {} ({}) VALUES (ZEROBLOB({}))",
            SQLITE_CACHE_TABLE_NAME, SQLITE_CACHE_COLUMN_NAME, blob_size
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
        let buffer_size = 100;
        let mut cache = SqliteStoreCache::new(
            replacer,
            sqlite_base_path.to_str().unwrap().to_string(),
            Some(buffer_size),
        )
        .expect("create sqlite store cache failed");

        let data1 = Bytes::from_static(
            b"What can I hold you with?
I offer you lean streets, desperate sunsets, the
moon of the jagged suburbs.
I offer you the bitterness of a man who has looked
long and long at the lonely moon.
I offer you my ancestors, my dead men, the ghosts
that living men have honoured in bronze.
I offer you whatever insight my books may hold,
whatever manliness or humour my life.
I offer you the loyalty of a man who has never
been loyal.",
        );

        let data2 = Bytes::from_static(
            b"I offer you that kernel of myself that I have saved,
somehow-the central heart that deals not
in words, traffics not with dreams, and is
untouched by time, by joy, by adversities.
I offer you the memory of a yellow rose seen at
sunset, years before you were born.
I offer you explanations of yourself, theories about
yourself, authentic and surprising news of
yourself.
I can give you my loneliness, my darkness, the
hunger of my heart; I am trying to bribe you
with uncertainty, with danger, with defeat.
",
        );
        let data_size = data1.len() + data2.len();
        let remote_location = "what-can-i-hold-you-with.txt".to_string();
        let data_stream = futures::stream::iter(vec![Ok(data1.clone()), Ok(data2.clone())]).boxed();
        let bytes_written = cache
            .put_data_to_cache(remote_location.clone(), Some(data_size), data_stream)
            .await
            .expect("put data to cache failed");
        assert_eq!(bytes_written, data1.len() + data2.len());

        let mut rx = cache
            .get_data_from_cache(remote_location.clone())
            .await
            .expect("get data from cache failed")
            .expect("data not found in cache");

        let mut result = String::new();
        let mut total_bytes_read = 0;
        while let Some(bytes) = rx.recv().await {
            let bytes = bytes.expect("read data from cache failed");
            total_bytes_read += bytes.len();
            result += &String::from_utf8(bytes.to_vec()).expect("convert bytes to string failed");
        }
        assert_eq!(total_bytes_read, data_size);

        let data = format!(
            "{}{}",
            String::from_utf8(data1.to_vec()).unwrap(),
            String::from_utf8(data2.to_vec()).unwrap()
        );
        assert_eq!(result, data);
    }
}
