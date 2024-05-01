pub mod blob;

use std::fs;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use log::warn;
use parpulse_client::RequestParams;
use rusqlite::{Connection, DatabaseName, OpenFlags};
use tokio::sync::{
    mpsc::{channel, Receiver},
    Mutex,
};

use crate::{
    cache::replacer::{DataStoreReplacer, ReplacerValue},
    error::ParpulseResult,
    storage_reader::{s3::S3Reader, s3_diskmock::MockS3Reader, AsyncStorageReader},
};

use self::blob::{SqliteBlob, SqliteBlobReader};

use super::{cache_key_from_request, DataStoreCache};

const SQLITE_CACHE_TABLE_NAME: &str = "parpulse_cache";
const SQLITE_CACHE_COLUMN_NAME: &str = "content";
const SQLITE_MAX_BLOB_SIZE: usize = 512 * 1024 * 1024; // 512 MB
const SQLITE_BLOB_CHANNEL_CAPACITY: usize = 5;

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
    replacer: Mutex<R>,
    sqlite_base_path: String,
    reader_buffer_size: usize,
}

impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> SqliteStoreCache<R> {
    pub fn new(
        replacer: R,
        sqlite_base_path: String,
        reader_buffer_size: usize,
    ) -> ParpulseResult<Self> {
        let db = Connection::open(&sqlite_base_path)?;
        let create_table_stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} ({} BLOB);",
            SQLITE_CACHE_TABLE_NAME, SQLITE_CACHE_COLUMN_NAME
        );
        db.execute_batch(&create_table_stmt)?;

        Ok(Self {
            replacer: Mutex::new(replacer),
            sqlite_base_path,
            reader_buffer_size,
        })
    }
}

impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> Drop
    for SqliteStoreCache<R>
{
    fn drop(&mut self) {
        if fs::metadata(&self.sqlite_base_path).is_ok() {
            fs::remove_file(self.sqlite_base_path.clone()).expect("remove sqlite db files failed");
        } else {
            warn!("sqlite db file not found: {}", self.sqlite_base_path);
        }
    }
}

#[async_trait]
impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> DataStoreCache
    for SqliteStoreCache<R>
{
    async fn get_data_from_cache(
        &self,
        request: &RequestParams,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        let remote_location = cache_key_from_request(request);
        let mut replacer = self.replacer.lock().await;
        if let Some(replacer_value) = replacer.get(&remote_location) {
            let (tx, rx) = channel(SQLITE_BLOB_CHANNEL_CAPACITY);
            let row_id = *replacer_value.as_value();
            let sqlite_base_path = self.sqlite_base_path.clone();
            let buffer_size = self.reader_buffer_size;

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

    async fn put_data_to_cache(&self, request: &RequestParams) -> ParpulseResult<usize> {
        let remote_location = cache_key_from_request(request);
        let (mut data_stream, blob_size) = {
            match request {
                RequestParams::S3((bucket, keys)) => {
                    let reader = S3Reader::new(bucket.clone(), keys.clone().to_vec()).await;
                    let data_size = reader.get_object_size().await;
                    (reader.into_stream().await?, data_size)
                }
                RequestParams::MockS3((bucket, keys)) => {
                    let reader = MockS3Reader::new(bucket.clone(), keys.clone().to_vec()).await;
                    let data_size = reader.get_object_size().await;
                    (reader.into_stream().await?, data_size)
                }
            }
        };
        let blob_size = blob_size.unwrap_or(SQLITE_MAX_BLOB_SIZE);
        let mut replacer = self.replacer.lock().await;
        let sqlite_base_path = self.sqlite_base_path.clone();
        let db = Connection::open(sqlite_base_path)?;
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
        let cache = SqliteStoreCache::new(
            replacer,
            sqlite_base_path.to_str().unwrap().to_string(),
            buffer_size,
        )
        .expect("create sqlite store cache failed");

        let bucket = "tests-text".to_string();
        let keys = vec!["what-can-i-hold-you-with".to_string()];
        let request = RequestParams::MockS3((bucket, keys));
        let bytes_written = cache
            .put_data_to_cache(&request)
            .await
            .expect("put data to cache failed");
        assert_eq!(bytes_written, 930);

        let mut rx = cache
            .get_data_from_cache(&request)
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
        assert_eq!(total_bytes_read, 930);
    }
}
