pub mod blob;

use std::fs;

use async_trait::async_trait;
use bytes::Bytes;
use rusqlite::Connection;
use tokio::sync::{mpsc::Receiver, RwLock};

use crate::{
    cache::replacer::{DataStoreReplacer, ReplacerValue},
    error::ParpulseResult,
    storage_reader::StorageReaderStream,
};

use super::DataStoreCache;

const SQLITE_CACHE_TABLE_NAME: &str = "parpulse_cache";
const SQLITE_CACHE_COLUMN_NAME: &str = "content";

pub type SqliteStoreReplacerKey = String;
pub struct SqliteStoreReplacerValue {
    pub(crate) row_id: i64,
    pub(crate) size: usize,
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
    db: Connection,
}

impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> SqliteStoreCache<R> {
    pub fn new(replacer: R, sqlite_base_path: String) -> ParpulseResult<Self> {
        let db = Connection::open(sqlite_base_path.clone())?;

        let create_table_stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} ({} BLOB);",
            SQLITE_CACHE_TABLE_NAME, SQLITE_CACHE_COLUMN_NAME
        );
        db.execute_batch(&create_table_stmt)?;

        Ok(Self {
            replacer: RwLock::new(replacer),
            sqlite_base_path,
            db,
        })
    }
}

impl<R: DataStoreReplacer<SqliteStoreReplacerKey, SqliteStoreReplacerValue>> Drop
    for SqliteStoreCache<R>
{
    fn drop(&mut self) {
        // FIXME(Yuanxin): close sqlite connection before removing the db files?
        // self.db.close().expect("close sqlite connection failed");
        fs::remove_dir_all(self.sqlite_base_path.clone()).expect("remove sqlite db files failed");
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
            let data_store_key = replacer_value.as_value();
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                let data = Bytes::from("Hello, World!");
                tx.send(Ok(data)).await.unwrap();
            });
            Ok(Some(rx))
        } else {
            Ok(None)
        }
    }

    async fn put_data_to_cache(
        &mut self,
        _remote_location: String,
        _data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        unimplemented!()
    }
}
