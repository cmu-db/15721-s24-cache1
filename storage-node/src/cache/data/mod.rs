use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

use crate::{error::ParpulseResult, storage_reader::StorageReaderStream};

pub mod disk;

#[async_trait]
pub trait DataStore {
    async fn read_data(&self, key: &str)
        -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>>;
    async fn write_data(&self, key: String, data: StorageReaderStream) -> ParpulseResult<usize>;
    async fn clean_data(&self, key: &str) -> ParpulseResult<()>;

    fn data_store_key(&self, remote_location: &str) -> String;
}
