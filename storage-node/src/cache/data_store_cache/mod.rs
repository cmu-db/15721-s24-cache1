use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

use crate::{error::ParpulseResult, storage_reader::StorageReaderStream};

pub mod memdisk;

#[async_trait]
pub trait DataStoreCache {
    async fn get_data_from_cache(
        &mut self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>>;

    async fn put_data_to_cache(
        &mut self,
        remote_location: String,
        data_size: usize,
        data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize>;
}
