use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

use crate::{error::ParpulseResult, storage_reader::StorageReaderStream};

pub mod memdisk;

#[async_trait]
pub trait DataStoreCache {
    async fn get_data_from_cache(
        &self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>>;

    /// Put data to cache. Accepts a stream of bytes and returns the number of bytes written.
    /// The data_size parameter is optional and can be used to hint the cache about the size of the data.
    /// If the data_size is not provided, the cache implementation should try to determine the size of
    /// the data.
    async fn put_data_to_cache(
        &self,
        remote_location: String,
        data_size: Option<usize>,
        data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize>;
}
