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
        data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize>;
}

// /// [`DataStore`] is a local store where the contents of remote objects are stored and retrieved.
// /// It is used to cache the data from the remote storage to reduce the latency of reading the
// /// data from the remote storage.
// #[async_trait]
// pub trait DataStore {
//     /// Reads data from the data store. The method returns a stream of data read from the data
//     /// store.
//     async fn read_data(&self, key: &str)
//         -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>>;

//     /// Writes data to the data store. The method accepts a stream of data to write to the data
//     /// store.
//     /// TODO: We may need to push the response writer down to the data store as well.
//     async fn write_data(&self, key: String, data: StorageReaderStream) -> ParpulseResult<usize>;

//     /// Cleans the data from the data store.
//     async fn clean_data(&self, key: &str) -> ParpulseResult<()>;

//     /// Returns the key for the data store. The key should be cached in the data store cache.
//     fn data_store_key(&self, remote_location: &str) -> String;
// }
