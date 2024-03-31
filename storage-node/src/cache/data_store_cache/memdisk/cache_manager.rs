use tokio::sync::RwLock;

use crate::{
    cache::{data_store_cache::DataStoreCache, policy::DataStoreReplacer},
    disk::disk_manager::DiskManager,
    error::ParpulseResult,
    storage_reader::StorageReaderStream,
};

use super::data_store::disk::DiskStore;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

pub struct MemDiskStoreCache<R: DataStoreReplacer + Send + Sync> {
    disk_store: DiskStore,
    /// Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    disk_replacer: RwLock<R>,
}

impl<R: DataStoreReplacer + Send + Sync> MemDiskStoreCache<R> {
    pub fn new(disk_replacer: R, disk_base_path: String) -> Self {
        let disk_manager = DiskManager::default();
        let disk_store = DiskStore::new(disk_manager, disk_base_path);
        Self {
            disk_store,
            disk_replacer: RwLock::new(disk_replacer),
        }
    }
}

#[async_trait]
impl<R: DataStoreReplacer + Send + Sync> DataStoreCache for MemDiskStoreCache<R> {
    async fn get_data_from_cache(
        &self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        // TODO: Refine the lock.
        let mut data_store_cache = self.disk_replacer.write().await;
        match data_store_cache.get(&remote_location) {
            Some((data_store_key, _)) => {
                if let Some(data) = self.disk_store.read_data(data_store_key).await? {
                    Ok(Some(data))
                } else {
                    unreachable!()
                }
            }
            None => Ok(None),
        }
    }

    async fn put_data_to_cache(
        &self,
        remote_location: String,
        data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        let data_store_key = self.disk_store.data_store_key(&remote_location);
        // TODO: Write the data store cache first w/ `incompleted` state and update the state
        // after finishing writing into the data store.
        let data_size = self
            .disk_store
            .write_data(data_store_key.clone(), data_stream)
            .await?;
        let mut data_store_cache = self.disk_replacer.write().await;
        if !data_store_cache.put(remote_location, (data_store_key.clone(), data_size)) {
            self.disk_store.clean_data(&data_store_key).await?;
        }
        Ok(data_size)
    }
}
