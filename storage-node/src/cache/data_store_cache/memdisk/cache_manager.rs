use futures::stream::StreamExt;
use tokio::sync::RwLock;

use crate::{
    cache::{
        data_store_cache::DataStoreCache,
        policy::{DataStoreReplacer, ParpulseDataStoreCacheKey},
    },
    disk::disk_manager::DiskManager,
    error::{ParpulseError, ParpulseResult},
    storage_reader::StorageReaderStream,
};

use super::data_store::{disk::DiskStore, memory::MemStore};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

/// The default maximum single file size for the memory cache.
/// If the file size exceeds this value, the file will be stored in the disk cache.
/// TODO(lanlou): make this value configurable.
pub const DEFAULT_MEM_CACHE_MAX_FILE_SIZE: usize = 1024 * 512;

pub struct MemDiskStoreCache<R: DataStoreReplacer + Send + Sync> {
    disk_store: DiskStore,
    mem_store: Option<MemStore>,
    /// Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    disk_replacer: RwLock<R>,
    mem_replacer: Option<RwLock<R>>,
}

impl<R: DataStoreReplacer + Send + Sync> MemDiskStoreCache<R> {
    pub fn new(
        disk_replacer: R,
        disk_base_path: String,
        mem_replacer: Option<R>,
        mem_max_file_size: Option<usize>,
    ) -> Self {
        let disk_manager = DiskManager::default();
        let disk_store = DiskStore::new(disk_manager, disk_base_path);

        if mem_replacer.is_some() {
            let mem_store =
                MemStore::new(mem_max_file_size.unwrap_or(DEFAULT_MEM_CACHE_MAX_FILE_SIZE));
            MemDiskStoreCache {
                disk_store,
                mem_store: Some(mem_store),
                disk_replacer: RwLock::new(disk_replacer),
                mem_replacer: Some(RwLock::new(mem_replacer.unwrap())),
            }
        } else {
            MemDiskStoreCache {
                disk_store,
                mem_store: None,
                disk_replacer: RwLock::new(disk_replacer),
                mem_replacer: None,
            }
        }
    }
}

#[async_trait]
impl<R: DataStoreReplacer + Send + Sync> DataStoreCache for MemDiskStoreCache<R> {
    async fn get_data_from_cache(
        &mut self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        // TODO: Refine the lock.
        if let Some(mem_store) = &self.mem_store {
            let mut mem_repalcer = self.mem_replacer.as_ref().unwrap().write().await;
            if let Some((data_store_key, _)) = mem_repalcer.get(&remote_location) {
                match mem_store.read_data(data_store_key) {
                    Ok(Some(rx)) => return Ok(Some(rx)),
                    Ok(None) => {
                        return Err(ParpulseError::Internal(
                            "Memory replacer and memory store keys are inconsistent.".to_string(),
                        ))
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        let mut disk_replacer = self.disk_replacer.write().await;
        match disk_replacer.get(&remote_location) {
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
        &mut self,
        remote_location: String,
        mut data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        let mut bytes_to_disk = None;
        let mut evicted_bytes_to_disk: Option<
            Vec<(ParpulseDataStoreCacheKey, (Vec<Bytes>, usize))>,
        > = None;
        if let Some(mem_store) = &mut self.mem_store {
            let mem_store_key = mem_store.data_store_key(&remote_location);
            let mut bytes_written = 0;
            loop {
                match data_stream.next().await {
                    Some(Ok(bytes)) => {
                        bytes_written += bytes.len();
                        if let Some((bytes_vec, _)) =
                            mem_store.write_data(mem_store_key.clone(), bytes)
                        {
                            bytes_to_disk = Some(bytes_vec);
                            break;
                        }
                    }
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }

            if bytes_to_disk.is_none() {
                let mut mem_replacer = self.mem_replacer.as_ref().unwrap().write().await;
                let (status, evicted_keys) = mem_replacer.put(
                    remote_location.clone(),
                    (mem_store_key.clone(), bytes_written),
                );
                if !status {
                    // Put the data to disk cache.
                    bytes_to_disk = Some(mem_store.clean_data(&mem_store_key).unwrap().0);
                }
                if let Some(evicted_keys) = evicted_keys {
                    evicted_bytes_to_disk = Some(
                        evicted_keys
                            .iter()
                            .map(|key| {
                                let (bytes_vec, data_size) = mem_store.clean_data(key).unwrap();
                                (key.clone(), (bytes_vec, data_size))
                            })
                            .collect(),
                    );
                }
            }
        }

        if let Some(evicted_bytes_to_disk) = evicted_bytes_to_disk {
            for (key, (bytes_vec, data_size)) in evicted_bytes_to_disk {
                let disk_store_key = self.disk_store.data_store_key(&key);
                self.disk_store
                    .write_data(disk_store_key.clone(), Some(bytes_vec), None)
                    .await?;
                let mut disk_store_cache = self.disk_replacer.write().await;
                if !disk_store_cache
                    .put(remote_location.clone(), (disk_store_key.clone(), data_size))
                    .0
                {
                    self.disk_store.clean_data(&disk_store_key).await?;
                }
            }
        }

        let disk_store_key = self.disk_store.data_store_key(&remote_location);
        // TODO: Write the data store cache first w/ `incompleted` state and update the state
        // after finishing writing into the data store.
        let data_size = self
            .disk_store
            .write_data(disk_store_key.clone(), bytes_to_disk, Some(data_stream))
            .await?;
        let mut data_store_cache = self.disk_replacer.write().await;
        if !data_store_cache
            .put(remote_location, (disk_store_key.clone(), data_size))
            .0
        {
            self.disk_store.clean_data(&disk_store_key).await?;
        }
        Ok(data_size)
    }
}
