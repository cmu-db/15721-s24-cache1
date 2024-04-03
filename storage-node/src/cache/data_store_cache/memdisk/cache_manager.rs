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

pub struct MemDiskStoreCache<R: DataStoreReplacer> {
    disk_store: DiskStore,
    mem_store: Option<MemStore>,
    /// Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    disk_replacer: RwLock<R>,
    mem_replacer: Option<RwLock<R>>,
}

/// This method creates a `MemDiskStoreCache` instance, and memory can be disabled by setting
/// `mem_replacer` to `None`.
/// If one file is larger than `mem_max_file_size`, we will not write it into memory. And all the files
/// in the memory cannot exceed the `maximum_capacity` of the memory replacer.
/// Ideally, mem_max_file_size should always be less than or equal to the maximum capacity of the memory.
impl<R: DataStoreReplacer> MemDiskStoreCache<R> {
    pub fn new(
        disk_replacer: R,
        disk_base_path: String,
        mem_replacer: Option<R>,
        mem_max_file_size: Option<usize>,
    ) -> Self {
        let disk_manager = DiskManager::default();
        let disk_store = DiskStore::new(disk_manager, disk_base_path);

        if mem_replacer.is_some() {
            let mut mem_max_file_size =
                mem_max_file_size.unwrap_or(DEFAULT_MEM_CACHE_MAX_FILE_SIZE);
            let replacer_max_capacity = mem_replacer.as_ref().unwrap().max_capacity();
            if mem_max_file_size > replacer_max_capacity {
                // TODO: better log.
                println!("The maximum file size > replacer's max capacity, so we set maximum file size = 1/5 of the maximum capacity.");
                // By default in this case, replacer can store at least 5 files.
                mem_max_file_size = replacer_max_capacity / 5;
            }

            let mem_store = MemStore::new(mem_max_file_size);
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
impl<R: DataStoreReplacer> DataStoreCache for MemDiskStoreCache<R> {
    async fn get_data_from_cache(
        &mut self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        // TODO: Refine the lock.
        if let Some(mem_store) = &self.mem_store {
            let mut mem_replacer = self.mem_replacer.as_ref().unwrap().write().await;
            if let Some((data_store_key, _)) = mem_replacer.get(&remote_location) {
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
        // TODO: Refine the lock.
        // TODO(lanlou): Also write the data to network.
        let mut bytes_to_disk = None;
        let mut bytes_mem_written = 0;
        let mut evicted_bytes_to_disk: Option<
            Vec<(ParpulseDataStoreCacheKey, (Vec<Bytes>, usize))>,
        > = None;
        // 1. If the mem_store is enabled, first try to write the data to memory.
        // Note: Only file which size < mem_max_file_size can be written to memory.
        if let Some(mem_store) = &mut self.mem_store {
            loop {
                match data_stream.next().await {
                    Some(Ok(bytes)) => {
                        bytes_mem_written += bytes.len();
                        // TODO: Need to write the data to network.
                        if let Some((bytes_vec, _)) =
                            mem_store.write_data(remote_location.clone(), bytes)
                        {
                            // If write_data returns something, it means the file size is too large
                            // to fit in the memory. We should put it to disk cache.
                            bytes_to_disk = Some(bytes_vec);
                            break;
                        }
                    }
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }

            if bytes_to_disk.is_none() {
                // If bytes_to_disk has nothing, it means the data has been successfully written to memory.
                // We have to put it into mem_replacer too.
                let mut mem_replacer = self.mem_replacer.as_ref().unwrap().write().await;
                let replacer_put_status = mem_replacer.put(
                    remote_location.clone(),
                    (remote_location.clone(), bytes_mem_written),
                );
                // Insertion fails.
                if replacer_put_status.is_none() {
                    // Put the data to disk cache.
                    bytes_to_disk = Some(mem_store.clean_data(&remote_location).unwrap().0);
                } else {
                    // If successfully putting it into mem_replacer, we should record the evicted data,
                    // delete them from mem_store, and put all of them to disk cache.
                    if let Some(evicted_keys) = replacer_put_status {
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
        }

        // 2. If applicable, put all the evicted data into disk cache.
        // Don't need to write the data to network for evicted keys.
        if let Some(evicted_bytes_to_disk) = evicted_bytes_to_disk {
            for (remote_location_evicted, (bytes_vec, data_size)) in evicted_bytes_to_disk {
                assert_ne!(remote_location_evicted, remote_location);
                let disk_store_key = self.disk_store.data_store_key(&remote_location_evicted);
                self.disk_store
                    .write_data(disk_store_key.clone(), Some(bytes_vec), None)
                    .await?;
                let mut disk_replacer = self.disk_replacer.write().await;
                if disk_replacer
                    .put(remote_location_evicted, (disk_store_key.clone(), data_size))
                    .is_none()
                {
                    self.disk_store.clean_data(&disk_store_key).await?;
                }
            }
        }

        // 3. If the data is successfully written to memory, directly return.
        if self.mem_store.is_some() && bytes_to_disk.is_none() {
            return Ok(bytes_mem_written);
        }

        // 4. If the data is not written to memory_cache successfully, then cache it to disk.
        // Need to write the data to network for the current key.
        let disk_store_key = self.disk_store.data_store_key(&remote_location);
        // TODO: Write the data store cache first w/ `incompleted` state and update the state
        // after finishing writing into the data store.
        let data_size = self
            .disk_store
            .write_data(disk_store_key.clone(), bytes_to_disk, Some(data_stream))
            .await?;
        let mut disk_replacer = self.disk_replacer.write().await;
        if disk_replacer
            .put(remote_location, (disk_store_key.clone(), data_size))
            .is_none()
        {
            self.disk_store.clean_data(&disk_store_key).await?;
            // TODO: do we need to notify the caller this failure?
        }
        Ok(data_size)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cache::policy::lru::LruReplacer,
        storage_reader::{s3_diskmock::MockS3Reader, AsyncStorageReader},
    };

    use super::*;
    #[tokio::test]
    async fn test_put_data_evicted_from_mem() {
        // 1. put small data1
        // 2. put small data2, making data1 evicted
        //    (TODO(lanlou): when adding pin & unpin, this test should be modified)
        // 3. get data1, should be from disk
        // 4. get data2, should be from memory
        // TODO(lanlou): What if we continue accessing data1 and never accessing data2 in
        // the future? There is no chance for data1 to be put back to memory again currently.
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let mut cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(950)),
            None,
        );
        let bucket = "tests-text".to_string();
        let keys = vec!["what-can-i-hold-you-with".to_string()];
        let data1_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let mut data_size = cache
            .put_data_to_cache(data1_key.clone(), reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(data_size, 930);

        let data2_key = bucket.clone() + &keys[0] + "1";
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        data_size = cache
            .put_data_to_cache(data2_key.clone(), reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(data_size, 930);

        data_size = 0;
        let mut rx = cache.get_data_from_cache(data1_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            data_size += data.len();
        }
        assert_eq!(data_size, 930);

        data_size = 0;
        let mut rx = cache.get_data_from_cache(data2_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            data_size += data.len();
        }
        assert_eq!(data_size, 930);
    }

    #[tokio::test]
    async fn test_put_get_disk_only() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let mut cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            None,
            None,
        );
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        let mut data_size = cache
            .put_data_to_cache(remote_location.clone(), reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(data_size, 113629);

        let keys = vec!["userdata2.parquet".to_string()];
        let remote_location2 = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket, keys).await;
        data_size = cache
            .put_data_to_cache(
                remote_location2.clone(),
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(data_size, 112193);

        let mut rx = cache
            .get_data_from_cache(remote_location)
            .await
            .unwrap()
            .unwrap();
        data_size = 0;
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            data_size += data.len();
        }
        assert_eq!(data_size, 113629);
    }

    #[tokio::test]
    async fn test_put_get_mem_disk() {
        // 1. put a small file (-> memory)
        // 2. put a large file (-> disk)
        // 3. get the small file
        // 4. get the large file
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let mut cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(950)),
            Some(950),
        );
        let bucket = "tests-text".to_string();
        let keys = vec!["what-can-i-hold-you-with".to_string()];
        let data1_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let mut data_size = cache
            .put_data_to_cache(data1_key.clone(), reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(data_size, 930);

        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let data2_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        data_size = cache
            .put_data_to_cache(data2_key.clone(), reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(data_size, 113629);

        data_size = 0;
        let mut rx = cache.get_data_from_cache(data1_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            data_size += data.len();
        }
        assert_eq!(data_size, 930);

        data_size = 0;
        let mut rx = cache.get_data_from_cache(data2_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            data_size += data.len();
        }
        assert_eq!(data_size, 113629);
    }
}
