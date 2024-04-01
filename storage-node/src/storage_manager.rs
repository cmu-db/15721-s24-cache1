use std::vec;

use crate::{
    cache::data_store_cache::DataStoreCache,
    error::{ParpulseError, ParpulseResult},
    storage_reader::{s3_diskmock::MockS3Reader, AsyncStorageReader},
};

use storage_common::RequestParams;

/// [`StorageManager`] handles the request from the storage client.
///
/// We should allow concurrent requests fed into the storage manager,
/// which should be responsible for handling multiple requests at the
/// same time.

pub struct StorageManager<C: DataStoreCache> {
    /// We don't use lock here because `data_store_cache` itself should handle the concurrency.
    data_store_cache: C,
}

impl<C: DataStoreCache> StorageManager<C> {
    pub fn new(data_store_cache: C) -> Self {
        Self { data_store_cache }
    }

    // In dummy function, we assume request = bucket:key
    // TODO(lanlou): When we change dummy_s3_request, we should also change tests
    fn dummy_s3_request(&self, request: String) -> (String, Vec<String>) {
        let mut iter = request.split(':');
        let bucket = iter.next().unwrap().to_string();
        let keys = vec![iter.next().unwrap().to_string()];
        (bucket, keys)
    }

    pub async fn get_data(&mut self, request: RequestParams) -> ParpulseResult<usize> {
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        // 3. If needed, update the cache with the data fetched from the storage reader.
        // TODO: Support more request types.
        let request = match request {
            RequestParams::S3(request) => request,
            _ => {
                return Err(ParpulseError::Internal(
                    "Unsupported request types".to_string(),
                ))
            }
        };
        let (bucket, keys) = self.dummy_s3_request(request.clone());

        // FIXME: Cache key should be <bucket + key>. Might refactor the underlying S3
        // reader as one S3 key for one reader.
        let data_rx = self
            .data_store_cache
            .get_data_from_cache(request.clone())
            .await?;
        if let Some(mut data_rx) = data_rx {
            let mut data_size: usize = 0;
            while let Some(data) = data_rx.recv().await {
                match data {
                    Ok(bytes) => {
                        // TODO: Put the data into network. May need to push down the response
                        // stream to data store.
                        data_size += bytes.len();
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(data_size)
        } else {
            // Get data from the underlying storage and put the data into the cache.
            let reader = MockS3Reader::new(bucket.clone(), keys).await;
            let data_size = self
                .data_store_cache
                .put_data_to_cache(request, reader.into_stream().await?)
                .await?;
            Ok(data_size)
        }
    }
}

/// fn buffer(&self) -> &[u8]; ensures Iterator has a buffer
/// This buffer function returns the starting point of the result.
/// **NOTE**: The result buffer must be **CONTINUOUS** in bytes with the size in Item as its length.
pub trait ParpulseReaderIterator: Iterator<Item = ParpulseResult<usize>> {
    fn buffer(&self) -> &[u8];
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::cache::{
        data_store_cache::memdisk::cache_manager::MemDiskStoreCache, policy::lru::LruReplacer,
    };

    use super::*;

    #[tokio::test]
    async fn test_storage_manager_disk_only() {
        let dummy_size = 1000000;
        let cache = LruReplacer::new(dummy_size);

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let cache_base_path = dir.join("test-storage-manager");

        let data_store_cache =
            MemDiskStoreCache::new(cache, cache_base_path.display().to_string(), None, None);
        let mut storage_manager = StorageManager::new(data_store_cache);

        let request_path = "tests-parquet:userdata1.parquet";
        let request = RequestParams::S3(request_path.to_string());

        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 113629);
        let delta_time_miss = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 113629);
        let delta_time_hit = Instant::now() - start_time;

        println!(
            "Delta time miss: {:?}, delta time hit: {:?}",
            delta_time_miss, delta_time_hit
        );
        assert!(delta_time_miss > delta_time_hit);
    }

    #[tokio::test]
    async fn test_storage_manager_mem_disk_1() {
        // 1. get small data (-> memory)
        // 2. get large data (-> disk)
        // 3. get small data again
        // 4. get large data again
        // 5. compare time
        let dummy_size = 1000000;
        let disk_cache = LruReplacer::new(dummy_size);
        let mem_cache = LruReplacer::new(dummy_size);

        let tmp = tempfile::tempdir().unwrap();
        let disk_cache_base_path = tmp.path().to_owned();

        let data_store_cache = MemDiskStoreCache::new(
            disk_cache,
            disk_cache_base_path.display().to_string(),
            Some(mem_cache),
            Some(950),
        );
        let mut storage_manager = StorageManager::new(data_store_cache);

        let request_path_small = "tests-text:what-can-i-hold-you-with";
        let request_small = RequestParams::S3(request_path_small.to_string());

        let result = storage_manager.get_data(request_small.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 930);

        let request_path_large = "tests-parquet:userdata2.parquet";
        let request_large = RequestParams::S3(request_path_large.to_string());

        let result = storage_manager.get_data(request_large.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 112193);

        // Get data again.
        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request_large).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 112193);
        let delta_time_hit_disk = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request_small).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 930);
        let delta_time_hit_mem = Instant::now() - start_time;

        println!(
            "For small and large files, Delta time hit mem: {:?}, delta time hit disk: {:?}",
            delta_time_hit_mem, delta_time_hit_disk
        );
        assert!(delta_time_hit_disk > delta_time_hit_mem);
    }

    #[tokio::test]
    async fn test_storage_manager_mem_disk_2() {
        // 1. get large data1 (-> memory)
        // 2. get large data2 (-> memory, and evict data1 to disk)
        // 3. get data1 again
        // 4. get data2 again
        // 5. compare time
        let disk_cache = LruReplacer::new(1000000);
        let mem_cache = LruReplacer::new(120000);

        let tmp = tempfile::tempdir().unwrap();
        let disk_cache_base_path = tmp.path().to_owned();

        let data_store_cache = MemDiskStoreCache::new(
            disk_cache,
            disk_cache_base_path.display().to_string(),
            Some(mem_cache),
            Some(120000),
        );
        let mut storage_manager = StorageManager::new(data_store_cache);

        let request_path_data1 = "tests-parquet:userdata1.parquet";
        let request_data1 = RequestParams::S3(request_path_data1.to_string());

        let result = storage_manager.get_data(request_data1.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 113629);

        let request_path_data2 = "tests-parquet:userdata2.parquet";
        let request_data2 = RequestParams::S3(request_path_data2.to_string());

        let result = storage_manager.get_data(request_data2.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 112193);

        // Get data again. Now data2 in memory and data1 in disk.
        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request_data1).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 113629);
        let delta_time_hit_disk = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request_data2).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 112193);
        let delta_time_hit_mem = Instant::now() - start_time;

        println!(
            "For almost same files, delta time hit mem: {:?}, delta time hit disk: {:?}",
            delta_time_hit_mem, delta_time_hit_disk
        );
        assert!(delta_time_hit_disk > delta_time_hit_mem);
    }
}
