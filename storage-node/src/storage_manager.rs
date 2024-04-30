use crate::{
    cache::data_store_cache::{cache_key_from_request, DataStoreCache},
    common::hash::calculate_hash_crc32fast,
    error::ParpulseResult,
};

use bytes::Bytes;
use log::debug;
use parpulse_client::RequestParams;
use tokio::sync::mpsc::Receiver;

/// [`StorageManager`] handles the request from the storage client.
///
/// We should allow concurrent requests fed into the storage manager,
/// which should be responsible for handling multiple requests at the
/// same time.

pub struct StorageManager<C: DataStoreCache> {
    /// We don't use lock here because `data_store_cache` itself should handle the concurrency.
    data_store_caches: Vec<C>,
}

impl<C: DataStoreCache> StorageManager<C> {
    pub fn new(data_store_caches: Vec<C>) -> Self {
        Self { data_store_caches }
    }

    pub async fn get_data(
        &self,
        request: RequestParams,
    ) -> ParpulseResult<Receiver<ParpulseResult<Bytes>>> {
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        // 3. If needed, update the cache with the data fetched from the storage reader.

        // TODO: Support more request types.

        // FIXME: Cache key should be <bucket + key>. Might refactor the underlying S3
        // reader as one S3 key for one reader.
        let cache_key = cache_key_from_request(&request);
        let hash = calculate_hash_crc32fast(cache_key.as_bytes());
        let cache_index = hash % self.data_store_caches.len();
        let data_store_cache = self.data_store_caches.get(cache_index).unwrap();

        debug!(
            "For cache key: {}, the corresponding data_store_cache index {}",
            cache_key, cache_index
        );

        let data_rx = data_store_cache.get_data_from_cache(&request).await?;
        if let Some(data_rx) = data_rx {
            Ok(data_rx)
        } else {
            data_store_cache.put_data_to_cache(&request).await?;
            // TODO (kunle): Push down the response writer rather than calling get_data_from_cache again.
            let data_rx = data_store_cache.get_data_from_cache(&request).await?;
            if data_rx.is_none() {
                panic!("Data should be in the cache now. {}", cache_key.clone());
            }
            Ok(data_rx.unwrap())
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
    use futures::join;
    use std::{sync::Arc, time::Instant};

    use crate::cache::{data_store_cache::memdisk::MemDiskStoreCache, replacer::lru::LruReplacer};

    use super::*;

    async fn consume_receiver(mut rx: Receiver<ParpulseResult<Bytes>>) -> usize {
        let mut total_bytes = 0;
        while let Some(data) = rx.recv().await {
            match data {
                Ok(bytes) => {
                    total_bytes += bytes.len();
                }
                Err(e) => panic!("Error receiving data: {:?}", e),
            }
        }
        total_bytes
    }

    #[tokio::test]
    async fn test_storage_manager_disk_only() {
        let dummy_size = 1000000;
        let cache = LruReplacer::new(dummy_size);

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let cache_base_path = dir.join("test-storage-manager");

        let data_store_cache =
            MemDiskStoreCache::new(cache, cache_base_path.display().to_string(), None, None);
        let storage_manager = StorageManager::new(vec![data_store_cache]);

        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let request = RequestParams::MockS3((bucket, keys));

        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request.clone()).await;
        assert!(result.is_ok());
        let mut data_rx = result.unwrap();
        let mut total_bytes = 0;
        while let Some(data) = data_rx.recv().await {
            match data {
                Ok(bytes) => {
                    total_bytes += bytes.len();
                }
                Err(e) => panic!("Error receiving data: {:?}", e),
            }
        }
        assert_eq!(total_bytes, 113629);
        let delta_time_miss = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request).await;
        assert!(result.is_ok());
        let data_rx = result.unwrap();
        assert_eq!(consume_receiver(data_rx).await, 113629);
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
        let storage_manager = StorageManager::new(vec![data_store_cache]);

        let request_path_small_bucket = "tests-text".to_string();
        let request_path_small_keys = vec!["what-can-i-hold-you-with".to_string()];
        let request_small =
            RequestParams::MockS3((request_path_small_bucket, request_path_small_keys));

        let result = storage_manager.get_data(request_small.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 930);

        let request_path_large_bucket = "tests-parquet".to_string();
        let request_path_large_keys = vec!["userdata2.parquet".to_string()];
        let request_large =
            RequestParams::MockS3((request_path_large_bucket, request_path_large_keys));

        let result = storage_manager.get_data(request_large.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);

        // Get data again.
        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request_large).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);
        let delta_time_hit_disk = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request_small).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 930);
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
        let storage_manager = StorageManager::new(vec![data_store_cache]);

        let request_path_bucket1 = "tests-parquet".to_string();
        let request_path_keys1 = vec!["userdata1.parquet".to_string()];
        let request_data1 = RequestParams::MockS3((request_path_bucket1, request_path_keys1));

        let result = storage_manager.get_data(request_data1.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 113629);

        let request_path_bucket2 = "tests-parquet".to_string();
        let request_path_keys2 = vec!["userdata2.parquet".to_string()];
        let request_data2 = RequestParams::MockS3((request_path_bucket2, request_path_keys2));

        let result = storage_manager.get_data(request_data2.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);

        // Get data again. Now data2 in memory and data1 in disk.
        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request_data1).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 113629);
        let delta_time_hit_disk = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request_data2).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);
        let delta_time_hit_mem = Instant::now() - start_time;

        println!(
            "For almost same files, delta time hit mem: {:?}, delta time hit disk: {:?}",
            delta_time_hit_mem, delta_time_hit_disk
        );
        assert!(delta_time_hit_disk > delta_time_hit_mem);
    }

    #[tokio::test]
    async fn test_storage_manager_parallel_1() {
        let disk_cache = LruReplacer::new(1000000);

        let tmp = tempfile::tempdir().unwrap();
        let disk_cache_base_path = tmp.path().to_owned();

        let data_store_cache = MemDiskStoreCache::new(
            disk_cache,
            disk_cache_base_path.display().to_string(),
            None,
            None,
        );
        let storage_manager = Arc::new(StorageManager::new(vec![data_store_cache]));

        let request_path_bucket1 = "tests-parquet".to_string();
        let request_path_keys1 = vec!["userdata1.parquet".to_string()];
        let request_data1 = RequestParams::MockS3((request_path_bucket1, request_path_keys1));

        let request_path_bucket2 = "tests-parquet".to_string();
        let request_path_keys2 = vec!["userdata2.parquet".to_string()];
        let request_data2 = RequestParams::MockS3((request_path_bucket2, request_path_keys2));

        let storage_manager_1 = storage_manager.clone();
        let request_data1_1 = request_data1.clone();
        let get_data_fut_1 =
            tokio::spawn(async move { storage_manager_1.get_data(request_data1_1).await });

        let storage_manager_2 = storage_manager.clone();
        let request_data1_2 = request_data1.clone();
        let get_data_fut_2 =
            tokio::spawn(async move { storage_manager_2.get_data(request_data1_2).await });

        let storage_manager_3 = storage_manager.clone();
        let request_data2_3 = request_data2.clone();
        let get_data_fut_3 =
            tokio::spawn(async move { storage_manager_3.get_data(request_data2_3).await });

        let storage_manager_4 = storage_manager.clone();
        let request_data1_4 = request_data1.clone();
        let get_data_fut_4 =
            tokio::spawn(async move { storage_manager_4.get_data(request_data1_4).await });

        let result = join!(
            get_data_fut_1,
            get_data_fut_2,
            get_data_fut_3,
            get_data_fut_4
        );
        assert!(result.0.is_ok());
        assert_eq!(consume_receiver(result.0.unwrap().unwrap()).await, 113629);
        assert!(result.1.is_ok());
        assert_eq!(consume_receiver(result.1.unwrap().unwrap()).await, 113629);
        assert!(result.2.is_ok());
        assert_eq!(consume_receiver(result.2.unwrap().unwrap()).await, 112193);
        assert!(result.3.is_ok());
        assert_eq!(consume_receiver(result.3.unwrap().unwrap()).await, 113629);
    }

    #[tokio::test]
    async fn test_storage_manager_parallel_2() {
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
        let storage_manager = Arc::new(StorageManager::new(vec![data_store_cache]));

        let request_path_bucket1 = "tests-parquet".to_string();
        let request_path_keys1 = vec!["userdata2.parquet".to_string()];
        let request_data1 = RequestParams::MockS3((request_path_bucket1, request_path_keys1));

        let request_path_bucket2 = "tests-parquet".to_string();
        let request_path_keys2 = vec!["userdata1.parquet".to_string()];
        let request_data2 = RequestParams::MockS3((request_path_bucket2, request_path_keys2));

        let mut start_time = Instant::now();

        let storage_manager_1 = storage_manager.clone();
        let request_data1_1 = request_data1.clone();
        let get_data_fut_1 =
            tokio::spawn(async move { storage_manager_1.get_data(request_data1_1).await });

        let storage_manager_2 = storage_manager.clone();
        let request_data1_2 = request_data1.clone();
        let get_data_fut_2 =
            tokio::spawn(async move { storage_manager_2.get_data(request_data1_2).await });

        let storage_manager_3 = storage_manager.clone();
        let request_data2_3 = request_data2.clone();
        let get_data_fut_3 =
            tokio::spawn(async move { storage_manager_3.get_data(request_data2_3).await });

        let storage_manager_4 = storage_manager.clone();
        let request_data2_4 = request_data2.clone();
        let get_data_fut_4 =
            tokio::spawn(async move { storage_manager_4.get_data(request_data2_4).await });

        let storage_manager_5 = storage_manager.clone();
        let request_data1_5 = request_data1.clone();
        let get_data_fut_5 =
            tokio::spawn(async move { storage_manager_5.get_data(request_data1_5).await });

        let result = join!(
            get_data_fut_1,
            get_data_fut_2,
            get_data_fut_3,
            get_data_fut_4,
            get_data_fut_5
        );
        assert!(result.0.is_ok());
        assert_eq!(consume_receiver(result.0.unwrap().unwrap()).await, 112193);
        assert!(result.1.is_ok());
        assert_eq!(consume_receiver(result.1.unwrap().unwrap()).await, 112193);
        assert!(result.2.is_ok());
        assert_eq!(consume_receiver(result.2.unwrap().unwrap()).await, 113629);
        assert!(result.3.is_ok());
        assert_eq!(consume_receiver(result.3.unwrap().unwrap()).await, 113629);
        assert!(result.4.is_ok());
        assert_eq!(consume_receiver(result.4.unwrap().unwrap()).await, 112193);

        let delta_time_miss = Instant::now() - start_time;

        start_time = Instant::now();

        let storage_manager_1 = storage_manager.clone();
        let request_data2_1 = request_data2.clone();
        let get_data_fut_1 =
            tokio::spawn(async move { storage_manager_1.get_data(request_data2_1).await });

        let storage_manager_2 = storage_manager.clone();
        let request_data1_2 = request_data1.clone();
        let get_data_fut_2 =
            tokio::spawn(async move { storage_manager_2.get_data(request_data1_2).await });

        let storage_manager_3 = storage_manager.clone();
        let request_data2_3 = request_data2.clone();
        let get_data_fut_3 =
            tokio::spawn(async move { storage_manager_3.get_data(request_data2_3).await });

        let storage_manager_4 = storage_manager.clone();
        let request_data1_4 = request_data1.clone();
        let get_data_fut_4 =
            tokio::spawn(async move { storage_manager_4.get_data(request_data1_4).await });

        let storage_manager_5 = storage_manager.clone();
        let request_data1_5 = request_data1.clone();
        let get_data_fut_5 =
            tokio::spawn(async move { storage_manager_5.get_data(request_data1_5).await });

        let result = join!(
            get_data_fut_1,
            get_data_fut_2,
            get_data_fut_3,
            get_data_fut_4,
            get_data_fut_5
        );
        assert!(result.0.is_ok());
        assert_eq!(consume_receiver(result.0.unwrap().unwrap()).await, 113629);
        assert!(result.1.is_ok());
        assert_eq!(consume_receiver(result.1.unwrap().unwrap()).await, 112193);
        assert!(result.2.is_ok());
        assert_eq!(consume_receiver(result.2.unwrap().unwrap()).await, 113629);
        assert!(result.3.is_ok());
        assert_eq!(consume_receiver(result.3.unwrap().unwrap()).await, 112193);
        assert!(result.4.is_ok());
        assert_eq!(consume_receiver(result.4.unwrap().unwrap()).await, 112193);

        let delta_time_hit = Instant::now() - start_time;

        println!(
            "For parallel test 2, delta time miss: {:?}, delta time miss: {:?}",
            delta_time_miss, delta_time_hit
        );
        assert!(delta_time_miss > delta_time_hit);
    }

    #[tokio::test]
    async fn test_fanout_cache() {
        let data_store_cache_num = 6;
        let mut data_store_caches = Vec::new();
        for _ in 0..data_store_cache_num {
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
            data_store_caches.push(data_store_cache);
        }
        let storage_manager = Arc::new(StorageManager::new(data_store_caches));

        let request_path_bucket1 = "tests-parquet".to_string();
        let request_path_keys1 = vec!["userdata1.parquet".to_string()];
        let request_data1 = RequestParams::MockS3((request_path_bucket1, request_path_keys1));

        let result = storage_manager.get_data(request_data1.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 113629);
        let request_path_bucket2 = "tests-parquet".to_string();
        let request_path_keys2 = vec!["userdata2.parquet".to_string()];
        let request_data2 = RequestParams::MockS3((request_path_bucket2, request_path_keys2));
        let result = storage_manager.get_data(request_data2.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);

        let request_path_bucket3 = "tests-text".to_string();
        let request_path_keys3: Vec<String> = vec!["what-can-i-hold-you-with".to_string()];
        let request_data3 = RequestParams::MockS3((request_path_bucket3, request_path_keys3));
        let result = storage_manager.get_data(request_data3.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 930);

        let request_path_bucket4 = "tests-parquet".to_string();
        let request_path_keys4: Vec<String> = vec!["small_random_data.parquet".to_string()];
        let request_data4 = RequestParams::MockS3((request_path_bucket4, request_path_keys4));
        let result = storage_manager.get_data(request_data4.clone()).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 2013);
    }

    #[tokio::test]
    async fn test_fanout_cach_parallel() {
        let data_store_cache_num = 6;
        let mut data_store_caches = Vec::new();
        for _ in 0..data_store_cache_num {
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
            data_store_caches.push(data_store_cache);
        }
        let storage_manager = Arc::new(StorageManager::new(data_store_caches));

        let request_path_bucket1 = "tests-parquet".to_string();
        let request_path_keys1 = vec!["userdata2.parquet".to_string()];
        let request_data1 = RequestParams::MockS3((request_path_bucket1, request_path_keys1));

        let request_path_bucket2 = "tests-parquet".to_string();
        let request_path_keys2 = vec!["userdata1.parquet".to_string()];
        let request_data2 = RequestParams::MockS3((request_path_bucket2, request_path_keys2));

        let request_path_bucket3 = "tests-text".to_string();
        let request_path_keys3 = vec!["what-can-i-hold-you-with".to_string()];
        let request_data3 = RequestParams::MockS3((request_path_bucket3, request_path_keys3));

        let storage_manager_1 = storage_manager.clone();
        let request_data1_1 = request_data1.clone();
        let get_data_fut_1 =
            tokio::spawn(async move { storage_manager_1.get_data(request_data1_1).await });

        let storage_manager_2 = storage_manager.clone();
        let request_data1_2 = request_data1.clone();
        let get_data_fut_2 =
            tokio::spawn(async move { storage_manager_2.get_data(request_data1_2).await });

        let storage_manager_3 = storage_manager.clone();
        let request_data3_3 = request_data3.clone();
        let get_data_fut_3 =
            tokio::spawn(async move { storage_manager_3.get_data(request_data3_3).await });

        let storage_manager_4 = storage_manager.clone();
        let request_data2_4 = request_data2.clone();
        let get_data_fut_4 =
            tokio::spawn(async move { storage_manager_4.get_data(request_data2_4).await });

        let storage_manager_5 = storage_manager.clone();
        let request_data2_5 = request_data2.clone();
        let get_data_fut_5 =
            tokio::spawn(async move { storage_manager_5.get_data(request_data2_5).await });

        let storage_manager_6 = storage_manager.clone();
        let request_data1_6 = request_data1.clone();
        let get_data_fut_6 =
            tokio::spawn(async move { storage_manager_6.get_data(request_data1_6).await });

        let storage_manager_7 = storage_manager.clone();
        let request_data3_7 = request_data3.clone();
        let get_data_fut_7 =
            tokio::spawn(async move { storage_manager_7.get_data(request_data3_7).await });

        let result = join!(
            get_data_fut_1,
            get_data_fut_2,
            get_data_fut_3,
            get_data_fut_4,
            get_data_fut_5,
            get_data_fut_6,
            get_data_fut_7
        );
        assert!(result.0.is_ok());
        assert_eq!(consume_receiver(result.0.unwrap().unwrap()).await, 112193);
        assert!(result.1.is_ok());
        assert_eq!(consume_receiver(result.1.unwrap().unwrap()).await, 112193);
        assert!(result.2.is_ok());
        assert_eq!(consume_receiver(result.2.unwrap().unwrap()).await, 930);
        assert!(result.3.is_ok());
        assert_eq!(consume_receiver(result.3.unwrap().unwrap()).await, 113629);
        assert!(result.4.is_ok());
        assert_eq!(consume_receiver(result.4.unwrap().unwrap()).await, 113629);
        assert!(result.5.is_ok());
        assert_eq!(consume_receiver(result.5.unwrap().unwrap()).await, 112193);
        assert!(result.6.is_ok());
        assert_eq!(consume_receiver(result.6.unwrap().unwrap()).await, 930);
    }
}
