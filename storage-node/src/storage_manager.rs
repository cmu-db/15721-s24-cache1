use crate::{
    cache::data_store_cache::DataStoreCache,
    error::ParpulseResult,
    storage_reader::{s3::S3Reader, s3_diskmock::MockS3Reader, AsyncStorageReader},
};

use bytes::Bytes;
use storage_client::RequestParams;
use tokio::sync::mpsc::Receiver;

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

    pub async fn get_data(
        &mut self,
        request: RequestParams,
        is_mem_disk_cache: bool,
    ) -> ParpulseResult<Receiver<ParpulseResult<Bytes>>> {
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        // 3. If needed, update the cache with the data fetched from the storage reader.
        // TODO: Support more request types.
        let is_s3_request = matches!(request, RequestParams::S3(_));
        let (bucket, keys) = match request {
            RequestParams::S3((bucket, keys)) => (bucket, keys),
            RequestParams::MockS3((bucket, keys)) => (bucket, keys),
        };

        // FIXME: Cache key should be <bucket + key>. Might refactor the underlying S3
        // reader as one S3 key for one reader.
        let cache_key = format!("{}-{}", bucket, keys.join(","));
        let data_rx = self
            .data_store_cache
            .get_data_from_cache(cache_key.clone())
            .await?;
        if let Some(data_rx) = data_rx {
            Ok(data_rx)
        } else {
            let (stream, data_size) = if is_s3_request {
                let reader = S3Reader::new(bucket.clone(), keys).await;
                let data_size = if is_mem_disk_cache {
                    None
                } else {
                    Some(reader.get_object_size().await?)
                };
                (reader.into_stream().await?, data_size)
            } else {
                let reader = MockS3Reader::new(bucket.clone(), keys).await;
                let data_size = if is_mem_disk_cache {
                    None
                } else {
                    Some(reader.get_object_size().await?)
                };
                (reader.into_stream().await?, data_size)
            };
            self.data_store_cache
                .put_data_to_cache(cache_key.clone(), data_size, stream)
                .await?;
            // TODO (kunle): Push down the response writer rather than calling get_data_from_cache again.
            let data_rx = self
                .data_store_cache
                .get_data_from_cache(cache_key.clone())
                .await?
                .unwrap();
            Ok(data_rx)
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
        let mut storage_manager = StorageManager::new(data_store_cache);

        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let request = RequestParams::MockS3((bucket, keys));

        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request.clone(), true).await;
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
        let result = storage_manager.get_data(request, true).await;
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
        let mut storage_manager = StorageManager::new(data_store_cache);

        let request_path_small_bucket = "tests-text".to_string();
        let request_path_small_keys = vec!["what-can-i-hold-you-with".to_string()];
        let request_small =
            RequestParams::MockS3((request_path_small_bucket, request_path_small_keys));

        let result = storage_manager.get_data(request_small.clone(), true).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 930);

        let request_path_large_bucket = "tests-parquet".to_string();
        let request_path_large_keys = vec!["userdata2.parquet".to_string()];
        let request_large =
            RequestParams::MockS3((request_path_large_bucket, request_path_large_keys));

        let result = storage_manager.get_data(request_large.clone(), true).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);

        // Get data again.
        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request_large, true).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);
        let delta_time_hit_disk = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request_small, true).await;
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
        let mut storage_manager = StorageManager::new(data_store_cache);

        let request_path_bucket1 = "tests-parquet".to_string();
        let request_path_keys1 = vec!["userdata1.parquet".to_string()];
        let request_data1 = RequestParams::MockS3((request_path_bucket1, request_path_keys1));

        let result = storage_manager.get_data(request_data1.clone(), true).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 113629);

        let request_path_bucket2 = "tests-parquet".to_string();
        let request_path_keys2 = vec!["userdata2.parquet".to_string()];
        let request_data2 = RequestParams::MockS3((request_path_bucket2, request_path_keys2));

        let result = storage_manager.get_data(request_data2.clone(), true).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);

        // Get data again. Now data2 in memory and data1 in disk.
        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request_data1, true).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 113629);
        let delta_time_hit_disk = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request_data2, true).await;
        assert!(result.is_ok());
        assert_eq!(consume_receiver(result.unwrap()).await, 112193);
        let delta_time_hit_mem = Instant::now() - start_time;

        println!(
            "For almost same files, delta time hit mem: {:?}, delta time hit disk: {:?}",
            delta_time_hit_mem, delta_time_hit_disk
        );
        assert!(delta_time_hit_disk > delta_time_hit_mem);
    }
}
