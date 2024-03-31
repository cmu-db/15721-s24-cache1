use crate::{
    cache::data_store_cache::DataStoreCache,
    error::ParpulseResult,
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

    fn dummy_s3_request(&self) -> (String, Vec<String>) {
        let bucket = "tests-parquet".to_string();
        let keys = vec![
            "userdata1.parquet".to_string(),
            "userdata2.parquet".to_string(),
        ];
        (bucket, keys)
    }

    pub async fn get_data(&mut self, _request: RequestParams) -> ParpulseResult<usize> {
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        // 3. If needed, update the cache with the data fetched from the storage reader.
        // TODO: Support more request types.
        let (bucket, keys) = self.dummy_s3_request();

        // FIXME: Cache key should be <bucket + key>. Might refactor the underlying S3
        // reader as one S3 key for one reader.
        let data_rx = self
            .data_store_cache
            .get_data_from_cache(bucket.clone())
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
                .put_data_to_cache(bucket, reader.into_stream().await?)
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
    async fn test_storage_manager() {
        let dummy_size = 1000000;
        let cache = LruReplacer::new(dummy_size);

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let cache_base_path = dir.join("test-storage-manager");

        let data_store_cache =
            MemDiskStoreCache::new(cache, cache_base_path.display().to_string(), None, None);
        let mut storage_manager = StorageManager::new(data_store_cache);

        let request_path = "dummy_s3_request";
        let request = RequestParams::S3(request_path.to_string());

        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 113629 + 112193);
        let delta_time_miss = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 113629 + 112193);
        let delta_time_hit = Instant::now() - start_time;

        println!(
            "Delta time miss: {:?}, delta time hit: {:?}",
            delta_time_miss, delta_time_hit
        );
        assert!(delta_time_miss > delta_time_hit);
    }
}
