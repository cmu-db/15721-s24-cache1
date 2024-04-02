use bytes::Bytes;

use tokio::sync::{mpsc::Receiver, RwLock};

use crate::{
    cache::{data::DataStore, policy::DataStoreCache},
    error::ParpulseResult,
    storage_reader::{s3_diskmock::MockS3Reader, AsyncStorageReader, StorageReaderStream},
};

use storage_common::RequestParams;

/// [`StorageManager`] handles the request from the storage client.
///
/// We should allow concurrent requests fed into the storage manager,
/// which should be responsible for handling multiple requests at the
/// same time.

pub struct StorageManager<C: DataStoreCache, DS: DataStore> {
    /// Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    data_store_cache: RwLock<C>,
    /// We don't use lock here because `data_store` itself should handle the concurrency.
    data_store: DS,
}

impl<C: DataStoreCache, DS: DataStore> StorageManager<C, DS> {
    pub fn new(data_store_cache: C, data_store: DS) -> Self {
        Self {
            data_store_cache: RwLock::new(data_store_cache),
            data_store,
        }
    }

    fn dummy_s3_request(&self) -> (String, Vec<String>) {
        let bucket = "tests-parquet".to_string();
        let keys = vec![
            "userdata1.parquet".to_string(),
            "userdata2.parquet".to_string(),
        ];
        (bucket, keys)
    }

    pub async fn get_data(
        &self,
        _request: RequestParams,
    ) -> ParpulseResult<Receiver<ParpulseResult<Bytes>>> {
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        // 3. If needed, update the cache with the data fetched from the storage reader.
        // TODO: Support more request types.
        let (bucket, keys) = self.dummy_s3_request();

        // FIXME: Cache key should be <bucket + key>. Might refactor the underlying S3
        // reader as one S3 key for one reader.
        let data_rx = self.get_data_from_cache(bucket.clone()).await?;
        if let Some(data_rx) = data_rx {
            Ok(data_rx)
        } else {
            let reader = MockS3Reader::new(bucket.clone(), keys).await;
            let stream = reader.into_stream().await?;
            self.put_data_to_cache(bucket.clone(), stream).await?;
            let data_rx = self.get_data_from_cache(bucket).await?.unwrap();
            Ok(data_rx)
        }
    }

    pub async fn get_data_from_cache(
        &self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        // TODO: Refine the lock.
        let mut data_store_cache = self.data_store_cache.write().await;
        match data_store_cache.get(&remote_location) {
            Some((data_store_key, _)) => {
                if let Some(data) = self.data_store.read_data(data_store_key).await? {
                    Ok(Some(data))
                } else {
                    unreachable!()
                }
            }
            None => Ok(None),
        }
    }

    pub async fn put_data_to_cache(
        &self,
        remote_location: String,
        data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        let data_store_key = self.data_store.data_store_key(&remote_location);
        // TODO: Write the data store cache first w/ `incompleted` state and update the state
        // after finishing writing into the data store.
        let data_size = self
            .data_store
            .write_data(data_store_key.clone(), data_stream)
            .await?;
        let mut data_store_cache = self.data_store_cache.write().await;
        if !data_store_cache.put(remote_location, (data_store_key.clone(), data_size)) {
            self.data_store.clean_data(&data_store_key).await?;
        }
        Ok(data_size)
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

    use crate::{
        cache::{data::disk::DiskStore, policy::lru::LruCache},
        disk::disk_manager::DiskManager,
    };

    use super::*;

    #[tokio::test]
    async fn test_storage_manager() {
        let dummy_size = 1000000;
        let cache = LruCache::new(dummy_size);

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let cache_base_path = dir.join("test-storage-manager");
        let data_store = DiskStore::new(
            DiskManager::default(),
            cache_base_path.display().to_string(),
        );
        let storage_manager = StorageManager::new(cache, data_store);

        let request_path = "dummy_s3_request";
        let request = RequestParams::S3(request_path.to_string());

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
        assert_eq!(total_bytes, 113629 + 112193);
        let delta_time_miss = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request).await;
        assert!(result.is_ok());
        let mut data_rx = result.unwrap();
        total_bytes = 0;
        while let Some(data) = data_rx.recv().await {
            match data {
                Ok(bytes) => {
                    total_bytes += bytes.len();
                }
                Err(e) => panic!("Error receiving data: {:?}", e),
            }
        }
        assert_eq!(total_bytes, 113629 + 112193);
        let delta_time_hit = Instant::now() - start_time;

        println!(
            "Delta time miss: {:?}, delta time hit: {:?}",
            delta_time_miss, delta_time_hit
        );
        assert!(delta_time_miss > delta_time_hit);
    }
}
