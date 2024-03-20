use std::{io, sync::Arc, time::Duration};

use anyhow::bail;
use tokio::sync::{Mutex, RwLock};

use crate::{
    cache::{lru::LruCache, CacheValue, ParpulseCache},
    disk::disk_manager::DiskManager,
    error::ParpulseResult,
    server::RequestParams,
    storage_reader::{mock_s3::MockS3Reader, StorageReader, StorageReaderIterator},
};

/// [`StorageManager`] handles the request from the storage client.
///
/// We should allow concurrent requests fed into the storage manager,
/// which should be responsible for handling multiple requests at the
/// same time.

pub struct StorageManager<C: ParpulseCache> {
    // TODO: Consider making the cache lock-free.
    cache: RwLock<C>,
    disk_manager: Mutex<DiskManager>,
    // TODO: cache_base_path should be from config
    // Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    cache_base_path: String,
}

impl<C: ParpulseCache> StorageManager<C> {
    pub fn new(cache: C, disk_manager: DiskManager, cache_base_path: String) -> Self {
        Self {
            cache: RwLock::new(cache),
            disk_manager: Mutex::new(disk_manager),
            cache_base_path,
        }
    }

    pub async fn get_data(&self, request: RequestParams) -> ParpulseResult<usize> {
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        // 3. If needed, update the cache with the data fetched from the storage reader.

        // TODO: Support more request types.
        let key = request.as_file().unwrap();
        let mut cache = self.cache.write().await;

        match cache.get(key) {
            Some(value) => {
                // Directly get data from the cache.
                let cache_value_size = value.size();
                let cache_file_path = value.as_value();

                let mut output_iterator = self
                    .disk_manager
                    .lock()
                    .await
                    .disk_read_sync_iterator(cache_file_path, 1024)?;
                let mut data_size: usize = 0;
                loop {
                    match output_iterator.next() {
                        Some(Ok(bytes_read)) => {
                            let buffer = output_iterator.buffer();
                            // TODO: Put the data into network
                            println!("Output {} bytes", bytes_read);
                            data_size += bytes_read;
                        }
                        Some(Err(e)) => return Err(e),
                        None => break,
                    }
                }

                Ok(data_size)
            }

            None => {
                // Get data from the underlying storage and put the data into the cache.
                let cache_value_path = format!("{}{}", &self.cache_base_path, key);

                // TODO:
                // 1. Set buffer size from config
                // 2. Support more types of storage.
                let reader = MockS3Reader::new(key.clone(), Some(Duration::from_secs(2)), 1024);

                // TODO:
                // 1. Put the logics into `CacheManager`, which should handle the case where the write-out
                // size exceeds the value size (value size > cache size).
                // 2. We should call `iter.next()` outside `CacheManager` or `DiskManager` b/c we want to
                // write the data to the response buffer at the same time.
                let data_size = self
                    .disk_manager
                    .lock()
                    .await
                    .write_reader_to_disk_sync(reader.read()?, &cache_value_path)?;
                if !cache.put(key.clone(), (cache_value_path.clone(), data_size)) {
                    self.disk_manager
                        .lock()
                        .await
                        .remove_file(&cache_value_path)?;
                    // TODO:
                    // 1. Define our own error type.
                    // 2. It's actually valid if the data cannot fit in the cache. We just write the data
                    // into the response buffer.
                    return Err(
                        io::Error::new(io::ErrorKind::Other, "Failed to put into cache").into(),
                    );
                }

                // TODO: Put the data into network

                Ok(data_size)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Instant};

    use super::*;

    #[tokio::test]
    async fn test_storage_manager() {
        let dummy_size = 1000000;
        let cache = LruCache::new(dummy_size);
        let disk_manager = DiskManager::default();
        let storage_manager = StorageManager::new(cache, disk_manager, "test/".to_string());

        let request_path = "tests/parquet/house_price.parquet";
        let request = RequestParams::File(request_path.to_string());

        let mut start_time = Instant::now();
        let result = storage_manager.get_data(request.clone()).await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            fs::metadata(request_path).unwrap().len() as usize
        );
        let delta_time_miss = Instant::now() - start_time;

        start_time = Instant::now();
        let result = storage_manager.get_data(request).await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            fs::metadata(request_path).unwrap().len() as usize
        );
        let delta_time_hit = Instant::now() - start_time;

        println!(
            "Delta time miss: {:?}, delta time hit: {:?}",
            delta_time_miss, delta_time_hit
        );
        assert!(delta_time_miss > delta_time_hit);

        fs::remove_dir_all(storage_manager.cache_base_path.clone());
    }
}
