use std::{io, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::{
    cache::{lru::LruCache, ParpulseCache},
    disk::disk_manager::DiskManager,
    server::RequestParams,
    storage_reader::{mock_s3::MockS3Reader, StorageReader, StorageReaderIterator},
    StorageResult,
};

/// [`StorageManager`] handles the request from the storage client.
///
/// We should allow concurrent requests fed into the storage manager,
/// which should be responsible for handling multiple requests at the
/// same time.

pub struct StorageManager<C: ParpulseCache> {
    // TODO: Consider making the cache lock-free. See the comments for
    // `ParpulseCache`.
    cache: Arc<RwLock<C>>,
    // TODO: Consider making the disk manager lock-free.
    // Currently the lock is to protect the statistics, please consider more decent strategy.
    disk_manager: Arc<RwLock<DiskManager>>,
    // TODO: cache_base_path should be from config
    // Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    pub cache_base_path: String,
}

impl<C: ParpulseCache> StorageManager<C> {
    pub fn new(cache: C, disk_manager: DiskManager, cache_base_path: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(cache)),
            disk_manager: Arc::new(RwLock::new(disk_manager)),
            cache_base_path,
        }
    }

    pub async fn get_data(&self, _request: RequestParams) -> StorageResult<usize> {
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        // 3. If needed, update the cache with the data fetched from the storage reader.

        let key = match _request {
            RequestParams::File(ref file_path) => file_path,
            RequestParams::S3(ref s3_path) => s3_path,
        };

        let mut cache_file_path: Option<String> = None;
        let mut cache_size: Option<usize> = None;

        {
            // TODO(urgent): Cache read lock releases here, but disk read hasn't finished, what if this item is evicted now? Maybe we need `pin_count`, or keep the lock until the end of this method
            let mut cache = self.cache.write().await;
            if let Some(value) = cache.get(key) {
                cache_file_path = Some(value.0.clone());
                cache_size = Some(value.1);
            }
        }

        if cache_file_path.is_none() {
            let cache_value_path = format!("{}{}", &self.cache_base_path, key);

            // TODO: 1. update buffer_size, maybe from config 2. use _request type to new reader, not directly new MockS3Reader
            let reader = MockS3Reader::new(key.clone(), Some(Duration::from_secs(2)), 1024);

            let mut disk_manager = self.disk_manager.write().await;
            // TODO: assume that disk file path is the same as the key
            cache_size = Some(
                disk_manager.write_reader_to_disk_sync(reader.read_sync()?, &cache_value_path)?,
            );

            let mut cache = self.cache.write().await;
            if !cache.put(key.clone(), (cache_value_path.clone(), cache_size.unwrap())) {
                disk_manager.remove_file(&cache_value_path)?;
                // FIXME: Is there a cache error from Datafusion? Should all of our errors belong to I/O error in Datafusion error? Or we need to define our own error
                return Err(
                    io::Error::new(io::ErrorKind::Other, "Failed to put into cache").into(),
                );
            }

            cache_file_path = Some(cache_value_path);
        }
        // TODO(urgent): If we need calcuate the statistic for read, even if we only want to read disk_manager, we need to grab write lock since we want to modify statistics! (need refactor)
        // TODO: update buffer_size, maybe from config
        let mut output_iterator = self
            .disk_manager
            .read()
            .await
            .disk_read_sync_iterator(&cache_file_path.unwrap(), 1024)?;
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
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Instant};

    use super::*;

    #[tokio::test]
    async fn test_storage_manager() {
        let dummy_size = 1000000;
        let cache = LruCache::new(dummy_size);
        let disk_manager = DiskManager {};
        let storage_manager = StorageManager::new(cache, disk_manager, "test/".to_string());

        let request_path = "mock_parquet/House_price.parquet";
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
