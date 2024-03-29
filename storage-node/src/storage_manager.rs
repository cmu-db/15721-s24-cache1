use std::{io, sync::Arc, time::Duration};

use anyhow::bail;
use bytes::BytesMut;
use futures::{FutureExt, Stream};
use tokio::sync::{Mutex, RwLock};

use crate::{
    cache::{lru::LruCache, CacheValue, ParpulseCache},
    disk::disk_manager_sync::DiskManagerSync,
    error::ParpulseResult,
    storage_reader::{s3_diskmock::MockS3Reader, AsyncStorageReader, SyncStorageReader},
};

use storage_common::RequestParams;

/// [`StorageManager`] handles the request from the storage client.
///
/// We should allow concurrent requests fed into the storage manager,
/// which should be responsible for handling multiple requests at the
/// same time.

pub struct StorageManager<C: ParpulseCache> {
    // TODO: Consider making the cache lock-free.
    cache: RwLock<C>,
    disk_manager: Mutex<DiskManagerSync>,
    // TODO: cache_base_path should be from config
    // Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    cache_base_path: String,
}

impl<C: ParpulseCache> StorageManager<C> {
    pub fn new(cache: C, disk_manager: DiskManagerSync, cache_base_path: String) -> Self {
        Self {
            cache: RwLock::new(cache),
            disk_manager: Mutex::new(disk_manager),
            cache_base_path,
        }
    }

    // TODO: This is a dummy function.
    fn parse_s3_request(&self, request: &str) -> ParpulseResult<(String, Vec<String>)> {
        let bucket = "tests-parquet".to_string();
        let keys = vec![
            "userdata1.parquet".to_string(),
            "userdata2.parquet".to_string(),
        ];
        Ok((bucket, keys))
    }

    // TODO: this function uses sync disk_manager, we should add a function to use async, and
    // switch between them via configuration for benchmark.
    pub async fn get_data(&self, request: RequestParams) -> ParpulseResult<usize> {
        /// 1. Try to get data from the cache first.
        /// 2. If cache miss, then go to storage reader to fetch the data from
        /// the underlying storage.
        /// 3. If needed, update the cache with the data fetched from the storage reader.
        // TODO: Support more request types.
        let s3_request = request.as_s3().unwrap();
        let (bucket, keys) = self.parse_s3_request(s3_request)?;
        let mut cache = self.cache.write().await;

        // TODO: now cache key is s3 raw request string, but it may be unreasonable, since
        // for different requests, keys may overlap, maybe we should make key to be `bucket + one key`.
        // And this needs refactor from both `storage_manager` and `s3`, since we need distinguish the
        // bytes for different keys.
        match cache.get(s3_request) {
            Some(value) => {
                // Directly get data from the cache.
                let cache_value_size = value.size();
                let cache_file_path = value.as_value();

                let mut output_iterator = self
                    .disk_manager
                    .lock()
                    .await
                    .disk_read_iterator(cache_file_path, 1024)?;
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
                let cache_value_path = format!("{}{}", &self.cache_base_path, s3_request);

                // TODO:
                // 1. Set buffer size from config
                // 2. Support more types of storage.
                let reader = MockS3Reader::new(bucket, keys).await;

                // TODO:
                // 1. Put the logics into `CacheManager`, which should handle the case where the write-out
                // size exceeds the value size (value size > cache size).
                // 2. We should call `iter.next()` outside `CacheManager` or `DiskManager` b/c we want to
                // write the data to the response buffer at the same time.
                let data_size = self
                    .disk_manager
                    .lock()
                    .await
                    .write_stream_reader_to_disk(reader.into_stream().await?, &cache_value_path)
                    .await?;
                if !cache.put(s3_request.clone(), (cache_value_path.clone(), data_size)) {
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

/// The reason why we not use ParpulseResult<Bytes> is this trait allows the possibility for
/// a fixed buffer, which makes sense to some disk method, and a fixed buffer may reduce unnecessary
/// memory allocation.
/// Current disk_manager implementation has one big drawback: it cannot send data to network and read
/// next data from disk at the same time. But this can be solved by 2 ways:
/// 1. the method from s3, buffer will be consumed and extended, but the memory allocation every time
///    may be expensive.
/// 2. use 2 fixed buffers, one for reading from disk, one for sending to network, and use boolean
///    member buffer_current to indicate which buffer is current buffer. But, it needs double space of
///    original buffer, which means we can serve less requests, but one single request will handle faster.
///    The first method also has similar problem, but it's more flexible in memory size.
/// We can implement both and benchmark at the end. This trait can be extended for both.
/// Currently, s3 utilizes method 1, and disk utilizes one single fixed buffer.

/// fn buffer(&self) -> &[u8]; ensures Iterator has a buffer
/// This buffer function returns the starting point of the result.
/// **NOTE**: The result buffer must be **CONTINUOUS** in bytes with the size in Item as its length.
pub trait ParpulseReaderIterator: Iterator<Item = ParpulseResult<usize>> {
    fn buffer(&self) -> &[u8];
}

pub trait ParpulseReaderStream: Stream<Item = ParpulseResult<usize>> {
    fn buffer(&self) -> &[u8];
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Instant};

    use super::*;

    #[tokio::test]
    async fn test_storage_manager() {
        let dummy_size = 1000000;
        let cache = LruCache::new(dummy_size);
        let disk_manager = DiskManagerSync::default();

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let storage_manager = StorageManager::new(
            cache,
            disk_manager,
            dir.join("test_storage_manager.txt").display().to_string(),
        );

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
