use log::{info, warn};
use parpulse_client::{RequestParams, S3Request};

use std::sync::Arc;
use std::time::Instant;
use std::{net::IpAddr, sync::Mutex};
use tokio_stream::wrappers::ReceiverStream;
use warp::{Filter, Rejection};

use crate::{
    cache::{
        data_store_cache::{memdisk::MemDiskStoreCache, sqlite::SqliteStoreCache},
        replacer::{lru::LruReplacer, lru_k::LruKReplacer},
    },
    common::config::{ParpulseConfig, ParpulseConfigCachePolicy, ParpulseConfigDataStore},
    error::ParpulseResult,
    storage_manager::{StorageManager, StorageManagerImpl},
};

const CACHE_BASE_PATH: &str = "parpulse-cache";
const DEFAULT_DATA_STORE_CACHE_NUM: usize = 3;
const DEFAULT_MEM_CACHE_SIZE: usize = 200 * 1024 * 1024;
const DEFAULT_DISK_CACHE_SIZE: usize = 1024 * 1024 * 1024;
const DEFAULT_SQLITE_CACHE_SIZE: usize = 200 * 1024 * 1024;
const DEFAULT_MEM_CACHE_MAX_FILE_SIZE: usize = 10 * 1024 * 1024 + 1;
const DEFAULT_LRU_K_VALUE: usize = 2;
const DEFAULT_MAX_DISK_READER_BUFFER_SIZE: usize = 100 * 1024 * 1024;
const DEFAULT_SQLITE_BLOB_READER_BUFFER_SIZE: usize = 1024;

async fn route(storage_manager: Arc<impl StorageManager + 'static>, ip_addr: &str, port: u16) {
    let route = warp::path!("file")
        .and(warp::path::end())
        .and(warp::query::<S3Request>())
        .and_then(move |params: S3Request| {
            let start_time = Instant::now();
            let storage_manager = storage_manager.clone();
            if params.is_test {
                info!(
                    "[Parpulse Logging] received test request {} for bucket: {}, keys: {:?}",
                    params.request_id, params.bucket, params.keys
                );
            } else {
                info!(
                    "[Parpulse Logging] received request {} for bucket: {}, keys: {:?}",
                    params.request_id, params.bucket, params.keys
                );
            }
            async move {
                let bucket = params.bucket;
                let keys = params.keys;
                let request = if params.is_test {
                    RequestParams::MockS3((bucket, vec![keys]))
                } else {
                    RequestParams::S3((bucket, vec![keys]))
                };
                let request_id = params.request_id;

                let result = storage_manager.get_data(request_id, request).await;
                match result {
                    Ok(data_rx) => {
                        let stream = ReceiverStream::new(data_rx);
                        let body = warp::hyper::Body::wrap_stream(stream);
                        let server_time = format!("{}", start_time.elapsed().as_micros());
                        let response = warp::http::Response::builder()
                            .header("Content-Type", "text/plain")
                            .header("Server-Time", server_time.clone())
                            .body(body)
                            .unwrap();
                        Ok::<_, Rejection>(warp::reply::with_status(
                            response,
                            warp::http::StatusCode::OK,
                        ))
                    }
                    Err(e) => {
                        let error_message = format!("Failed to get data: {}", e);
                        let response = warp::http::Response::builder()
                            .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                            .body(error_message.into())
                            .unwrap();
                        Ok::<_, Rejection>(warp::reply::with_status(
                            response,
                            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                        ))
                    }
                }
            }
        });

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let shutdown = warp::path!("shutdown").map({
        let tx = Arc::clone(&tx); // Clone the Arc, not the sender.
        move || {
            let mut tx = tx.lock().unwrap();
            if let Some(tx) = tx.take() {
                tx.send(()).unwrap();
            }
            warp::http::StatusCode::OK
        }
    });

    let heartbeat = warp::path!("heartbeat").map(|| warp::http::StatusCode::OK);

    // Catch a request that does not match any of the routes above.
    let catch_all = warp::any()
        .and(warp::path::full())
        .map(|path: warp::path::FullPath| {
            warn!("Catch all route hit. Path: {}", path.as_str());
            warp::http::StatusCode::NOT_FOUND
        });

    let routes = route.or(heartbeat).or(shutdown).or(catch_all);
    let ip_addr: IpAddr = ip_addr.parse().unwrap();
    let process = tokio::spawn(async move {
        warp::serve(routes).run((ip_addr, port)).await;
    });

    // Kill the process when the receiver receives a message.
    rx.await.unwrap();
    process.abort();
}

pub async fn storage_node_serve(
    ip_addr: &str,
    port: u16,
    config: ParpulseConfig,
) -> ParpulseResult<()> {
    let data_store_cache_num = config
        .data_store_cache_num
        .unwrap_or(DEFAULT_DATA_STORE_CACHE_NUM);
    match config.data_store {
        ParpulseConfigDataStore::Memdisk => {
            let disk_cache_size = config.disk_cache_size.unwrap_or(DEFAULT_DISK_CACHE_SIZE);
            let mem_cache_size = config.mem_cache_size.unwrap_or(DEFAULT_MEM_CACHE_SIZE);
            let mem_cache_file_size = config
                .mem_cache_file_size
                .unwrap_or(DEFAULT_MEM_CACHE_MAX_FILE_SIZE);
            let max_disk_reader_buffer_size = config
                .max_disk_reader_buffer_size
                .unwrap_or(DEFAULT_MAX_DISK_READER_BUFFER_SIZE);
            let cache_base_path = config.cache_path.unwrap_or(CACHE_BASE_PATH.to_string());
            match config.cache_policy {
                ParpulseConfigCachePolicy::Lru => {
                    info!("[Parpulse Configuration] starting storage node with {} mem-disk cache(s) and LRU cache policy, disk cache size: {}, mem cache size: {}, mem cache file size: {}, max disk reader buffer size: {}", data_store_cache_num, disk_cache_size, mem_cache_size, mem_cache_file_size, max_disk_reader_buffer_size);
                    let mut data_store_caches = Vec::new();
                    for i in 0..data_store_cache_num {
                        let disk_replacer = LruReplacer::new(disk_cache_size);
                        let mem_replacer = LruReplacer::new(mem_cache_size);
                        let data_store_cache = MemDiskStoreCache::new(
                            disk_replacer,
                            i.to_string() + &cache_base_path,
                            Some(mem_replacer),
                            Some(mem_cache_file_size),
                            max_disk_reader_buffer_size,
                        );
                        data_store_caches.push(data_store_cache);
                    }
                    let storage_manager = Arc::new(StorageManagerImpl::new(data_store_caches));
                    route(storage_manager, ip_addr, port).await;
                }
                ParpulseConfigCachePolicy::Lruk => {
                    info!("[Parpulse Configuration] starting storage node with {} mem-disk cache(s) and LRU-K cache policy, disk cache size: {}, mem cache size: {}, mem cache file size: {}, max disk reader buffer size: {}", data_store_cache_num, disk_cache_size, mem_cache_size, mem_cache_file_size, max_disk_reader_buffer_size);
                    let mut data_store_caches = Vec::new();
                    let k = config.cache_lru_k.unwrap_or(DEFAULT_LRU_K_VALUE);
                    for i in 0..data_store_cache_num {
                        let disk_replacer = LruKReplacer::new(disk_cache_size, k);
                        let mem_replacer = LruKReplacer::new(mem_cache_size, k);
                        let data_store_cache = MemDiskStoreCache::new(
                            disk_replacer,
                            i.to_string() + &cache_base_path,
                            Some(mem_replacer),
                            Some(mem_cache_file_size),
                            max_disk_reader_buffer_size,
                        );
                        data_store_caches.push(data_store_cache);
                    }
                    let storage_manager = Arc::new(StorageManagerImpl::new(data_store_caches));
                    route(storage_manager, ip_addr, port).await;
                }
            };
        }
        ParpulseConfigDataStore::Disk => {
            let disk_cache_size = config.disk_cache_size.unwrap_or(DEFAULT_DISK_CACHE_SIZE);
            let cache_base_path = config.cache_path.unwrap_or(CACHE_BASE_PATH.to_string());
            let max_disk_reader_buffer_size = config
                .max_disk_reader_buffer_size
                .unwrap_or(DEFAULT_MAX_DISK_READER_BUFFER_SIZE);
            match config.cache_policy {
                ParpulseConfigCachePolicy::Lru => {
                    info!("[Parpulse Configuration] starting storage node with {} disk-only cache(s) and LRU cache policy, disk cache size: {}, max disk reader buffer size: {}", data_store_cache_num, disk_cache_size, max_disk_reader_buffer_size);
                    let mut data_store_caches = Vec::new();
                    for i in 0..data_store_cache_num {
                        let disk_replacer = LruReplacer::new(disk_cache_size);
                        let data_store_cache = MemDiskStoreCache::new(
                            disk_replacer,
                            i.to_string() + &cache_base_path,
                            None,
                            None,
                            max_disk_reader_buffer_size,
                        );
                        data_store_caches.push(data_store_cache);
                    }
                    let storage_manager = Arc::new(StorageManagerImpl::new(data_store_caches));
                    route(storage_manager, ip_addr, port).await;
                }
                ParpulseConfigCachePolicy::Lruk => {
                    info!("[Parpulse Configuration] starting storage node with {} disk-only cache(s) and LRU-K cache policy, disk cache size: {}, max disk reader buffer size: {}", data_store_cache_num, disk_cache_size, max_disk_reader_buffer_size);
                    let mut data_store_caches = Vec::new();
                    let k = config.cache_lru_k.unwrap_or(DEFAULT_LRU_K_VALUE);
                    for i in 0..data_store_cache_num {
                        let disk_replacer = LruKReplacer::new(disk_cache_size, k);
                        let data_store_cache = MemDiskStoreCache::new(
                            disk_replacer,
                            i.to_string() + &cache_base_path,
                            None,
                            None,
                            max_disk_reader_buffer_size,
                        );
                        data_store_caches.push(data_store_cache);
                    }
                    let storage_manager = Arc::new(StorageManagerImpl::new(data_store_caches));
                    route(storage_manager, ip_addr, port).await;
                }
            }
        }
        ParpulseConfigDataStore::Sqlite => {
            let sqlite_base_path =
                config.cache_path.unwrap_or(CACHE_BASE_PATH.to_string()) + "sqlite.db3";
            let sqlite_cache_size = config.mem_cache_size.unwrap_or(DEFAULT_SQLITE_CACHE_SIZE);
            let sqlite_blob_reader_buffer_size = config
                .sqlite_blob_reader_buffer_size
                .unwrap_or(DEFAULT_SQLITE_BLOB_READER_BUFFER_SIZE);
            match config.cache_policy {
                ParpulseConfigCachePolicy::Lru => {
                    info!("[Parpulse Configuration] starting storage node with {} sqlite cache(s) and LRU cache policy, cache size: {}, blob reader buffer size: {}", data_store_cache_num, sqlite_cache_size, sqlite_blob_reader_buffer_size);
                    let mut data_store_caches = Vec::new();
                    for i in 0..data_store_cache_num {
                        let replacer = LruReplacer::new(sqlite_cache_size);
                        let sqlite_data_cache = SqliteStoreCache::new(
                            replacer,
                            i.to_string() + &sqlite_base_path,
                            sqlite_blob_reader_buffer_size,
                        )?;
                        data_store_caches.push(sqlite_data_cache);
                    }
                    let storage_manager = Arc::new(StorageManagerImpl::new(data_store_caches));
                    route(storage_manager, ip_addr, port).await;
                }
                ParpulseConfigCachePolicy::Lruk => {
                    info!("[Parpulse Configuration] starting storage node with {} sqlite cache(s) and LRU-K cache policy, cache size: {}, blob reader buffer size: {}", data_store_cache_num, sqlite_cache_size, sqlite_blob_reader_buffer_size);
                    let k = config.cache_lru_k.unwrap_or(DEFAULT_LRU_K_VALUE);
                    let mut data_store_caches = Vec::new();
                    for i in 0..data_store_cache_num {
                        let replacer = LruKReplacer::new(sqlite_cache_size, k);
                        let sqlite_data_cache = SqliteStoreCache::new(
                            replacer,
                            i.to_string() + &sqlite_base_path,
                            sqlite_blob_reader_buffer_size,
                        )?;
                        data_store_caches.push(sqlite_data_cache);
                    }
                    let storage_manager = Arc::new(StorageManagerImpl::new(data_store_caches));
                    route(storage_manager, ip_addr, port).await;
                }
            }
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use reqwest::Client;
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;

    /// WARNING: Put userdata1.parquet in the storage-node/tests/parquet directory before running this test.
    #[tokio::test]
    #[allow(clippy::field_reassign_with_default)]
    async fn test_server() {
        let original_file_path = "tests/parquet/userdata1.parquet";
        let mut config = ParpulseConfig::default();
        config.data_store_cache_num = Some(6);
        // Start the server
        let server_handle = tokio::spawn(async move {
            storage_node_serve("127.0.0.1", 3030, config).await.unwrap();
        });

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // Test1: test_download_file
        let url =
            "http://localhost:3030/file?bucket=tests-parquet&keys=userdata1.parquet&is_test=true";
        let client = Client::new();
        let mut response = client
            .get(url)
            .send()
            .await
            .expect("Failed to get response from the server.");
        assert!(
            response.status().is_success(),
            "Failed to download file. Status code: {}",
            response.status()
        );

        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("userdata1.parquet");
        let mut file = fs::File::create(&file_path).unwrap();

        // Stream the response body and write to the file
        while let Some(chunk) = response.chunk().await.unwrap() {
            file.write_all(&chunk).unwrap();
        }
        assert!(file_path.exists(), "File not found after download");

        // Check if file sizes are equal
        assert_eq!(
            fs::metadata(original_file_path).unwrap().len(),
            fs::metadata(file_path.clone()).unwrap().len()
        );

        assert_eq!(fs::metadata(file_path).unwrap().len(), 113629);

        // Test2: test_file_not_exist
        let url =
            "http://localhost:3030/file?bucket=tests-parquet&keys=not_exist.parquet&is_test=true";
        let client = Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .expect("Failed to get response from the server.");

        assert!(
            response.status().is_server_error(),
            "Expected 500 status code"
        );

        server_handle.abort();
    }
}
