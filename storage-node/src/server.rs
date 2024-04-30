use log::{info, warn};
use parpulse_client::{RequestParams, S3Request};
use std::net::IpAddr;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use warp::{Filter, Rejection};

use crate::{
    cache::{
        data_store_cache::{memdisk::MemDiskStoreCache, sqlite::SqliteStoreCache},
        replacer::lru::LruReplacer,
    },
    error::ParpulseResult,
    storage_manager::StorageManager,
};

const CACHE_BASE_PATH: &str = "cache/";
const DATA_STORE_CACHE_NUMBER: usize = 6;

pub async fn storage_node_serve(ip_addr: &str, port: u16) -> ParpulseResult<()> {
    // Should at least be able to store one 100MB file in the cache.
    // TODO: Read the type of the cache from config.
    let dummy_size_per_mem_cache = 1024 * 1024 * 1024;

    let mut data_store_caches = Vec::new();

    for i in 0..DATA_STORE_CACHE_NUMBER {
        let mem_replacer = LruReplacer::new(dummy_size_per_mem_cache);
        let sqlite_data_cache = SqliteStoreCache::new(
            mem_replacer,
            CACHE_BASE_PATH.to_string() + &i.to_string(),
            None,
        )?;
        data_store_caches.push(sqlite_data_cache);
    }

    let is_mem_disk_cache = false;
    // TODO: try to use more fine-grained lock instead of locking the whole storage_manager
    let storage_manager = Arc::new(StorageManager::new(data_store_caches));

    let route = warp::path!("file")
        .and(warp::path::end())
        .and(warp::query::<S3Request>())
        .and_then(move |params: S3Request| {
            let storage_manager = storage_manager.clone();
            if params.is_test {
                info!(
                    "Received test request for bucket: {}, keys: {:?}",
                    params.bucket, params.keys
                );
            } else {
                info!(
                    "Received request for bucket: {}, keys: {:?}",
                    params.bucket, params.keys
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
                let result = storage_manager.get_data(request, is_mem_disk_cache).await;
                match result {
                    Ok(data_rx) => {
                        let stream = ReceiverStream::new(data_rx);
                        let body = warp::hyper::Body::wrap_stream(stream);
                        let response = warp::http::Response::builder()
                            .header("Content-Type", "text/plain")
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

    let heartbeat = warp::path!("heartbeat").map(|| warp::http::StatusCode::OK);

    // Catch a request that does not match any of the routes above.
    let catch_all = warp::any()
        .and(warp::path::full())
        .map(|path: warp::path::FullPath| {
            warn!("Catch all route hit. Path: {}", path.as_str());
            warp::http::StatusCode::NOT_FOUND
        });

    let routes = route.or(heartbeat).or(catch_all);
    let ip_addr: IpAddr = ip_addr.parse().unwrap();
    warp::serve(routes).run((ip_addr, port)).await;

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
    async fn test_server() {
        let original_file_path = "tests/parquet/userdata1.parquet";

        // Start the server
        let server_handle = tokio::spawn(async move {
            storage_node_serve("127.0.0.1", 3030).await.unwrap();
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
