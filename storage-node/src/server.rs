use futures::lock::Mutex;
use std::sync::Arc;
use storage_common::{RequestParams, S3Request};
use tokio_stream::wrappers::ReceiverStream;
use warp::{Filter, Rejection};

use crate::{
    cache::{
        data_store_cache::memdisk::cache_manager::MemDiskStoreCache, policy::lru::LruReplacer,
    },
    error::ParpulseResult,
    storage_manager::StorageManager,
};

const CACHE_BASE_PATH: &str = "cache/";

pub async fn storage_node_serve() -> ParpulseResult<()> {
    let dummy_size = 1000000;
    // TODO: Read the type of the cache from config.
    let cache = LruReplacer::new(dummy_size);
    // TODO: cache_base_path should be from config
    let data_store_cache = MemDiskStoreCache::new(cache, CACHE_BASE_PATH.to_string(), None, None);
    // TODO: try to use more fine-grained lock instead of locking the whole storage_manager
    let storage_manager = Arc::new(Mutex::new(StorageManager::new(data_store_cache)));

    let route = warp::path!("file")
        .and(warp::path::end())
        .and(warp::query::<S3Request>())
        .and_then(move |params: S3Request| {
            let storage_manager = storage_manager.clone();
            println!(
                "Received request for bucket: {}, keys: {:?}",
                params.bucket, params.keys
            );
            async move {
                let bucket = params.bucket;
                let keys = params.keys;
                let request = RequestParams::S3((bucket, vec![keys]));
                let result = storage_manager.lock().await.get_data(request).await;
                let data_rx = result.unwrap();

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
        });

    // Catch a request that does not match any of the routes above.
    let catch_all = warp::any()
        .and(warp::path::full())
        .map(|path: warp::path::FullPath| {
            println!("Catch all route hit. Path: {}", path.as_str());
            warp::http::StatusCode::NOT_FOUND
        });

    let routes = route.or(catch_all);
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;

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
    async fn test_download_file() {
        let original_file_path = "tests/parquet/userdata1.parquet";

        // Start the server
        let server_handle = tokio::spawn(async move {
            storage_node_serve().await.unwrap();
        });

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let url = "http://localhost:3030/file?bucket=tests-parquet&keys=userdata1.parquet";
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
            fs::metadata(file_path).unwrap().len()
        );

        server_handle.abort();
    }
}
