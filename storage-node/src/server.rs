use storage_common::RequestParams;
use tokio_stream::wrappers::ReceiverStream;
use warp::{Filter, Rejection};

use crate::{
    cache::{data::disk::DiskStore, policy::lru::LruCache},
    disk::disk_manager::DiskManager,
    error::ParpulseResult,
    storage_manager::StorageManager,
};

pub async fn storage_node_serve() -> ParpulseResult<()> {
    // TODO: Read the type of the cache from config.
    let dummy_size = 1000000;

    // FIXME (kunle): We need to get the file from storage manager. For now we directly read
    // the file from disk. I will update it after the storage manager provides the relevant API.
    let route = warp::path!("file" / String)
        .and(warp::path::end())
        .and_then(move |file_name: String| {
            async move {
                let cache = LruCache::new(dummy_size);
                let disk_manager = DiskManager::default();
                // TODO: cache_base_path should be from config
                let data_store = DiskStore::new(disk_manager, "cache/".to_string());
                let storage_manager = StorageManager::new(cache, data_store);
                println!("File Name: {}", file_name);
                let bucket = "tests-parquet".to_string();
                let keys = vec![file_name];
                let request = RequestParams::S3((bucket, keys));
                let result = storage_manager.get_data(request).await;
                let data_rx = result.unwrap();

                let stream = ReceiverStream::new(data_rx);
                let body = warp::hyper::Body::wrap_stream(stream);
                let response = warp::http::Response::builder()
                    .header("Content-Type", "text/plain")
                    .body(body)
                    .unwrap();
                return Ok::<_, Rejection>(warp::reply::with_status(
                    response,
                    warp::http::StatusCode::OK,
                ));
            }
        });

    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;

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

        let url = "http://localhost:3030/file/userdata1.parquet";
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
