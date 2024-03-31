use std::path::Path;
use storage_common::RequestParams;
use tokio::fs::File as AsyncFile;
use tokio_util::io::ReaderStream;
use warp::{Filter, Rejection};

use crate::{
    cache::{
        data_store_cache::memdisk::cache_manager::MemDiskStoreCache, policy::lru::LruReplacer,
    },
    error::ParpulseResult,
    storage_manager::StorageManager,
};

pub async fn storage_node_serve(file_dir: RequestParams) -> ParpulseResult<()> {
    // TODO: Read the type of the cache from config.
    let dummy_size = 10;
    let cache = LruReplacer::new(dummy_size);
    // TODO: cache_base_path should be from config
    let data_store_cache = MemDiskStoreCache::new(cache, "cache/".to_string(), None, None);
    let _storage_manager = StorageManager::new(data_store_cache);

    // FIXME (kunle): We need to get the file from storage manager. For now we directly read
    // the file from disk. I will update it after the storage manager provides the relevant API.
    let route = warp::path!("file" / String)
        .and(warp::path::end())
        .and_then(move |file_name: String| {
            // Have to clone file_dir to move it into the closure
            let file_dir = file_dir.clone();
            async move {
                let file_path = match file_dir {
                    RequestParams::File(dir) => format!("{}/{}", dir, file_name),
                    _ => {
                        return Err(warp::reject::not_found());
                    }
                };

                if !Path::new(&file_path).exists() {
                    return Err(warp::reject::not_found());
                }

                println!("File Path in the server: {}", file_path);
                let file = match AsyncFile::open(&file_path).await {
                    Ok(file) => file,
                    Err(e) => {
                        eprintln!("Error opening file: {}", e);
                        return Err(warp::reject::not_found());
                    }
                };

                let stream = ReaderStream::new(file);
                let body = warp::hyper::Body::wrap_stream(stream);
                let response = warp::http::Response::builder()
                    .header("Content-Type", "text/plain")
                    .body(body)
                    .unwrap();

                // Return the file content as response
                Ok::<_, Rejection>(warp::reply::with_status(
                    response,
                    warp::http::StatusCode::OK,
                ))
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
        let file_dir = RequestParams::File("tests/parquet".to_string());

        // Start the server
        let server_handle = tokio::spawn(async move {
            storage_node_serve(file_dir).await.unwrap();
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

        let original_file_path = "tests/parquet/userdata1.parquet";
        let original_file = fs::read(original_file_path).unwrap();
        let downloaded_file = fs::read(file_path).unwrap();
        assert_eq!(original_file, downloaded_file);

        server_handle.abort();
    }
}
