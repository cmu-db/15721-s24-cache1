use enum_as_inner::EnumAsInner;
use std::fs;
use std::path::Path;
use storage_common::RequestParams;
use tokio::fs::File as AsyncFile;
use tokio_util::io::ReaderStream;
use warp::{Filter, Rejection, Reply};

use crate::{
    cache::lru::LruCache, disk::disk_manager_sync::DiskManagerSync, error::ParpulseResult,
    storage_manager::StorageManager,
};

pub async fn storage_node_serve() -> ParpulseResult<()> {
    // TODO: Read the type of the cache from config.
    let dummy_size = 10;
    let cache = LruCache::new(dummy_size);
    let disk_manager = DiskManagerSync::default();
    let storage_manager = StorageManager::new(cache, disk_manager, "cache/".to_string());

    // FIXME (kunle): We need to get the file from storage manager. For now we directly read
    // the file from disk. I will update it after the storage manager provides the relevant API.
    let route = warp::path!("file" / String)
        .and(warp::path::end())
        .and_then(|file_name: String| async move {
            // let file_path = format!("storage-node/tests/parquet/{}", file_name);
            let file_path = format!("tests/parquet/{}", file_name);
            if !Path::new(&file_path).exists() {
                return Err(warp::reject::not_found());
            }
            println!("File Path: {}", file_path);
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
        });

    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use reqwest::Client;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    /// WARNING: Put userdata1.parquet in the storage-node/tests/parquet directory before running this test.
    #[tokio::test]
    async fn test_download_file() -> Result<()> {
        // Start the server
        let server_handle = tokio::spawn(async {
            storage_node_serve().await.unwrap();
        });

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let url = "http://localhost:3030/file/userdata1.parquet";
        let client = Client::new();
        let mut response = client.get(url).send().await?;
        assert!(
            response.status().is_success(),
            "Failed to download file. Status code: {}",
            response.status()
        );

        let temp_dir = tempdir()?;
        let file_path = temp_dir.path().join("userdata1.parquet");
        let mut file = File::create(&file_path)?;

        // Stream the response body and write to the file
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk)?;
        }
        assert!(file_path.exists(), "File not found after download");

        let original_file_path = "tests/parquet/userdata1.parquet";
        let original_file = fs::read(original_file_path)?;
        let downloaded_file = fs::read(file_path)?;
        assert_eq!(original_file, downloaded_file);

        server_handle.abort();

        Ok(())
    }
}
