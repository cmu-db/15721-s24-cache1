use enum_as_inner::EnumAsInner;
use std::fs;
use std::path::Path;
use storage_common::RequestParams;
use warp::{Filter, Rejection, Reply};

use crate::{
    cache::lru::LruCache, disk::disk_manager::DiskManager, error::ParpulseResult,
    storage_manager::StorageManager,
};

pub async fn storage_node_serve() -> ParpulseResult<()> {
    // TODO: Read the type of the cache from config.
    let dummy_size = 10;
    let cache = LruCache::new(dummy_size);
    let disk_manager = DiskManager::default();
    let storage_manager = StorageManager::new(cache, disk_manager, "cache/".to_string());

    // TODO (kunle): We need to get the file from storage manager. For now we directly read
    // the file from disk. I will update it after the storage manager provides the relevant API.
    let route = warp::path!("file" / String)
        .and(warp::path::end())
        .and_then(|file_name: String| async move {
            let file_path = format!("data/{}", file_name);
            if !Path::new(&file_path).exists() {
                return Err(warp::reject::not_found());
            }
            println!("File Path: {}", file_path);
            let file_content = match fs::read(&file_path) {
                Ok(content) => content,
                Err(e) => {
                    eprintln!("Error reading file: {}", e);
                    return Err(warp::reject::not_found());
                }
            };
            // Return the file content as response
            Ok::<_, Rejection>(warp::reply::with_header(
                file_content,
                "Content-Type",
                "text/plain",
            ))
        });

    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;

    Ok(())
}
