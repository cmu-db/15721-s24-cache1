use enum_as_inner::EnumAsInner;

use crate::{
    cache::lru::LruCache, disk::disk_manager_sync::DiskManagerSync, error::ParpulseResult,
    storage_manager::StorageManager,
};

// FIXME: Discuss with catalog team for more information.
#[derive(Clone, EnumAsInner)]
pub enum RequestParams {
    File(String),
    S3(String),
}

pub async fn storage_node_serve() -> ParpulseResult<()> {
    // TODO: Read the type of the cache from config.
    let dummy_size = 10;
    let cache = LruCache::new(dummy_size);
    let disk_manager = DiskManagerSync::default();
    let storage_manager = StorageManager::new(cache, disk_manager, "cache/".to_string());

    // TODO:
    // 1. Start the server here and listen on the requests.
    // 2. Feed the request into the storage manager.
    todo!()
}
