use crate::{cache::lru::LruCache, disk::disk_manager::DiskManager, storage_manager::StorageManager, StorageResult};

// FIXME: Discuss with catalog team for more information.
pub enum RequestParams {
    File(String),
    S3(String),
}

pub async fn storage_node_serve() -> StorageResult<()> {
    // TODO: Read the type of the cache from config.
    let dummy_size = 10;
    let cache = LruCache::new(dummy_size);
    let mut disk_manager = DiskManager {};
    let storage_manager = StorageManager::new(cache, disk_manager);

    // TODO:
    // 1. Start the server here and listen on the requests.
    // 2. Feed the request into the storage manager.
    todo!()
}
