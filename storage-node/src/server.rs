use enum_as_inner::EnumAsInner;

use crate::{
    cache::{
        data_store_cache::memdisk::cache_manager::MemDiskStoreCache, policy::lru::LruReplacer,
    },
    error::ParpulseResult,
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
    let cache = LruReplacer::new(dummy_size);
    // TODO: cache_base_path should be from config
    let data_store_cache = MemDiskStoreCache::new(cache, "cache/".to_string(), None, None);
    let _storage_manager = StorageManager::new(data_store_cache);

    // TODO:
    // 1. Start the server here and listen on the requests.
    // 2. Feed the request into the storage manager.
    todo!()
}
