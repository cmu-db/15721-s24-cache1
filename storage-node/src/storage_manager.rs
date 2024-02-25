use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    cache::{lru::LruCache, ParpulseCache}, disk::disk_manager::DiskManager, server::RequestParams, StorageResult
};

/// [`StorageManager`] handles the request from the storage client.
///
/// We should allow concurrent requests fed into the storage manager,
/// which should be responsible for handling multiple requests at the
/// same time.
pub struct StorageManager<C: ParpulseCache> {
    // TODO: Consider making the cache lock-free. See the comments for
    // `ParpulseCache`.
    cache: Arc<RwLock<C>>,
    disk_manager: Arc<RwLock<DiskManager>>,
}

impl<C: ParpulseCache> StorageManager<C> {
    pub fn new(cache: C, disk_manager: DiskManager) -> Self {
        Self {
            cache: Arc::new(RwLock::new(cache)),
            disk_manager: Arc::new(RwLock::new(disk_manager)),
        }
    }

    pub async fn get_data(&self, _request: RequestParams) -> StorageResult<()> {
        // TODO:
        // 1. Try to get data from the cache first.
        // 2. If cache miss, then go to storage reader to fetch the data from
        // the underlying storage.
        todo!()
    }
}
