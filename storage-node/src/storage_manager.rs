use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{cache::lru::LruCache, server::RequestParams, StorageResult};

pub struct StorageManager {
    // TODO: Might refine the lock later.
    cache: Arc<RwLock<LruCache>>,
}

impl StorageManager {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache::new())),
        }
    }

    pub async fn get_data(&self, _request: RequestParams) -> StorageResult<()> {
        todo!()
    }
}
