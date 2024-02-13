use crate::storage_manager::StorageManager;

// FIXME: Discuss with catalog team for more information.
pub enum RequestParams {
    File(String),
    S3(String),
}

pub struct StorageServer {
    /// We should allow concurrent requests fed into the storage manager,
    /// which should be responsible for handling multiple requests at the
    /// same time.
    storage_manager: StorageManager,
}

impl StorageServer {
    pub fn new() -> Self {
        Self {
            storage_manager: StorageManager::new(),
        }
    }

    pub async fn start(&self) {
        // 1. Start the server here and listen on the requests.
        // 2. Feed the request into the storage manager.
        todo!()
    }
}
