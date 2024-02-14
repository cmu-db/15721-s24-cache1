use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use hyper::Uri;
use tokio::sync::mpsc::Receiver;

use crate::{StorageClient, StorageRequest};

pub struct StorageClientImpl {
    _storage_server_endpoint: Uri,
    _catalog_server_endpoint: Uri,
}

impl StorageClientImpl {
    pub fn new(
        storage_server_endpoint_str: &str,
        catalog_server_endpoint_str: &str,
    ) -> Result<Self> {
        let storage_server_endpoint = storage_server_endpoint_str.parse::<Uri>().map_err(|_| {
            anyhow!(
                "cannot resolve storage server endpoint: {}",
                storage_server_endpoint_str
            )
        })?;
        let catalog_server_endpoint = catalog_server_endpoint_str.parse::<Uri>().map_err(|_| {
            anyhow!(
                "cannot resolve catalog server endpoint: {}",
                catalog_server_endpoint_str
            )
        })?;
        Ok(Self {
            _storage_server_endpoint: storage_server_endpoint,
            _catalog_server_endpoint: catalog_server_endpoint,
        })
    }
}

#[async_trait::async_trait]
impl StorageClient for StorageClientImpl {
    async fn request_data(&self, _request: StorageRequest) -> Result<Receiver<RecordBatch>> {
        todo!()
    }

    async fn request_data_sync(&self, _request: StorageRequest) -> Result<Vec<RecordBatch>> {
        todo!()
    }
}
