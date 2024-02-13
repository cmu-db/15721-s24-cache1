use arrow::record_batch::RecordBatch;
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use hyper::Uri;

use crate::{StorageClient, StorageRequest, StorageResult};

pub struct StorageClientImpl {
    _storage_server_endpoint: Uri,
    _catalog_server_endpoint: Uri,
}

impl StorageClientImpl {
    pub fn new(
        storage_server_endpoint_str: &str,
        catalog_server_endpoint_str: &str,
    ) -> StorageResult<Self> {
        let storage_server_endpoint = storage_server_endpoint_str.parse::<Uri>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "cannot connect to the storage server with uri {}",
                storage_server_endpoint_str
            ))
        })?;
        let catalog_server_endpoint = catalog_server_endpoint_str.parse::<Uri>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "cannot connect to the catalog server with uri {}",
                catalog_server_endpoint_str
            ))
        })?;
        Ok(Self {
            _storage_server_endpoint: storage_server_endpoint,
            _catalog_server_endpoint: catalog_server_endpoint,
        })
    }
}

#[async_trait::async_trait]
impl StorageClient for StorageClientImpl {
    async fn request_data(
        &self,
        _request: StorageRequest,
    ) -> StorageResult<SendableRecordBatchStream> {
        todo!()
    }

    async fn request_data_sync(&self, _request: StorageRequest) -> StorageResult<Vec<RecordBatch>> {
        todo!()
    }
}
