use anyhow::{anyhow, Ok, Result};
use arrow_array::RecordBatch;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use hyper::Uri;
use log::info;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use reqwest::Client;
use std::fs::File;
use std::io::Write;
use storage_common::RequestParams;
use tempfile::tempdir;
use tokio::fs::File as AsyncFile;
use tokio::sync::mpsc::{channel, Receiver};

use crate::{StorageClient, StorageRequest};

/// The batch size for the record batch.
const BATCH_SIZE: usize = 3;
const CHANNEL_CAPACITY: usize = 10;

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

    /// Returns the physical location of the requested data in RequestParams.
    async fn get_info_from_catalog(&self, _request: StorageRequest) -> Result<RequestParams> {
        // FIXME (kunle): Need to discuss with the catalog team.
        // The following line serves as a placeholder for now.
        Ok(RequestParams::File("userdata1.parquet".to_string()))
    }
}

#[async_trait::async_trait]
impl StorageClient for StorageClientImpl {
    async fn request_data(&self, _request: StorageRequest) -> Result<Receiver<RecordBatch>> {
        // First we need to get the location of the parquet file from the catalog server.
        let _location = match self.get_info_from_catalog(_request).await? {
            RequestParams::File(location) => location,
            _ => {
                return Err(anyhow!(
                    "Failed to get location of the file from the catalog server."
                ));
            }
        };

        // Then we need to send the request to the storage server.
        let url = format!("{}file/{}", self._storage_server_endpoint, _location);
        let client = Client::new();
        info!("Requesting data from: {}", url);
        let response = client.get(url).send().await?;
        if response.status().is_success() {
            // Store the streamed Parquet file in a temporary file.
            // FIXME: 1. Do we really need streaming here?
            //       2. Do we need to store the file in a temporary file?
            let temp_dir = tempdir()?;
            let file_path = temp_dir.path().join("tmp.parquet");
            let mut file = File::create(&file_path)?;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)?;
            }

            // Convert the Parquet file to a record batch.
            let file = AsyncFile::open(file_path).await.unwrap();
            let builder = ParquetRecordBatchStreamBuilder::new(file)
                .await
                .unwrap()
                .with_batch_size(BATCH_SIZE);

            let mask = ProjectionMask::all();
            let stream = builder.with_projection(mask).build().unwrap();
            let results = stream.try_collect::<Vec<_>>().await.unwrap();

            // Return the record batch as a stream.
            let (tx, rx) = channel(CHANNEL_CAPACITY);
            tokio::spawn(async move {
                for result in results {
                    tx.send(result).await.unwrap();
                }
            });
            Ok(rx)
        } else {
            Err(anyhow::anyhow!(
                "Failed to download file. Response: {:?}",
                response.status()
            ))
        }
    }

    // TODO (kunle): I don't think this function is necessary.
    async fn request_data_sync(&self, _request: StorageRequest) -> Result<Vec<RecordBatch>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use mockito::Server;
    use storage_common::init_logger;

    #[test]
    fn setup() {
        init_logger();
    }

    /// WARNING: Put userdata1.parquet in the storage-node/tests/parquet directory before running this test.
    #[tokio::test]
    async fn test_storage_client_wo_ee_catalog() {
        // Create a mock server to serve the parquet file.
        let mut server = Server::new_async().await;
        info!("server host: {}", server.host_with_port());
        server
            .mock("GET", "/file/userdata1.parquet")
            .with_body_from_file("../storage-node/tests/parquet/userdata1.parquet")
            .create_async()
            .await;

        let server_endpoint = server.url() + "/";
        // The catalog endpoint is not used anyway. So randomly pass a url.
        let storage_client = StorageClientImpl::new(&server_endpoint, "localhost:3031")
            .expect("Failed to create storage client.");
        let request = StorageRequest::Table(1);
        let mut receiver = storage_client
            .request_data(request)
            .await
            .expect("Failed to get data from the server.");
        let mut record_batches = vec![];
        while let Some(record_batch) = receiver.recv().await {
            record_batches.push(record_batch);
        }
        assert!(!record_batches.is_empty());

        let first_batch = &record_batches[0];
        assert_eq!(first_batch.num_columns(), 13);

        let first_names = StringArray::from(vec!["Amanda", "Albert", "Evelyn"]);
        let last_names = StringArray::from(vec!["Jordan", "Freeman", "Morgan"]);
        assert_eq!(
            first_batch.column(2).as_any().downcast_ref::<StringArray>(),
            Some(&first_names)
        );
        assert_eq!(
            first_batch.column(3).as_any().downcast_ref::<StringArray>(),
            Some(&last_names)
        );
    }
}
