use anyhow::{anyhow, Ok, Result};
use arrow_array::RecordBatch;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use hyper::Uri;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use reqwest::Client;
use std::fs::File;
use std::io::Write;
use storage_common::RequestParams;
use tempfile::tempdir;
use tokio::fs::File as AsyncFile;
use tokio::sync::mpsc::{channel, Receiver};

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
    async fn get_info_from_catalog(&self, _request: StorageRequest) -> Result<RequestParams> {
        // TODO: Need to discuss with the catalog team.
        // The following line serves as a placeholder for now.
        Ok(RequestParams::File("random_data.parquet".to_string()))
    }

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
        println!("Requesting data from: {}", url);
        let response = client.get(url).send().await?;
        if response.status().is_success() {
            // Store the streamed Parquet file in a temporary file.
            let temp_dir = tempdir()?;
            let file_path = temp_dir.path().join("tmp.parquet");
            let mut file = File::create(&file_path)?;

            // Copy the streamed content to the temporary file
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
                .with_batch_size(3);

            let mask = ProjectionMask::all();
            let stream = builder.with_projection(mask).build().unwrap();
            let results = stream.try_collect::<Vec<_>>().await.unwrap();

            // Return the record batch as a stream.
            let (tx, rx) = channel(1);
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

    #[tokio::test]
    async fn test_storage_client() -> Result<()> {
        let storage_client =
            StorageClientImpl::new("http://localhost:3030", "http://localhost:3031")?;
        let request = StorageRequest::Table(1);
        let mut receiver = storage_client.request_data(request).await?;
        let mut record_batches = vec![];
        while let Some(record_batch) = receiver.recv().await {
            record_batches.push(record_batch);
        }
        println!("Record batches: {:?}", record_batches);
        assert!(!record_batches.is_empty());
        Ok(())
    }

    #[tokio::test]
    /// Put random_data.csv (can have anything inside) under `data` before
    /// running this test.
    /// This test ONLY tests the correctness of the server.
    async fn test_download_file() -> Result<()> {
        let url = "http://localhost:3030/file/random_data.csv";
        let client = Client::new();
        let mut response = client.get(url).send().await?;

        assert!(
            response.status().is_success(),
            "Failed to download file. Status code: {}",
            response.status()
        );

        let temp_dir = tempdir()?;
        let file_path = temp_dir.path().join("random_data.csv");

        let mut file = File::create(&file_path)?;

        // Stream the response body and write to the file
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk)?;
        }

        assert!(file_path.exists(), "File not found after download");

        Ok(())
    }
}
