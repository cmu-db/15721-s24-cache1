use anyhow::{anyhow, Ok, Result};
use arrow_array::RecordBatch;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use hyper::Uri;
use lazy_static::lazy_static;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use reqwest::{Client, Response, Url};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use storage_common::RequestParams;
use tempfile::tempdir;
use tokio::fs::File as AsyncFile;
use tokio::sync::mpsc::{channel, Receiver};

use crate::{StorageClient, StorageRequest, TableId};

/// The batch size for the record batch.
const BATCH_SIZE: usize = 100;
const CHANNEL_CAPACITY: usize = 10;
const PARAM_BUCKET_KEY: &str = "bucket";
const PARAM_KEYS_KEY: &str = "keys";

lazy_static! {
    static ref TABLE_FILE_MAP: HashMap<TableId, String> = {
        let mut m = HashMap::new();
        // For mock s3
        m.insert(0, "userdata1.parquet".to_string());
        // All the remainings are for real s3
        m.insert(1, "1m/random_data_1m_1.parquet".to_string());
        m.insert(2, "1m/random_data_1m_2.parquet".to_string());
        m.insert(3, "1m/random_data_1m_3.parquet".to_string());
        m.insert(4, "1m/random_data_1m_4.parquet".to_string());
        m.insert(5, "1m/random_data_1m_5.parquet".to_string());
        m.insert(6, "1m/random_data_1m_6.parquet".to_string());
        m.insert(7, "1m/random_data_1m_7.parquet".to_string());
        m.insert(8, "1m/random_data_1m_8.parquet".to_string());
        m.insert(9, "1m/random_data_1m_9.parquet".to_string());
        m.insert(10, "100m/random_data_100m_0.parquet".to_string());
        m.insert(11, "100m/random_data_100m_1.parquet".to_string());
        m.insert(12, "100m/random_data_100m_2.parquet".to_string());
        m.insert(13, "100m/random_data_100m_3.parquet".to_string());
        m.insert(14, "100m/random_data_100m_4.parquet".to_string());
        m.insert(15, "100m/random_data_100m_5.parquet".to_string());
        m.insert(16, "100m/random_data_100m_6.parquet".to_string());
        m.insert(17, "100m/random_data_100m_7.parquet".to_string());
        m.insert(18, "100m/random_data_100m_8.parquet".to_string());
        m.insert(19, "100m/random_data_100m_9.parquet".to_string());
        m
    };
}

pub struct StorageClientImpl {
    storage_server_endpoint: Uri,
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
            storage_server_endpoint,
            _catalog_server_endpoint: catalog_server_endpoint,
        })
    }

    /// Returns the physical location of the requested data in RequestParams.
    async fn get_info_from_catalog(&self, request: StorageRequest) -> Result<RequestParams> {
        let bucket = "parpulse-test".to_string();
        let table_id = match request {
            StorageRequest::Table(id) => id,
            _ => {
                return Err(anyhow!("Only table request is supported."));
            }
        };
        let keys = vec![TABLE_FILE_MAP.get(&table_id).unwrap().to_string()];
        Ok(RequestParams::S3((bucket, keys)))
    }

    async fn get_data_from_response(response: Response) -> Result<Receiver<RecordBatch>> {
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

    async fn get_info_from_catalog_test(&self, request: StorageRequest) -> Result<RequestParams> {
        let bucket = "tests-parquet".to_string();
        let table_id = match request {
            StorageRequest::Table(id) => id,
            _ => {
                return Err(anyhow!("Only table request is supported."));
            }
        };
        let keys = vec![TABLE_FILE_MAP.get(&table_id).unwrap().to_string()];
        Ok(RequestParams::MockS3((bucket, keys)))
    }

    fn get_request_url_and_params(
        &self,
        location: (String, Vec<String>),
    ) -> (String, Vec<(&str, String)>) {
        let url = format!("{}file", self.storage_server_endpoint);
        let params = vec![
            (PARAM_BUCKET_KEY, location.0),
            (PARAM_KEYS_KEY, location.1.join(",")),
        ];
        (url, params)
    }

    pub async fn request_data_test(
        &self,
        request: StorageRequest,
    ) -> Result<Receiver<RecordBatch>> {
        // First we need to get the location of the parquet file from the catalog server.
        let location = match self.get_info_from_catalog_test(request).await? {
            RequestParams::MockS3(location) => location,
            _ => {
                return Err(anyhow!(
                    "Failed to get location of the file from the catalog server."
                ));
            }
        };

        // Then we need to send the request to the storage server.
        let client = Client::new();
        let (url, mut params) = self.get_request_url_and_params(location);
        params.push(("is_test", "true".to_owned()));

        let url = Url::parse_with_params(&url, params)?;
        let response = client.get(url).send().await?;

        Self::get_data_from_response(response).await
    }
}

#[async_trait::async_trait]
impl StorageClient for StorageClientImpl {
    async fn request_data(&self, request: StorageRequest) -> Result<Receiver<RecordBatch>> {
        // First we need to get the location of the parquet file from the catalog server.
        let location = match self.get_info_from_catalog(request).await? {
            RequestParams::S3(location) => location,
            _ => {
                return Err(anyhow!(
                    "Failed to get location of the file from the catalog server."
                ));
            }
        };

        // Then we need to send the request to the storage server.
        let client = Client::new();
        let (url, params) = self.get_request_url_and_params(location);
        let url = Url::parse_with_params(&url, params)?;
        let response = client.get(url).send().await?;
        Self::get_data_from_response(response).await
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

    /// WARNING: Put userdata1.parquet in the storage-node/tests/parquet directory before running this test.
    #[tokio::test]
    async fn test_storage_client_disk() {
        // Create a mock server to serve the parquet file.
        let mut server = Server::new_async().await;
        println!("server host: {}", server.host_with_port());
        server
            .mock(
                "GET",
                "/file?bucket=tests-parquet&keys=userdata1.parquet&is_test=true",
            )
            .with_body_from_file("../storage-node/tests/parquet/userdata1.parquet")
            .create_async()
            .await;

        let server_endpoint = server.url() + "/";
        let storage_client = StorageClientImpl::new(&server_endpoint, "localhost:3031")
            .expect("Failed to create storage client.");
        // 0 is the table id for userdata1.parquet on local disk.
        let request = StorageRequest::Table(0);
        let mut receiver = storage_client
            .request_data_test(request)
            .await
            .expect("Failed to get data from the server.");
        let mut record_batches = vec![];
        while let Some(record_batch) = receiver.recv().await {
            record_batches.push(record_batch);
        }
        assert!(!record_batches.is_empty());

        let first_batch = &record_batches[0];
        assert_eq!(first_batch.num_columns(), 13);

        let real_first_names = StringArray::from(vec!["Amanda", "Albert", "Evelyn"]);
        let read_last_names = StringArray::from(vec!["Jordan", "Freeman", "Morgan"]);
        let first_names = first_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let last_names = first_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Check the first three entries in the first and last name columns.
        for i in 0..3 {
            assert_eq!(first_names.value(i), real_first_names.value(i));
            assert_eq!(last_names.value(i), read_last_names.value(i));
        }
    }
}
