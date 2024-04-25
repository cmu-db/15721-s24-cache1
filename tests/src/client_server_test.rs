/// This file serves as an integration test for the client and server.
/// WARNING: This test assumes that the data returned from the server is userdata1.parquet.
extern crate storage_client;
extern crate storage_common;
extern crate storage_node;

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, StringArray};
    use serial_test::serial;
    use storage_client::client::StorageClientImpl;
    use storage_client::{StorageClient, StorageRequest};
    use storage_common::init_logger;
    use storage_node::server::storage_node_serve;

    #[test]
    fn setup() {
        init_logger();
    }

    #[tokio::test]
    #[serial]
    async fn test_client_server_disk() {
        // The file dir should start from storage-node.
        // Start the server
        let server_handle = tokio::spawn(async move {
            storage_node_serve().await.unwrap();
        });

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let storage_client =
            StorageClientImpl::new("http://127.0.0.1:3030", "http://127.0.0.1:3031")
                .expect("Failed to create storage client.");
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

        server_handle.abort();
    }

    #[tokio::test]
    #[serial]
    async fn test_client_server_s3() {
        // Start the server
        let server_handle = tokio::spawn(async move {
            storage_node_serve().await.unwrap();
        });

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let storage_client =
            StorageClientImpl::new("http://127.0.0.1:3030", "http://127.0.0.1:3031")
                .expect("Failed to create storage client.");
        // Requesting random_data_1m_1.parquet
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
        assert_eq!(first_batch.num_columns(), 20);

        // Check the first 5 columns of the first row.
        let real_first_row = [0.191954, 0.481544, 0.470787, 0.779391, 0.218772];
        for (i, &real_value) in real_first_row.iter().enumerate() {
            let column = first_batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(column.value(0), real_value);
        }

        server_handle.abort();
    }
}
