/// This file serves as an integration test for the client and server.
/// WARNING: This test assumes that the data returned from the server is userdata1.parquet.
extern crate storage_client;
extern crate storage_common;
extern crate storage_node;

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use storage_client::client::StorageClientImpl;
    use storage_client::{StorageClient, StorageRequest};
    use storage_common::init_logger;
    use storage_node::server::storage_node_serve;

    #[test]
    fn setup() {
        init_logger();
    }

    #[tokio::test]
    // #[ignore = "Need to discuss how to set S3 params"]
    async fn test_client_server() {
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

        server_handle.abort();
    }
}
