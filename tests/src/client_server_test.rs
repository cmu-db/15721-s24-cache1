/// This file serves as an integration test for the client and server.
/// WARNING: This test assumes that the data returned from the server is userdata1.parquet.
extern crate storage_client;
extern crate storage_common;
extern crate storage_node;

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use arrow::array::StringArray;
    use storage_client::client::StorageClientImpl;
    use storage_client::{StorageClient, StorageRequest};
    use storage_common::RequestParams;
    use storage_node::server::storage_node_serve;

    #[tokio::test]
    async fn test_client_server() -> Result<()> {
        let file_dir = RequestParams::File("../storage-node/tests/parquet".to_string());

        // Start the server
        let server_handle = tokio::spawn(async move {
            storage_node_serve(file_dir).await.unwrap();
        });

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let storage_client =
            StorageClientImpl::new("http://127.0.0.1:3030", "http://127.0.0.1:3031")?;
        let request = StorageRequest::Table(1);
        let mut receiver = storage_client.request_data(request).await?;
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

        Ok(())
    }
}
