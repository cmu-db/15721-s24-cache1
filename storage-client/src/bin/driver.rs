use arrow_array::Float64Array;
use log::info;
use std::time::Instant;
use storage_client::{client::StorageClientImpl, StorageClient, StorageRequest};
use storage_common::init_logger;

/// This test is for benchmarking.

#[tokio::main]
async fn main() {
    init_logger();
    let storage_client =
        StorageClientImpl::new("http://44.220.220.131:8000", "http://127.0.0.1:3031")
            .expect("Failed to create storage client.");
    let start_time = Instant::now();
    // Requesting random_data_100m_0.parquet
    let request = StorageRequest::Table(10);
    let mut receiver = storage_client
        .request_data(request)
        .await
        .expect("Failed to get data from the server.");
    let mut record_batches = vec![];
    while let Some(record_batch) = receiver.recv().await {
        record_batches.push(record_batch);
    }
    info!("Time taken for 100m file: {:?}", start_time.elapsed());

    assert!(!record_batches.is_empty());

    let first_batch = &record_batches[0];
    assert_eq!(first_batch.num_columns(), 20);

    // Check the first 5 columns of the first row.
    let real_first_row = [
        0.869278151694903,
        0.5698583744743971,
        0.5731127546817466,
        0.9509491985107434,
        0.3949108352357301,
    ];
    for (i, &real_value) in real_first_row.iter().enumerate() {
        let column = first_batch
            .column(i)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(column.value(0), real_value);
    }
    info!("Succeed!")
}
