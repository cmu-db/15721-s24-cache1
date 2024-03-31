use storage_common::RequestParams;
use storage_node::server::storage_node_serve;

// TODO: Add config here.

#[tokio::main]
async fn main() {
    println!("starting storage node server...");
    let file_dir = RequestParams::File("storage-node/tests/parquet".to_string());
    storage_node_serve(file_dir).await.unwrap();
}
