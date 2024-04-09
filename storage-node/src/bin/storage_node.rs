use log::info;
use storage_common::init_logger;
use storage_node::server::storage_node_serve;

// TODO: Add config here.

#[tokio::main]
async fn main() {
    init_logger();
    info!("starting storage node server...");
    storage_node_serve().await.unwrap();
}
