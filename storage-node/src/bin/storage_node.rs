use log::info;
use storage_node::server::storage_node_serve;

// TODO: Add config here.

#[tokio::main]
async fn main() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();
    info!("starting storage node server...");
    storage_node_serve("0.0.0.0", 3030).await.unwrap();
}
