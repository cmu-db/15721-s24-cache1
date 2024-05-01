use clap::Parser;
use log::info;
use storage_node::{common::config::ParpulseConfig, server::storage_node_serve};

#[tokio::main]
async fn main() {
    // Init log.
    if let Err(e) = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()
    {
        println!("Failed to init logger: {:?}", e);
    }
    info!("starting storage node server...");
    let config = ParpulseConfig::parse();
    storage_node_serve("0.0.0.0", 3030, config).await.unwrap();
}
