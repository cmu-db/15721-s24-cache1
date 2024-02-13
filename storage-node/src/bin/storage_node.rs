use storage_node::server::StorageServer;

#[tokio::main]
async fn main() {
    println!("starting the storage server...");
    let server = StorageServer::new();
    server.start().await;
}
