use storage_node::server::storage_node_serve;

// TODO: Add config here.

#[tokio::main]
async fn main() {
    println!("starting storage node server...");
    storage_node_serve().await.unwrap();
}
