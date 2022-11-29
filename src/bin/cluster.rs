use mini_redis::raft_server;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    // Bind a TCP listener
    let redis_listener = TcpListener::bind(&format!("127.0.0.1:{}", 6379)).await?;
    let raft_listener = TcpListener::bind(&format!("127.0.0.1:{}", 63790)).await?;

    raft_server::run(redis_listener, raft_listener, signal::ctrl_c()).await;
    Ok(())
}
