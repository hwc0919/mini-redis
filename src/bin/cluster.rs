use clap::Parser;
use mini_redis::raft_server;
use raft_server::config::Config;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::signal;

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    let cli = Cli::parse();

    // Check nodes
    if !cli.peer_ids.contains(&cli.node_id) {
        panic!("Error: node_id not in peer_ids");
    }
    let node_addrs: HashMap<u64, SocketAddr> = cli
        .peer_ids
        .into_iter()
        .zip(cli.peer_addrs.into_iter())
        .collect();

    let config = Config {
        node_id: cli.node_id,
        redis_port: cli.port,
        node_addrs,
    };

    raft_server::run(config, signal::ctrl_c()).await;
    Ok(())
}

#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-cluster",
    version,
    author,
    about = "A Redis cluster with distributed consensus powered by raft"
)]
struct Cli {
    #[clap(long = "node-id")]
    node_id: u64,

    #[clap(long = "port", default_value = "6379")]
    port: u16,

    #[clap(long = "peer-id", required = true)]
    peer_ids: Vec<u64>,

    #[clap(long = "peer-addr", required = true)]
    peer_addrs: Vec<SocketAddr>,
}
