use std::collections::HashMap;
use std::net::SocketAddr;

pub struct Config {
    pub node_id: u64,
    pub redis_port: u16,
    pub node_addrs: HashMap<u64, SocketAddr>,
}
