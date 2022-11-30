use crate::raft_server::raft_node::RaftReceiver;
use crate::{DbDropGuard, Shutdown};
use raft::eraftpb::Message;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Semaphore};

pub mod config;
mod msg;
mod raft_node;
mod redis_listener;
use config::Config;

use raft_node::RaftNode;
use raft_node::RaftSender;
use redis_listener::Listener;
use tracing::{error, info};

pub async fn run(config: Config, shutdown: impl Future) {
    // Bind a TCP listener
    let redis_listener = TcpListener::bind(&format!("127.0.0.1:{}", config.redis_port))
        .await
        .unwrap();
    let raft_addr = config.node_addrs[&config.node_id];
    let raft_listener = TcpListener::bind(raft_addr).await.unwrap();

    // Every running loop will subscribe to this notify_shutdown channel.
    // Before main loop quit, sender will be dropped and all subscribers will be notified.
    let (notify_shutdown, _) = broadcast::channel(1);
    // Every running loop will possess a copy of shutdown_complete_tx.
    // After all running loops are ended, all tx will be dropped, and `shutdown_complete_rx` in main loop will be notified,
    // which means all resources have been cleaned up.
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // Channel to bridge redis server and raft system
    let (raft_msg_tx, raft_msg_rx) = mpsc::channel(1024); // TODO: buffer size

    let mut tx_map = HashMap::new();
    for (id, addr) in config.node_addrs.into_iter() {
        let (tx, rx) = mpsc::channel::<Message>(1024);
        tx_map.insert(id, tx);

        // Create connection to remote peers
        if id != config.node_id {
            let mut raft_conn = RaftSender::new(
                id,
                addr,
                rx,
                Shutdown::new(notify_shutdown.subscribe()),
                shutdown_complete_tx.clone(),
            );
            tokio::spawn(async move {
                raft_conn.run().await;
            });
        }
    }

    // Init raft listeners

    let mut raft_receiver = RaftReceiver::new(
        raft_listener,
        raft_msg_tx.clone(),
        notify_shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    let db_holder = DbDropGuard::new();
    // TODO: init raft
    let mut raft_node = RaftNode::new(
        config.node_id,
        db_holder.db(),
        raft_msg_rx,
        tx_map,
        Shutdown::new(notify_shutdown.subscribe()),
        shutdown_complete_tx.clone(),
    );

    // Init redis
    let mut listener = Listener {
        db: db_holder.db(),
        listener: redis_listener,
        raft_msg_tx,
        limit_connections: Arc::new(Semaphore::new(256)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // let raft_group: RawNode<>;

    // Run server until one of the following happens:
    // 1. redis server quit
    // 2. raft quit
    // 3. shutdown signal received
    tokio::select! {
        res = listener.run() => {
            if let Err(err) = res {
                error!(cause = %err, "Redis listener failed to accept");
            }
        }
        res = raft_receiver.run() => {
            if let Err(err) = res {
                error!(cause = %err, "Raft receiver error");
            }
        },
        res = raft_node.run() => {
            if let Err(err) = res {
                error!(cause = %err, "Raft node error");
            }
        },
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down");
        }
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = listener;
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let RaftNode {
        shutdown_complete_tx,
        ..
    } = raft_node;
    drop(shutdown_complete_tx);

    let RaftReceiver {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = raft_receiver;
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}
