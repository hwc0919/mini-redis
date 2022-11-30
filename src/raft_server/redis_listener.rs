use super::msg::{CommandCallback, Msg};
use crate::{Command, Connection, Db, Shutdown};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tracing::{debug, error, info, instrument};

pub(crate) struct Listener {
    pub(crate) db: Db,
    pub(crate) listener: TcpListener,
    pub(crate) raft_msg_tx: mpsc::Sender<Msg>,
    pub(crate) limit_connections: Arc<Semaphore>,
    pub(crate) notify_shutdown: broadcast::Sender<()>,
    pub(crate) shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
pub(crate) struct Handler {
    db: Db,
    connection: Connection,
    raft_msg_tx: mpsc::Sender<Msg>,
    shutdown: Shutdown,
    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,
}

impl Listener {
    pub(crate) async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            let socket = self.accept().await?;
            let mut handler = Handler {
                db: self.db.clone(),
                connection: Connection::new(socket),
                raft_msg_tx: self.raft_msg_tx.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

impl Handler {
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            // self.connection.read_frame()
            let maybe_frame_bytes = tokio::select! {
                res = self.connection.read_frame_and_copy_bytes() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            let frame_bytes = match maybe_frame_bytes {
                Some(frame_bytes) => frame_bytes,
                None => return Ok(()),
            };

            let (frame, raw_bytes) = frame_bytes;
            let cmd = Command::from_frame(frame)?;
            debug!(?cmd);

            // Currently only set and publish command need to be handled by raft,
            // other commands can be applied directly.
            if match cmd {
                Command::Set(_) | Command::Publish(_) => false,
                _ => true,
            } {
                cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                    .await?;
                continue;
            }

            // Send to raft
            let (commit_tx, mut commit_rx) = mpsc::channel(1);
            let callback = CommandCallback {
                raw_bytes,
                commit_tx,
            };
            // Send command to raft proposal queue
            if let Err(e) = self.raft_msg_tx.send(Msg::new_propose(callback)).await {
                println!("Error: raft_msg_tx.send() {}", e);
                self.connection
                    .write_frame(&crate::Frame::Error("Failed to send raft msg".into()))
                    .await?;
                break;
            }
            // Wait the proposal to be committed by raft
            // The current handle must hang up here, because redis requests should be processed in order.
            match commit_rx.recv().await {
                Some(Ok(frame)) => {
                    self.connection.write_frame(&frame).await?;
                }
                Some(Err(e)) => {
                    self.connection
                        .write_frame(&crate::Frame::Error(e.to_string()))
                        .await?;
                }
                None => {
                    self.connection
                        .write_frame(&crate::Frame::Error("command callback closed".into()))
                        .await?;
                }
            }
        }

        Ok(())
    }
}
