use super::msg::Msg;
use crate::db::Db;
use crate::Shutdown;
use protobuf::Message as PbMessage;
use raft::eraftpb::Message;
use raft::eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType};
use raft::{storage::MemStorage, RawNode};
use slog::{o, Drain};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tracing::error;

pub(crate) struct RaftNode {
    raft_group: RawNode<MemStorage>,
    db: Db,
    logger: slog::Logger,
    mailboxes: HashMap<u64, mpsc::Sender<Message>>,
    request_receiver: mpsc::Receiver<Msg>,
    shutdown: Shutdown,
    /// Not used directly. When all sender is dropped, the receiver will know everything is cleaned up.
    pub(crate) shutdown_complete_tx: mpsc::Sender<()>,
}

impl RaftNode {
    pub(crate) fn new(
        id: u64,
        db: Db,
        request_receiver: mpsc::Receiver<Msg>,
        mailboxes: HashMap<u64, mpsc::Sender<Message>>,
        shutdown: Shutdown,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> RaftNode {
        let storage = raft::storage::MemStorage::new_with_conf_state((vec![id], vec![]));
        let mut cfg = raft::Config::default();
        cfg.id = id;
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain)
            .chan_size(4096)
            .overflow_strategy(slog_async::OverflowStrategy::Block)
            .build()
            .fuse();
        let logger = slog::Logger::root(drain, o!());

        let logger = logger.new(o!("tag" => format!("peer_{}", id)));
        let raft_group = raft::RawNode::new(&cfg, storage, &logger).unwrap();

        RaftNode {
            raft_group,
            db,
            logger,
            mailboxes,
            request_receiver,
            shutdown,
            shutdown_complete_tx,
        }
    }

    pub(crate) async fn run(&mut self) -> crate::Result<()> {
        let tick_intv = tokio::time::Duration::from_millis(100);
        let mut next_tick = tokio::time::Instant::now() + tick_intv;
        while !self.shutdown.is_shutdown() {
            let msg = select! {
                _ = tokio::time::sleep_until(next_tick) => {
                    self.raft_group.tick();
                    next_tick += tick_intv;
                    continue;
                },
                _ = self.shutdown.recv() => {
                    break;
                }
                msg = self.request_receiver.recv() => match msg {
                    Some(msg) => msg,
                    None => break
                }
            };
            match msg {
                Msg::Propose { id, cmd } => {
                    self.raft_group
                        .propose(Vec::from(id.to_be_bytes()), Vec::from(cmd.raw_bytes))
                        .unwrap();
                }
                Msg::RaftMsg(msg) => {
                    self.raft_group.step(msg).unwrap();
                }
            }

            self.on_ready().await;
        }

        Ok(())
    }

    /// Send raft messages to peers
    async fn handle_messages(&mut self, msgs: Vec<Message>) {
        for msg in msgs {
            let to = msg.to;
            if let Err(e) = self.mailboxes[&to].try_send(msg) {
                slog::error!(
                    self.logger,
                    "send raft message to {} fail, let Raft retry it. Reason: {}",
                    to,
                    e
                );
            }
        }
    }

    fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) {
        let mut _last_apply_index = 0;

        for entry in committed_entries {
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryConfChange => {
                    // For conf change messages, make them effective.
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.raft_group.apply_conf_change(&cc).unwrap();
                    self.raft_group.mut_store().wl().set_conf_state(cs);
                }
                EntryType::EntryConfChangeV2 => {
                    // For conf change messages, make them effective.
                    let mut cc = ConfChangeV2::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.raft_group.apply_conf_change(&cc).unwrap();
                    self.raft_group.mut_store().wl().set_conf_state(cs);
                }
                EntryType::EntryNormal => {
                    // TODO: !!!!!!!!!!!! handle your app logic
                    println!("Handle entry {:?}", entry);
                    let mut cursor = std::io::Cursor::new(&entry.data[..]);
                    match crate::Frame::parse(&mut cursor) {
                        Ok(frame) => {
                            println!("Parsed redis frame from entry. Frame: {}", frame);
                            let cmd = crate::Command::from_frame(frame).unwrap();
                            println!("Parsed redis cmd from entry. cmd: {:?}", cmd);
                            let resp = cmd.apply_cmd(&self.db);
                            if !entry.context.is_empty() {
                                // TODO: peer or self?
                                // TODO: handle error?
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Failed to parse frame from entry data. Entry: {:?}, reason: {}",
                                entry, e
                            );
                        }
                    }
                }
            };
        }
    }

    async fn on_ready(&mut self) {
        if !self.raft_group.has_ready() {
            return;
        }

        // 0. Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raft_group.ready();

        // 1. Check whether messages is empty or not. If not, it means that the node will send messages to other nodes:
        if !ready.messages().is_empty() {
            // Send out the messages come from the node.
            self.handle_messages(ready.take_messages()).await;
        }

        // 2. Check whether snapshot is empty or not.
        // If not empty, it means that the Raft node has received a Raft snapshot
        // from the leader and we must apply the snapshot:
        if !ready.snapshot().is_empty() {
            self.raft_group
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot().clone())
                .unwrap();
        }

        // 3. Check whether committed_entires is empty or not.
        // If not, it means that there are some newly committed log entries which you must apply to the state machine.
        // Of course, after applying, you need to update the applied index and resume apply later:
        self.handle_committed_entries(ready.take_committed_entries());

        // 4. Check whether entries is empty or not.
        // If not empty, it means that there are newly added entries but have not been committed yet,
        // we must append the entries to the Raft log:
        if !ready.entries().is_empty() {
            // Append entries to the Raft log
            self.raft_group
                .mut_store()
                .wl()
                .append(ready.entries())
                .unwrap();
        }

        // 5. Check whether hs is empty or not. If not empty,
        // it means that the HardState of the node has changed.
        // For example, the node may vote for a new leader,
        // or the commit index has been increased.
        // We must persist the changed HardState:
        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            self.raft_group.mut_store().wl().set_hardstate(hs.clone());
        }

        // 6. Check whether persisted_messages is empty or not.
        // If not, it means that the node will send messages to other nodes
        // after persisting hardstate,entries and snapshot:
        if !ready.persisted_messages().is_empty() {
            // Send persisted messages to other peers.
            self.handle_messages(ready.take_persisted_messages()).await;
        }

        // 7. Call advance to notify that the previous work is completed.
        // Get the return value LightReady and handle its messages and committed_entries
        // like step 1 and step 3 does. Then call advance_apply to advance the applied index inside.
        let mut light_rd = self.raft_group.advance(ready);
        // Update commit index.
        // QUESTION: this step is not in doc???
        if let Some(commit) = light_rd.commit_index() {
            self.raft_group
                .mut_store()
                .wl()
                .mut_hard_state()
                .set_commit(commit);
        }
        self.handle_messages(light_rd.take_messages()).await;
        self.handle_committed_entries(light_rd.take_committed_entries());
        self.raft_group.advance_apply();
    }
}

pub(crate) struct RaftSender {
    addr: SocketAddr,
    receiver: mpsc::Receiver<Message>,
    stream: Option<TcpStream>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl RaftSender {
    pub(crate) fn new(
        addr: &str,
        receiver: mpsc::Receiver<Message>,
        shutdown: Shutdown,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> RaftSender {
        let addr = SocketAddr::from_str(addr).unwrap();

        RaftSender {
            addr,
            receiver,
            stream: None,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut last_msg = None;
        while !self.shutdown.is_shutdown() {
            let stream = match &mut self.stream {
                Some(stream) => stream,
                None => {
                    select! {
                        stream = RaftSender::connect(&self.addr) => {
                            self.stream = Some(stream);
                            continue;
                        }
                        _ = self.shutdown.recv() => {
                            return;
                        }
                    }
                }
            };

            'inner: loop {
                if let None = last_msg {
                    last_msg = self.receiver.recv().await;
                }
                match last_msg.take() {
                    Some(msg) => {
                        let bytes = msg.write_to_bytes().unwrap();
                        if let Err(e) = stream.write(&bytes[..]).await {
                            // Connection disconnected
                            eprintln!("Failed to write to peer, {}", e);
                            self.stream = None;
                            break 'inner;
                        }
                        // Send success
                        last_msg = None;
                    }
                    // Channel closed, stop running.
                    None => return,
                }
            }
        }
    }

    async fn connect(addr: &SocketAddr) -> TcpStream {
        loop {
            match TcpStream::connect(addr).await {
                Ok(stream) => return stream,
                Err(e) => {
                    eprintln!("Failed to connect to addr {}, reason: {}", addr, e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };
        }
    }
}

struct RaftReceiverHandler {
    stream: TcpStream,
    message_sender: mpsc::Sender<Msg>,

    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl RaftReceiverHandler {
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            // TODO: read from peer
            // Two ways: use a rpc crate, or encapsulate a Connection struct
            let mut buf = bytes::BytesMut::new();
            tokio::select! {
                res = self.stream.read(&mut buf) => {
                      res?
                },
                _ = self.shutdown.recv() => {
                    break;
                }
            };

            let mut msg = Message::new();
            msg.merge_from_bytes(&buf[..]).unwrap();
            if let Err(e) = self.message_sender.send(Msg::RaftMsg(msg)).await {
                eprintln!("Failed to send through message_sender, reason: {}", e);
                break;
            }
        }

        Ok(())
    }
}

pub(crate) struct RaftReceiver {
    listener: TcpListener,
    message_sender: mpsc::Sender<Msg>,
    pub(crate) notify_shutdown: broadcast::Sender<()>,
    pub(crate) shutdown_complete_tx: mpsc::Sender<()>,
}

impl RaftReceiver {
    pub(crate) fn new(
        listener: TcpListener,
        message_sender: mpsc::Sender<Msg>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> RaftReceiver {
        RaftReceiver {
            listener,
            message_sender,
            notify_shutdown,
            shutdown_complete_tx,
        }
    }

    pub(crate) async fn run(&mut self) -> crate::Result<()> {
        loop {
            let stream = self.accept().await?;
            let mut handler = RaftReceiverHandler {
                stream,
                message_sender: self.message_sender.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
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
