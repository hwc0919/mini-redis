use super::msg::Msg;
use crate::db::Db;
use crate::Shutdown;
use bytes::{Buf, BufMut, BytesMut};
use protobuf::Message as PbMessage;
use raft::eraftpb::Message;
use raft::eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType};
use raft::{storage::MemStorage, RawNode};
use slog::{o, Drain};
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tracing::error;

pub(crate) struct RaftNode {
    raft_group: RawNode<MemStorage>,
    db: Db,
    logger: slog::Logger,
    mailboxes: HashMap<u64, mpsc::Sender<Message>>,
    raft_msg_rx: mpsc::Receiver<Msg>,
    callbacks: HashMap<u64, mpsc::Sender<crate::Result<crate::Frame>>>,
    shutdown: Shutdown,
    /// Not used directly. When all sender is dropped, the receiver will know everything is cleaned up.
    pub(crate) shutdown_complete_tx: mpsc::Sender<()>,
}

impl RaftNode {
    pub(crate) fn new(
        id: u64,
        db: Db,
        raft_msg_rx: mpsc::Receiver<Msg>,
        mailboxes: HashMap<u64, mpsc::Sender<Message>>,
        shutdown: Shutdown,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> RaftNode {
        let nodes_ids: Vec<u64> = mailboxes.keys().cloned().collect();
        let storage = raft::storage::MemStorage::new_with_conf_state((nodes_ids, vec![]));
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
            raft_msg_rx,
            callbacks: HashMap::new(),
            shutdown,
            shutdown_complete_tx,
        }
    }

    pub(crate) async fn run(&mut self) -> crate::Result<()> {
        let tick_intv = tokio::time::Duration::from_millis(100);
        let mut next_tick = tokio::time::Instant::now() + tick_intv;

        while !self.shutdown.is_shutdown() {
            // QUESTION: will this select cause message dropping???
            select! {
                msg = self.raft_msg_rx.recv() => {
                    match msg {
                        Some(Msg::Propose { id, cmd }) => {
                            let mut context = Vec::new();
                            context.append(&mut Vec::from(self.raft_group.raft.id.to_be_bytes()));
                            context.append(&mut Vec::from(id.to_be_bytes()));
                            match self.raft_group.propose(context, Vec::from(cmd.raw_bytes)) {
                                Ok(_) => {
                                    println!("Propose success, add callback to map");
                                    self.callbacks.insert(id, cmd.commit_tx);
                                }
                                Err(e) => {
                                    eprintln!("Failed to propose, {}", e);
                                    let callback = cmd.commit_tx;

                                    let _ = callback
                                        .try_send(Ok(crate::Frame::Error("Proposal dropped.".into())));
                                }
                            }
                        }
                        Some(Msg::RaftMsg(msg)) =>
                        {
                            match self.raft_group.step(msg) {
                                Ok(_) => (),
                                Err(e) => {
                                    eprintln!("Failed to step msg, reason: {}", e);
                                }
                            };
                        }
                        None => {
                            println!("raft_msg_rx disconnected");
                            break;
                        }
                    }
                },
                _ = tokio::time::sleep_until(next_tick) => {
                    self.raft_group.tick();
                    next_tick += tick_intv;
                },
                _ = self.shutdown.recv() => {
                    break;
                }
            }

            self.on_ready();
        }

        Ok(())
    }

    /// Send raft messages to peers
    fn handle_messages(&mut self, msgs: Vec<Message>) {
        println!("Handle messages: {:?}", msgs);
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
                            if entry.context.len() == 16 {
                                let v: Vec<u8> = Vec::from(entry.context);
                                let arr: [u8; 8] = (&v[..8]).try_into().unwrap();
                                let raft_id = u64::from_be_bytes(arr);
                                // peer or self?
                                if raft_id == self.raft_group.raft.id {
                                    let arr: [u8; 8] = (&v[8..]).try_into().unwrap();
                                    let callback_id = u64::from_be_bytes(arr);
                                    if let Some(callback) = self.callbacks.remove(&callback_id) {
                                        tokio::spawn(async move {
                                            let _ = callback.send(resp).await;
                                        });
                                    }
                                }
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

    fn on_ready(&mut self) {
        if !self.raft_group.has_ready() {
            return;
        }

        // 0. Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raft_group.ready();

        // 1. Check whether messages is empty or not. If not, it means that the node will send messages to other nodes:
        if !ready.messages().is_empty() {
            // Send out the messages come from the node.
            self.handle_messages(ready.take_messages());
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
            self.handle_messages(ready.take_persisted_messages());
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
        self.handle_messages(light_rd.take_messages());
        self.handle_committed_entries(light_rd.take_committed_entries());
        self.raft_group.advance_apply();
    }
}

pub(crate) struct RaftSender {
    peer_id: u64,
    addr: SocketAddr,
    receiver: mpsc::Receiver<Message>,
    stream: Option<TcpStream>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl RaftSender {
    pub(crate) fn new(
        peer_id: u64,
        addr: SocketAddr,
        receiver: mpsc::Receiver<Message>,
        shutdown: Shutdown,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> RaftSender {
        RaftSender {
            peer_id,
            addr,
            receiver,
            stream: None,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    pub(crate) async fn run(&mut self) {
        println!("11111111111111");
        let mut last_msg = None;
        while !self.shutdown.is_shutdown() {
            println!("222222222222222");
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
                println!("333333333333333333");
                if let None = last_msg {
                    println!("RECV RECV RECV");
                    last_msg = self.receiver.recv().await;
                    println!("RaftSender ready to send message: {:?}", last_msg);
                }
                match last_msg.take() {
                    Some(msg) => {
                        let mut msg_bytes = msg.write_to_bytes().unwrap();
                        let msg_len = msg_bytes.len();
                        let mut bytes = Vec::new();
                        bytes.put_u64(msg_len as u64);
                        bytes.append(&mut msg_bytes);

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
                    None => {
                        println!("RaftSender to peer {} returned", self.peer_id);
                        return;
                    }
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
    raft_msg_tx: mpsc::Sender<Msg>,

    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl RaftReceiverHandler {
    async fn run(mut self) -> crate::Result<()> {
        let mut stream = BufWriter::new(self.stream);
        let mut msg_len: usize = 0;
        let mut buf = BytesMut::new();

        'outer: while !self.shutdown.is_shutdown() {
            tokio::select! {
                res = stream.read_buf(&mut buf) => {
                    println!("RaftReceiverHandler.read(), res: {:?}", res);
                    match res {
                        Ok(len) => {
                            if len == 0 {
                                eprintln!("Raft peer close connection");
                                break;
                            }
                        },
                        Err(e) => {
                            eprintln!("Error read from raft peer, {}", e);
                            break;
                        }
                    }
                },
                _ = self.shutdown.recv() => {
                    break;
                }
            };

            'inner: loop {
                println!("RaftReceiverHandler msg_len {}", msg_len);
                if msg_len == 0 {
                    if buf.len() < 8 {
                        break 'inner;
                    }
                    // peek u64
                    msg_len = u64::from_be_bytes((&buf[..8]).try_into().unwrap()) as usize;
                    buf.advance(8);
                }
                if buf.len() < msg_len {
                    break 'inner;
                }

                println!("Read from raft peer: {:?}", buf);
                let mut msg = Message::new();
                msg.merge_from_bytes(&buf[..msg_len]).unwrap();
                buf.advance(msg_len);
                msg_len = 0;
                if let Err(e) = self.raft_msg_tx.send(Msg::RaftMsg(msg)).await {
                    eprintln!("Failed to send through raft_msg_tx, reason: {}", e);
                    break 'outer;
                }
            }
        }

        Ok(())
    }
}

pub(crate) struct RaftReceiver {
    listener: TcpListener,
    raft_msg_tx: mpsc::Sender<Msg>,
    pub(crate) notify_shutdown: broadcast::Sender<()>,
    pub(crate) shutdown_complete_tx: mpsc::Sender<()>,
}

impl RaftReceiver {
    pub(crate) fn new(
        listener: TcpListener,
        raft_msg_tx: mpsc::Sender<Msg>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> RaftReceiver {
        RaftReceiver {
            listener,
            raft_msg_tx,
            notify_shutdown,
            shutdown_complete_tx,
        }
    }

    pub(crate) async fn run(&mut self) -> crate::Result<()> {
        loop {
            let stream = self.accept().await?;
            let handler = RaftReceiverHandler {
                stream,
                raft_msg_tx: self.raft_msg_tx.clone(),
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
