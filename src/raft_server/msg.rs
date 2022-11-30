use bytes::Bytes;
use raft::eraftpb::Message;
use std::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Debug)]
pub(crate) enum Msg {
    Propose { id: u64, cmd: CommandCallback },
    RaftMsg(Message),
}

impl Msg {
    pub(crate) fn new_propose(cmd: CommandCallback) -> Msg {
        static ID_MUTEX: Mutex<u64> = Mutex::new(0);
        let mut id = ID_MUTEX.lock().unwrap();
        *id += 1;
        Msg::Propose { id: *id, cmd }
    }
}

#[derive(Debug)]
pub(crate) struct CommandCallback {
    pub(crate) raw_bytes: Bytes,
    pub(crate) commit_tx: mpsc::Sender<crate::Result<crate::Frame>>,
}
