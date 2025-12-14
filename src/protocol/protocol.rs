use parking_lot::Mutex;
use std::sync::Arc;
use std::os::unix::io::RawFd;
use mod::connector::Conn;

// Short lived per-connection instance
pub struct Protocol {
    version: u64,
    conn: Arc<Mutex<Conn>>,
    netErr: String,
    addr: String,
    lt: Mutex<Option<Weak<LeaderTracker>>>,
}

pub struct SharedProtocol {
    pub proto: Arc<Protocol>,
}

