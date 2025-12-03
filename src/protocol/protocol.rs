use parking_lot::Mutex;
use std::sync::Arc;
use std::os::unix::io::RawFd;

pub struct Protocol {
    version: u64,
    conn: RawFd,
    netErr: String,
    addr: String,
    lt: Mutex<Option<Weak<LeaderTracker>>>,
}

pub struct SharedProtocol {
    pub proto: Arc<Protocol>,
}

