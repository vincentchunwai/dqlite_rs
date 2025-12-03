use parking_lot::Mutex;
use mod::protocol::Protocol;
use std::sync::Arc;

pub struct LeaderTracker {
    pub last_known_leader_addr: String,
    pub proto: Weak<Protocol>,
}