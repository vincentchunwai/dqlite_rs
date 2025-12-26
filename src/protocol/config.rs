use std::time::Duration;
use crate::protocol::connector::DialFunc;

#[derive(Debug, Clone)]
pub struct Config {
    pub dial: Option<Arc<dyn DialFunc>>,
    pub dial_timeout: Duration,
    pub attempt_timeout: Duration,
    pub backoff_factor: Duration,
    pub backoff_cap: Duration,
    pub retry_limit: Option<u32>,
    pub concurrent_leader_conns: u64,
    pub permit_shared: bool,
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_dial(mut self, dial: Arc<dyn DialFunc>) -> Self {
        self.dial = Some(dial);
        self
    }

    pub fn with_dial_timeout(mut self, timeout: Duration) -> Self {
        self.dial_timeout = timeout;
        self
    }

    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = timeout;
        self
    }

    pub fn with_backoff_factor(mut self, factor: Duration) -> Self {
        self.backoff_factor = factor;
        self
    }

    pub fn with_backoff_cap(mut self, cap: Duration) -> Self {
        self.backoff_cap = cap;
        self
    }

    pub fn with_retry_limit(mut self, limit: u32) -> Self {
        self.retry_limit = Some(limit);
        self
    }

    pub fn with_concurrent_leader_conns(mut self, conns: u64) -> Self {
        self.concurrent_leader_conns = conns;
        self
    }

    pub fn with_permit_shared(mut self, permit: bool) -> Self {
        self.permit_shared = permit;
        self
    }

    pub fn with_defaults(mut self, default_dial: Arc<dyn DialFunc>) -> Self {
        if self.dial.is_none() {
            self.dial = Some(default_dial);
        }
        if self.dial_timeout.is_zero() {
            self.dial_timeout = Duration::from_secs(5);
        }
        if self.attempt_timeout.is_zero() {
            self.attempt_timeout = Duration::from_secs(15);
        }
        if self.backoff_factor.is_zero() {
            self.backoff_factor = Duration::from_millis(100);
        }
        if self.backoff_cap.is_zero() {
            self.backoff_cap = Duration::from_secs(1);
        }
        if self.retry_limit.is_none() {
            self.retry_limit = Some(10);
        }
        if self.concurrent_leader_conns == 0 {
            self.concurrent_leader_conns = 10;
        }
        self
    }
}