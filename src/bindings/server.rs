use crate::bindings::{
    dqlite_node, dqlite_node_create, dqlite_node_destroy, dqlite_node_errmsg, dqlite_node_id,
    dqlite_node_set_network_latency, dqlite_node_set_snapshot_params_v2, dqlite_node_start,
    dqlite_node_stop, dqlite_note_set_bind_address, DQLITE_ERROR, DQLITE_MISUSE, DQLITE_NOMEM,
    DQLITE_OK, DQLITE_SNAPSHOT_TRAILING_DYNAMIC, DQLITE_SNAPSHOT_TRAILING_STATIC,
};
use libc::{SIGPIPE, SIG_IGN};
use std::ffi::{CStr, CString};
use std::fmt;
use std::os::unix::io::RawFd;
use std::ptr;
use std::sync::{Arc, Mutex};

// SIGPIPE terminates the process when a client disconnects while the server is writing a response
// Ignore SIGPIPE globally at startup
fn ignore_sigpipe() {
    unsafe {
        libc::signal(SIGPIPE, SIG_IGN);
    }
}

#[derive(Debug, Clone)]
pub enum DqliteError {
    NodeCreation(String),
    Configuration(String),
    Start(String),
    Stop(String),
    NulError(std::ffi::NulError),
}

impl From<std::ffi::NulError> for DqliteError {
    fn from(err: std::ffi::NulError) -> Self {
        DqliteError::NulError(err)
    }
}

impl fmt::Display for DqliteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DqliteError::NodeCreation(msg) => write!(f, "Node creation failed: {}", msg),
            DqliteError::Configuration(msg) => write!(f, "Configuration failed: {}", msg),
            DqliteError::Start(msg) => write!(f, "Start failed: {}", msg),
            DqliteError::Stop(msg) => write!(f, "Stop failed: {}", msg),
            DqliteError::NulError(err) => write!(f, "Nul error: {}", err),
        }
    }
}

impl std::error::Error for DqliteError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TrailingStrategy {
    Static,
    Dynamic,
}

impl TrailingStrategy {
    pub fn to_c_int(self) -> std::os::raw::c_int {
        match self {
            TrailingStrategy::Static => DQLITE_SNAPSHOT_TRAILING_STATIC as std::os::raw::c_int,
            TrailingStrategy::Dynamic => DQLITE_SNAPSHOT_TRAILING_DYNAMIC as std::os::raw::c_int,
        }
    }

    pub fn from_c_int(value: std::os::raw::c_int) -> Option<Self> {
        match value {
            0 => Some(TrailingStrategy::Static),
            1 => Some(TrailingStrategy::Dynamic),
            _ => None,
        }
    }
}

impl From<TrailingStrategy> for std::os::raw::c_int {
    fn from(strategy: TrailingStrategy) -> Self {
        strategy.to_c_int()
    }
}

#[repr(C)]
struct SnapShotParams {
    threshold: u64,
    trailing: u64,
    strategy: TrailingStrategy,
}

pub struct Node {
    node: *mut dqlite_node,
    connect_registry: Arc<Mutex<ConnectRegistry>>,
}

type ConnectRegistry =
    std::collections::HashMap<u64, Box<dyn Fn(&str) -> Result<RawFd, String> + Send + Sync>>;


impl Node {
    pub fn new(id: u64, address: &str, dir: &str) -> Result<Self, DqliteError> {
        let c_address = CString::new(address)?;
        let c_dir = CString::new(dir)?;
        let c_id = id as dqlite_node_id;

        let mut node_ptr: *mut dqlite_node = ptr::null_mut();

        let rc =
            unsafe { dqlite_node_create(c_id, c_address.as_ptr(), c_dir.as_ptr(), &mut node_ptr) };

        if rc != 0 {
            let err_msg = unsafe {
                CStr::from_ptr(dqlite_node_errmsg(node_ptr))
                    .to_string_lossy()
                    .into_owned()
            };

            unsafe { dqlite_node_destroy(node_ptr) };
            return Err(DqliteError::NodeCreation(err_msg));
        }

        Ok(Node {
            node: node_ptr,
            connect_registry: Arc::new(Mutex::new(ConnectRegistry::new())),
        })
    }

    pub fn set_bind_address(&self, address: &str) -> Result<(), DqliteError> {
        let c_address = CString::new(address)?;
        let rc = unsafe { dqlite_node_set_bind_address(self.node, c_address.as_ptr()) };

        if rc != 0 {
            let err_msg = unsafe {
                CStr::from_ptr(dqlite_node_errmsg(self.node))
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(DqliteError::Configuration(format!(
                "Failed to set bind address: {}",
                err_msg
            )));
        }
        Ok(())
    }

    pub fn set_network_latency(&self, nanoseconds: u64) -> Result<(), DqliteError> {
        let rc = unsafe { dqlite_node_set_network_latency(self.node, nanoseconds) };
        if rc != 0 {
            let err_msg = unsafe {
                CStr::from_ptr(dqlite_node_errmsg(self.node))
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(DqliteError::Configuration(format!(
                "Failed to set network latency: {}",
                err_msg
            )));
        }
        Ok(())
    }

    pub fn set_snapshot_params(&self, params: SnapShotParams) -> Result<(), DqliteError> {
        let threshold = params.threshold as u32;
        let trailing = params.trailing as u32;
        let strategy = params.strategy.to_c_int();

        let rc =
            unsafe { dqlite_node_set_snapshot_params_v2(self.node, threshold, trailing, strategy) };
        if rc != 0 {
            let err_msg = unsafe {
                CStr::from_ptr(dqlite_node_errmsg(self.node))
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(DqliteError::Configuration(format!(
                "Failed to set snapshot params: {}",
                err_msg
            )));
        }
        Ok(())
    }

    pub fn start(&self) -> Result<(), DqliteError> {
        let rc = unsafe { dqlite_node_start(self.node) };
        if rc != 0 {
            let err_msg = unsafe {
                CStr::from_ptr(dqlite_node_errmsg(self.node))
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(DqliteError::Start(err_msg));
        }

        Ok(())
    }

    pub fn stop(&self) -> Result<(), DqliteError> {
        let rc = unsafe { dqlite_node_stop(self.node) };
        if rc != 0 {
            let err_msg = unsafe {
                CStr::from_ptr(dqlite_node_errmsg(self.node))
                    .to_string_lossy()
                    .into_owned()
            };
            return Err(DqliteError::Stop(err_msg));
        }
        Ok(())
    }
}

// RAII wrapper for dqlite_node
impl Drop for Node {
    fn drop(&mut self) {
        if !self.node.is_null() {
            unsafe {
                dqlite_node_destroy(self.node);
            }
        }
    }
}

impl !Clone for Node {}
