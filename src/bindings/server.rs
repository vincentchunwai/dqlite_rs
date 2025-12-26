use crate::bindings::{
    dqlite_node, dqlite_node_create, dqlite_node_destroy, dqlite_node_errmsg, dqlite_node_id,
    dqlite_node_set_network_latency, dqlite_node_set_snapshot_params_v2, dqlite_node_start,
    dqlite_node_stop, dqlite_node_set_bind_address, 
    dqlite_node_set_connect_func, dqlite_node_set_failure_domain,
    dqlite_node_set_busy_timeout, dqlite_node_set_block_size,
    dqlite_node_get_bind_address, dqlite_node_describe_last_entry,
    dqlite_generate_node_id,
    DQLITE_ERROR, DQLITE_MISUSE, DQLITE_NOMEM,
    DQLITE_OK, DQLITE_SNAPSHOT_TRAILING_DYNAMIC, DQLITE_SNAPSHOT_TRAILING_STATIC,
};
use libc::{SIGPIPE, SIG_IGN};
use std::ffi::{CStr, CString};
use std::fmt;
use crate::protocol::connector::Conn;
use crate::protocol::connector::DialFunc;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use tokio::sync::CancellationToken;
use tokio::time::{timeout, Duration};
use tokio::runtime::Handle;

type ConnectHandle = u64;
type ConnectRegistry = HashMap<ConnectHandle, Box<DialFunc>>;
type ContextRegistry = HashMap<ConnectHandle, Arc<CancellationToken>>;
type RaftLogIndex = u64;
type RaftLogTerm = u64;

// Global registry for connect functions
lazy_static! {
    static ref CONNECT_REGISTRY: Arc<Mutex<ConnectRegistry>> = {
        Arc::new(Mutex::new(HashMap::new()))
    };

    static ref CONTEXT_REGISTRY: Arc<Mutex<ContextRegistry>> = {
        Arc::new(Mutex::new(HashMap::new()))
    };

    static ref RUNTIME_HANDLE: Arc<Mutex<Option<Handle>>> = {
        Arc::new(Mutex::new(None))
    };
}

static CONNECT_INDEX: AtomicU64 = AtomicU64::new(100);

// Initialize the runtime handle
pub fn init_runtime_handle(handle: Handle) {
    let mut rt = RUNTIME_HANDLE.lock().unwrap();
    *rt = Some(handle);
}

// SIGPIPE terminates the process when a client disconnects while the server is writing a response
// Ignore SIGPIPE globally at startup
fn ignore_sigpipe() {
    unsafe {
        libc::signal(SIGPIPE, SIG_IGN);
    }
}

// Helper function to safely extract error messages from dqlite_node
fn get_node_error(node: *mut dqlite_node, default_msg: &str) -> String {
    unsafe {
        let err_ptr = dqlite_node_errmsg(node);
        if err_ptr.is_null() {
            default_msg.to_string()
        } else {
            CStr::from_ptr(err_ptr)
                .to_string_lossy()
                .into_owned()
        }
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
    cancel_token: Arc<CancellationToken>,
}


impl Node {
    pub fn new(id: u64, address: &str, dir: &str) -> Result<Self, DqliteError> {
        let c_address = CString::new(address)?;
        let c_dir = CString::new(dir)?;
        let c_id = id as dqlite_node_id;
        let cancel_token = Arc::new(CancellationToken::new());

        let mut node_ptr: *mut dqlite_node = ptr::null_mut();

        let rc =
            unsafe { dqlite_node_create(c_id, c_address.as_ptr(), c_dir.as_ptr(), &mut node_ptr) };

        if rc != 0 {
            let err_msg = get_node_error(node_ptr, &format!("Failed to create node: error code {}", rc));
            unsafe { dqlite_node_destroy(node_ptr) };
            return Err(DqliteError::NodeCreation(err_msg));
        }

        Ok(Node {
            node: node_ptr,
            cancel_token,
        })
    }

    pub fn set_bind_address(&self, address: &str) -> Result<(), DqliteError> {
        let c_address = CString::new(address)?;
        let rc = unsafe { dqlite_node_set_bind_address(self.node, c_address.as_ptr()) };

        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to set bind address: error code {}", rc));
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
            let err_msg = get_node_error(self.node, &format!("Failed to set network latency: error code {}", rc));
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
            let err_msg = get_node_error(self.node, &format!("Failed to set snapshot params: error code {}", rc));
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
            let err_msg = get_node_error(self.node, &format!("Failed to start node: error code {}", rc));
            return Err(DqliteError::Start(err_msg));
        }

        Ok(())
    }

    pub fn stop(&self) -> Result<(), DqliteError> {
        let rc = unsafe { dqlite_node_stop(self.node) };
        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to stop node: error code {}", rc));
            return Err(DqliteError::Stop(err_msg));
        }
        Ok(())
    }

    pub fn set_failure_domain(&self, failure_domain: u64) -> Result<(), DqliteError> {
        let code = failure_domain as std::os::raw::c_ulonglong;
        let rc = unsafe { dqlite_node_set_failure_domain(self.node, code) };
        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to set failure domain: error code {}", rc));
            return Err(DqliteError::Configuration(format!(
                "Failed to set failure domain: {}",
                err_msg
            )));
        }
        Ok(())
    }

    pub fn set_busy_timeout(&self, timeout: u64) -> Result<(), DqliteError> {
        let ctimeout = timeout as std::os::raw::c_uint;
        let rc = unsafe { dqlite_node_set_busy_timeout(self.node, ctimeout) };
        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to set busy timeout: error code {}", rc));
            return Err(DqliteError::Configuration(format!(
                "Failed to set busy timeout: {}",
                err_msg
            )));
        }
        Ok(())
    }

    pub fn set_block_size(&self, size: usize) -> Result<(), DqliteError> {
        let rc = unsafe { dqlite_node_set_block_size(self.node, size) };
        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to set block size: error code {}", rc));
            return Err(DqliteError::Configuration(format!(
                "Failed to set block size: {}",
                err_msg
            )));
        }
        Ok(())
    }

    pub fn set_auto_recovery(&self, enabled: bool) -> Result<(), DqliteError> {
        let c_bool = enabled as std::os::raw::c_bool;
        let rc = unsafe { dqlite_node_set_auto_recovery(self.node, c_bool) };
        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to set auto recovery: error code {}", rc));
            return Err(DqliteError::Configuration(format!(
                "Failed to set auto recovery: {}",
                err_msg
            )));
        }
        Ok(())
    }

    pub fn get_bind_address(&self) -> Result<String, DqliteError> {
        let address = unsafe { dqlite_node_get_bind_address(self.node) };
        if address.is_null() {
            return Err(DqliteError::Configuration("Failed to get bind address".to_string()));
        }
        let address_str = unsafe {
            CStr::from_ptr(address)
                .to_string_lossy()
                .into_owned()
        };

        Ok(address_str)
    }

    pub fn close(&self) -> Result<(), DqliteError> {
        self.cancel_token.cancel();
        
        let rc = unsafe { dqlite_node_stop(self.node) };
        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to stop node: error code {}", rc));
            return Err(DqliteError::Stop(format!(
                "Failed to stop node: {}",
                err_msg
            )));
        }
        Ok(())
    }
    
    // TODO: Implement recover after protocol is implemented
    pub fn recover(&self) -> Result<(), DqliteError> {
        Err(DqliteError::Configuration("Not implemented yet".to_string()))
    }

    pub fn describe_last_entry(&self) -> Result<(RaftLogIndex, RaftLogTerm), DqliteError> {
        let mut index: u64 = 0;
        let mut term: u64 = 0;

        let rc = unsafe { dqlite_node_describe_last_entry(self.node, &mut index, &mut term)};
        if rc != 0 {
            let err_msg = get_node_error(self.node, &format!("Failed to describe last entry: error code {}", rc));
            return Err(DqliteError::Configuration(format!(
                "Failed to describe last entry: {}",
                err_msg
            )));
        }

        Ok((index, term))
    }

    pub fn generate_id(address: &str) -> Result<dqlite_node_id, DqliteError> {
        let c_address = CString::new(address)?;
        let id = unsafe { dqlite_generate_node_id(c_address.as_ptr())};
        Ok(id)
    }
    
}

// RAII wrapper for dqlite_node
impl Drop for Node {
    fn drop(&mut self) {

        self.cancel_token.cancel();

        if !self.node.is_null() {
            unsafe {
                dqlite_node_destroy(self.node);
            }
        }
    }
}

impl !Clone for Node {}


// Custom Connect function for dqlite_server_set_connect_func
#[no_mangle]
pub extern "C" fn connect_with_dial(
    handle: ConnectHandle,
    address: *const libc::c_char,
    fd: *mut libc::c_int,
) -> libc::c_int {
    use std::os::unix::io::RawFd;

    let rt_handle = {
        let rt = RUNTIME_HANDLE.lock().unwrap();
        rt.clone().expect("Runtime handle not initialized")
    };

    // Get Global Connect Registry
    let connect_reg = CONNECT_REGISTRY.lock().unwrap();
    let context_reg = CONTEXT_REGISTRY.lock().unwrap();

    let dial_fn = match connect_reg.get(&handle) {
        Some(dial_fn) => dial_fn.clone(),
        None => return 16, // RAFT_NOCONNECTION
    };

    let cancel_token = match context_reg.get(&handle) {
        Some(token) => token.clone(),
        None => return 16,
    };

    let addr_str = unsafe {
        CStr::from_ptr(address)
            .to_string_lossy()
            .into_owned()
    };

    drop(connect_reg);
    drop(context_reg);

    // Use the context for timeout and cancellation
    let result = rt_handle.block_on(async {
        let timeout_duration = Duration::from_secs(5);

        let dial_future = async {
            if cancel_token.is_cancelled(){
                return Err("cancelled".to_string());
            }

            let conn_result = tokio::task::spawn_blocking({
                let addr = addr_str.clone();
                move || dial_fn(&addr)
            }).await.map_err(|e| e.to_string())?;

            match conn_result {
                Ok(conn) => Ok(conn.as_raw_fd() as RawFd),
                Err(e) => Err(e),
            }
        };

        timeout(timeout_duration, dial_future).await
    });

    match result {
        Ok(Ok(socket_fd)) => {
            unsafe { *fd = socket_fd as libc::c_int };
            0
        }
        Ok(Err(_)) => 16, // RAFT_NOCONNECTION
        Err(_) => 16
    }
}

// C trampoline function to get passed to dqlite_node_set_connect_func
extern "C" fn connect_trampoline(
    data: *mut libc::c_void,
    address: *const libc::c_char,
    fd: *mut libc::c_int,
) -> libc::c_int {
    let handle = data as ConnectHandle;
    connect_with_dial(handle, address, fd)
}


impl Node {
    pub fn set_dial_func<F>(&self, dial: F) -> Result<(), DqliteError>
    where
        F: Fn(&str) -> Result<Conn, String> + Send + Sync + 'static,
    {
        // Get next handle (thread-safe increment)
        let handle = CONNECT_INDEX.fetch_add(1, Ordering::SeqCst);

        let mut connect_reg = CONNECT_REGISTRY.lock().unwrap();
        let mut context_reg = CONTEXT_REGISTRY.lock().unwrap();

        connect_reg.insert(handle, Box::new(dial));
        context_reg.insert(handle, self.cancel_token.clone());

        drop(connect_reg);
        drop(context_reg);

        // Pass handle (as void*) and trampoline function to dqlite_node_set_connect_func
        let rc = unsafe {
            dqlite_node_set_connect_func(
                self.node,
                connect_trampoline,
                handle as *mut libc::c_void,
            )
        };

        if rc != 0 {
            // Cleanup on error
            let mut connect_reg = CONNECT_REGISTRY.lock().unwrap();
            let mut context_reg = CONTEXT_REGISTRY.lock().unwrap();
            connect_reg.remove(&handle);
            context_reg.remove(&handle);

            let err_msg = get_node_error(self.node, &format!("Failed to set dial function: error code {}", rc));

            return Err(DqliteError::Configuration(format!(
                "Failed to set dial function: {}",
                err_msg
            )));
        }
        Ok(())
    }
}