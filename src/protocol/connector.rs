use parking_lot::Mutex;
use crate::protocol::Protocol;
use crate::protocol::store::{NodeStore, ObservableNodeStore};
use crate::protocol::config::Config;
use std::sync::{Arc, Weak};
use std::io::{self, Read, Write};
use std::net::{TcpStream, SocketAddr as TcpSocketAddr};
use std::os::unix::net::{UnixStream, SocketAddr as UnixSocketAddr};

// Unified address type
#[derive(Debug, Clone)]
pub enum Addr {
    Tcp(TcpSocketAddr),
    Unix(UnixSocketAddr),
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Addr::Tcp(addr) => write!(f, "{}", addr),
            Addr::Unix(Some(path)) => write!(f, "unix:{}", path.display()),
            Addr::Unix(None) => write!(f, "unix:<unnamed>"),
        }
    }
}

enum ConnectionType {
    Tcp(TcpStream),
    Unix(UnixStream),
}

pub struct Conn {
    inner: ConnectionType,
}

impl Conn {
    pub fn from_tcp((stream: TcpStream)) -> Self {
        Self {
            inner: ConnectionType::Tcp(stream),
        }
    }

    pub fn from_unix(stream: UnixStream) -> Self {
        Self {
            inner: ConnectionType::Unix(stream),
        }
    }

    pub fn local_addr(&self) -> io::Result<Addr> {
        match &self.inner {
            ConnectionType::Tcp(s) => s.local_addr().map(Addr::Tcp),
            ConnectionType::Unix(s) => {
                let addr = s.local_addr()?;
                Ok(Addr::Unix(addr.as_pathname().map(|p| p.to_owned())))
            }
        }
    }

    pub fn peer_addr(&self) -> io::Result<Addr> {
        match &self.inner {
            ConnectionType::Tcp(s) => s.peer_addr().map(Addr::Tcp),
            ConnectionType::Unix(s) => {
                let addr = s.peer_addr()?;
                Ok(Addr::Unix(addr.as_pathname().map(|p| p.to_owned())))
            }
        }
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            ConnectionType::Tcp(s) => s.read(buf),
            ConnectionType::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            ConnectionType::Tcp(s) => s.write(buf),
            ConnectionType::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            ConnectionType::Tcp(s) => s.flush(),
            ConnectionType::Unix(s) => s.flush(),
        }
    }
}

pub fn dial(addr: &str) -> Result<Conn, String> {
    if addr.starts_with("unix:") {
        let path = addr[5..];
        let stream = UnixStream::connect(path).map_err(|e| e.to_string())?;
        Ok(Conn::from_unix(stream))
    } else {
        let addr = addr.parse::<TcpSocketAddr>().map_err(|e| e.to_string())?;
        let stream = TcpStream::connect(addr).map_err(|e| e.to_string())?;
        Ok(Conn::from_tcp(stream))
    }
}

pub type DialFunc = Arc<dyn Fn(&str) -> Result<Conn, String> + Send + Sync + 'static>;

pub struct Connector<S: NodeStore + Send + Sync> {
    clientID: u64,
    store: Arc<ObservableNodeStore<S>>,
    nodeID: u64,
    nodeAddr: String,
    lt: Mutex<Option<Weak<LeaderTracker>>>,
    config: Arc<Config>,
}

pub struct LeaderTracker {
    pub last_known_leader_addr: String,
    pub proto: Weak<Protocol>,
}