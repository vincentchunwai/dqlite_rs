use parking_lot::Mutex;
use crate::protocol::Protocol;
use crate::protocol::store::{NodeStore, ObservableNodeStore};
use crate::protocol::config::Config;
use std::sync::{Arc, Weak};
use std::io::{self, Read, Write};
use tokio::net::{TcpStream, UnixStream, SocketAddr as TcpSocketAddr, UnixSocketAddr};
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::future::Future;
use std::net::SocketAddr as StdSocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
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
    pub fn from_tcp(stream: TcpStream) -> Self {
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

    pub fn as_raw_fd(&self) -> RawFd {
        match &self.inner { 
            ConnectionType::Tcp(s) => s.as_raw_fd(),
            ConnectionType::Unix(s) => s.as_raw_fd(),
        }
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut().inner {
            ConnectionType::Tcp(ref mut s) => {
                let result = Pin::new(s).poll_read(cx, buf);

                return result;
            }
            ConnectionType::Unix(ref mut s) => {
                let result = Pin::new(s).poll_read(cx, buf);

                return result;
            }
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut().inner {
            ConnectionType::Tcp(ref mut s) => {
                let result = Pin::new(s).poll_write(cx, buf);

                return result;
            }
            ConnectionType::Unix(ref mut s) => {
                let result = Pin::new(s).poll_write(cx, buf);

                return result;
            }
        }
    }
    
    fn poll_flush(self: Pin<&mut self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().inner {
            ConnectionType::Tcp(ref mut s) => {
                let result = Pin::new(s).poll_flush(cx);

                return result;
            }
            ConnectionType::Unix(ref mut s) => {
                let result = Pin::new(s).poll_flush(cx);
    
                return result;
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().inner {
            ConnectionType::Tcp(ref mut s) => {
                let result = Pin::new(s).poll_shutdown(cx);

                return result;
            }
            ConnectionType::Unix(ref mut s) => {
                let result = Pin::new(s).poll_shutdown(cx);

                return result;
            }
        }
    }
}

pub fn dial(addr: &str) -> Result<Conn, String> {
    if addr.starts_with("unix:") {
        let path = addr[5..];
        let stream = UnixStream::connect(path).await.map_err(|e| e.to_string())?;
        Ok(Conn::from_unix(stream))
    } else {
        let addr = addr.parse::<StdSocketAddr>().map_err(|e| e.to_string())?;
        let stream = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
        Ok(Conn::from_tcp(stream))
    }
}

pub type DialFunc = Arc<dyn Fn(&str) -> Pin<Box<dyn Future<Output = Result<Conn, String>> + Send + Sync + 'static>> + Send + Sync + 'static>;

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