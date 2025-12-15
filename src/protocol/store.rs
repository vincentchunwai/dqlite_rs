use serde::{Serialize, Deserialize};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeRole(u8);

impl NodeRole {
    pub const VOTER: NodeRole = NodeRole(0);
    pub const STAND_BY: NodeRole = NodeRole(1);
    pub const SPARE: NodeRole = NodeRole(2);

    pub fn new(value: u8) -> Result<Self, String> {
        match value {
            0 | 1 | 2 => Ok(NodeRole(value)),
            _ => Err(format!("Invalid NodeRole value: {}", value)),
        }
    }

    pub fn value(self) -> u8 {
        self.0
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            0 => write!(f, "voter"),
            1 => write!(f, "stand-by"),
            2 => write!(f, "spare"),
            _ => write!(f, "unknown role"),
        }
    }
}

impl Serialize for NodeRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.value())
    }
}

impl<'de> Deserialize<'de> for NodeRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    #[serde(rename = "ID")]
    pub id: u64,

    #[serde(rename = "Address")]
    pub addr: String,

    #[serde(rename = "Role")]
    pub role: NodeRole,
}

impl NodeInfo {
    // Validate if the node info is valid
    pub fn validate(&self) -> Result<(), NodeStoreError> {
        if self.addr.is_empty() {
            return Err(NodeStoreError::InvalidNode("Address is required".to_string()));
        }

        if self.addr.parse::<std::net::SocketAddr>().is_ok() {
            return Ok(());
        }

        // Abstract Unix socket address
        if self.addr.starts_with("@") {
            return Ok(());
        }

        // Path based
        if self.addr.starts_with("/") {
            return Ok(());
        }

        // Explicit unix:// prefix
        if self.addr.starts_with("unix:") {
            return Ok(());
        }

        return Err(NodeStoreError::InvalidNode(format!("Invalid address: {}", self.addr)));
    }
}

fn validate_nodes(nodes: &[NodeInfo]) -> NodeStoreResult<()> {
    let mut seen_ids = HashSet::new();
    let mut seen_addresses = HashSet::new();

    for node in nodes {
        node.validate()?;

        if !seen_ids.insert(node.id) {
            return Err(NodeStoreError::InvalidNode(format!("Duplicate node ID: {}", node.id)));
        }

        if !seen_addresses.insert(node.addr.clone()) {  // Fix: use addr, not address
            return Err(NodeStoreError::InvalidNode(format!("Duplicate node address: {}", node.addr)));
        }
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum NodeStoreError {
    #[error("Invalid Node info: {0}")]
    InvalidNode(String),

    #[error("Node not found: {0}")]
    NotFound { id: u64 },

    #[error("Concurrent modification detected")]
    VersionConflict,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Store error: {0}")]
    Store(String),
}

pub type NodeStoreResult<T> = Result<T, NodeStoreError>;

#[async_trait]
pub trait NodeStore: Send + Sync {
    async fn get_all(&self) -> NodeStoreResult<Vec<NodeInfo>>;

    async fn get_by_id(&self, id: u64) -> NodeStoreResult<Option<NodeInfo>>;

    async fn get_by_address(&self, address: &str) -> NodeStoreResult<Option<NodeInfo>>;

    async fn set_all(&self, nodes: Vec<NodeInfo>) -> NodeStoreResult<()>;

    async fn upsert(&self, node: NodeInfo) -> NodeStoreResult<()>;

    async fn remove(&self, id: u64) -> NodeStoreResult<bool>;

    async fn version(&self) -> NodeStoreResult<u64>; // For optimistic concurrency control

    async fn set_if_version(&self, node: NodeInfo, version: u64) -> NodeStoreResult<bool>;
}


