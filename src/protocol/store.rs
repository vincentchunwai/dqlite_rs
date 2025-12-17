use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::path::PathBuf;
use rusqlite::{Connection as SqliteConnection, params, Result as SqliteResult};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeRole(u8);

type NodeAddress = String;
type NodeId = u64;
type NodeVersion = u64;

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
    /// Get all nodes
    async fn get_all(&self) -> NodeStoreResult<Vec<NodeInfo>>;
    
    /// Get a single node by ID
    async fn get_by_id(&self, id: NodeId) -> NodeStoreResult<Option<NodeInfo>>;
    
    /// Get a single node by address
    async fn get_by_address(&self, address: &str) -> NodeStoreResult<Option<NodeInfo>>;
    
    /// Set all nodes (atomic replace)
    async fn set_all(&self, nodes: Vec<NodeInfo>) -> NodeStoreResult<()>;
    
    /// Add or update a single node
    async fn upsert(&self, node: NodeInfo) -> NodeStoreResult<()>;
    
    /// Remove a node by ID
    async fn remove(&self, id: NodeId) -> NodeStoreResult<bool>;
    
    /// Get the store version (for optimistic locking)
    async fn version(&self) -> NodeStoreResult<NodeVersion>;
    
    /// Set with version check (optimistic locking)
    async fn set_if_version(&self, nodes: Vec<NodeInfo>, version: NodeVersion) -> NodeStoreResult<()>;
}

pub struct NodeStoreBackend {
    nodes: Arc<RwLock<HashMap<u64, NodeInfo>>>,
    addresses: Arc<RwLock<HashMap<String, u64>>>,
    version: Arc<RwLock<u64>>,
}

impl NodeStoreBackend {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            addresses: Arc::new(RwLock::new(HashMap::new())),
            version: Arc::new(RwLock::new(0)),
        }
    }
    
    pub fn from_nodes(nodes: Vec<NodeInfo>) -> NodeStoreResult<Self> {
        validate_nodes(&nodes)?;
        
        let mut nodes_map = HashMap::new();
        let mut addresses_map = HashMap::new();
        
        for node in nodes {
            nodes_map.insert(node.id, node);
            addresses_map.insert(node.address.clone(), node.id);
        }
        
        Ok(Self {
            nodes: Arc::new(RwLock::new(nodes_map)),
            addresses: Arc::new(RwLock::new(addresses_map)),
            version: Arc::new(RwLock::new(0)),
        })
    }
    
    pub fn get_all(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().unwrap();
        nodes.values().copied().collect()
    }
    
    pub fn get_by_id(&self, id: u64) -> Option<NodeInfo> {
        let nodes = self.nodes.read().unwrap();
        nodes.get(&id).copied()
    }
    
    pub fn get_by_address(&self, address: &str) -> Option<NodeInfo> {
        let addresses = self.addresses.read().unwrap();
        if let Some(&id) = addresses.get(address) {
            let nodes = self.nodes.read().unwrap();
            nodes.get(&id).copied()
        } else {
            None
        }
    }
    
    pub fn set_all(&self, nodes: Vec<NodeInfo>) -> NodeStoreResult<()> {
        validate_nodes(&nodes)?;
        
        let mut store = self.nodes.write().unwrap();
        let mut addrs = self.addresses.write().unwrap();
        let mut version = self.version.write().unwrap();
        
        store.clear();
        addrs.clear();
        
        for node in nodes {
            store.insert(node.id, node);
            addrs.insert(node.address.clone(), node.id);
        }
        
        *version += 1;
        Ok(())
    }
    
    pub fn upsert(&self, node: NodeInfo) -> NodeStoreResult<()> {
        node.validate()?;
        
        let mut store = self.nodes.write().unwrap();
        let mut addrs = self.addresses.write().unwrap();
        let mut version = self.version.write().unwrap();
        
        if let Some(old_node) = store.get(&node.id) {
            addrs.remove(&old_node.address);
        }
        
        store.insert(node.id, node);
        addrs.insert(node.address.clone(), node.id);
        *version += 1;
        
        Ok(())
    }
    
    pub fn remove(&self, id: u64) -> bool {
        let mut store = self.nodes.write().unwrap();
        let mut addrs = self.addresses.write().unwrap();
        let mut version = self.version.write().unwrap();
        
        if let Some(node) = store.remove(&id) {
            addrs.remove(&node.address);
            *version += 1;
            true
        } else {
            false
        }
    }
    
    pub fn version(&self) -> u64 {
        let version = self.version.read().unwrap();
        *version
    }
    
    pub fn set_if_version(&self, nodes: Vec<NodeInfo>, expected_version: u64) -> NodeStoreResult<()> {
        let current_version = self.version();
        if current_version != expected_version {
            return Err(NodeStoreError::VersionConflict);
        }
        self.set_all(nodes)
    }
}

pub struct InMemoryNodeStore {
    backend: NodeStoreBackend,
}

impl InMemoryNodeStore {
    pub fn new() -> Self {
        Self {
            backend: NodeStoreBackend::new(),
        }
    }
}

#[async_trait]
impl NodeStore for InMemoryNodeStore {
    async fn get_all(&self) -> NodeStoreResult<Vec<NodeInfo>> {
        Ok(self.backend.get_all())
    }
    
    async fn get_by_id(&self, id: u64) -> NodeStoreResult<Option<NodeInfo>> {
        Ok(self.backend.get_by_id(id))
    }
    
    async fn get_by_address(&self, address: &str) -> NodeStoreResult<Option<NodeInfo>> {
        Ok(self.backend.get_by_address(address))
    }
    
    async fn set_all(&self, nodes: Vec<NodeInfo>) -> NodeStoreResult<()> {
        self.backend.set_all(nodes)
    }
    
    async fn upsert(&self, node: NodeInfo) -> NodeStoreResult<()> {
        self.backend.upsert(node)
    }
    
    async fn remove(&self, id: u64) -> NodeStoreResult<bool> {
        Ok(self.backend.remove(id))
    }
    
    async fn version(&self) -> NodeStoreResult<u64> {
        Ok(self.backend.version())
    }
    
    async fn set_if_version(&self, nodes: Vec<NodeInfo>, expected_version: u64) -> NodeStoreResult<()> {
        self.backend.set_if_version(nodes, expected_version)
    }
}

pub struct YamlNodeStore {
    backend: NodeStoreBackend,
    path: PathBuf,
}

impl YamlNodeStore {
    pub async fn new<P: AsRef<Path>>(path: P) -> NodeStoreResult<Self> {
        let path = path.as_ref().to_path_buf();

        let backend = if path.exists() {
            let content = fs::read_to_string(&path).await?;
            let nodes: Vec<NodeInfo> = serde_yaml::from_str(&content)
                .map_err(|e| NodeStoreError::Serialization(e.to_string()))?;

            NodeStoreBackend::from_nodes(nodes)?;
        } else {
            NodeStoreBackend::new()
        };

        Ok(Self { backend, path })
    }

    async fn save(&self) -> NodeStoreResult<()> {
        let nodes = self.backend.get_all();

        let yaml = serde_yaml::to_string(&nodes)
            .map_err(|e| NodeStoreError::Serialization(e.to_string()))?;

        let temp_path = self.path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(yaml.as_bytes()).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_path, &self.path).await?;
        Ok(())
    }
}

#[async_trait]
impl NodeStore for YamlNodeStore {
    async fn get_all(&self) -> NodeStoreResult<Vec<NodeInfo>> {
        Ok(self.backend.get_all())
    }
    
    async fn get_by_id(&self, id: u64) -> NodeStoreResult<Option<NodeInfo>> {
        Ok(self.backend.get_by_id(id))
    }
    
    async fn get_by_address(&self, address: &str) -> NodeStoreResult<Option<NodeInfo>> {
        Ok(self.backend.get_by_address(address))
    }
    
    async fn set_all(&self, nodes: Vec<NodeInfo>) -> NodeStoreResult<()> {
        self.backend.set_all(nodes)?;
        self.save().await
    }
    
    async fn upsert(&self, node: NodeInfo) -> NodeStoreResult<()> {
        self.backend.upsert(node)?;
        self.save().await
    }
    
    async fn remove(&self, id: u64) -> NodeStoreResult<bool> {
        let removed = self.backend.remove(id);
        if removed {
            self.save().await?;
        }
        Ok(removed)
    }
    
    async fn version(&self) -> NodeStoreResult<u64> {
        Ok(self.backend.version())
    }
    
    async fn set_if_version(&self, nodes: Vec<NodeInfo>, expected_version: u64) -> NodeStoreResult<()> {
        self.backend.set_if_version(nodes, expected_version)?;
        self.save().await
    }
}

pub struct DatabaseNodeStore {
    db: Arc<Mutex<SqliteConnection>>,
    version: Arc<RwLock<NodeVersion>>,
}


impl DatabaseNodeStore {
    pub async fn new<P: AsRef<Path>>(path: P) -> NodeStoreResult<Self> {
        let conn = SqliteConnection::open(path)
            .map_err(|e| NodeStoreError::Store(e.to_string()))?;

        // Create table with ALL fields (id, address, role, updated_at)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS servers (
                id INTEGER PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                role INTEGER NOT NULL,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )
        .map_err(|e| NodeStoreError::Store(e.to_string()))?;

        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            version: Arc::new(RwLock::new(0)),
        })
    }
}

#[async_trait]
impl NodeStore for DatabaseNodeStore {
    async fn get_all(&self) -> NodeStoreResult<Vec<NodeInfo>> {
        let db = self.db.lock().await;
        let mut stmt = db
            .prepare("SELECT id, address, role FROM servers")
            .map_err(|e| NodeStoreError::Store(e.to_string()))?;

        let nodes = stmt
            .query_map([], |row| {
                Ok(NodeInfo {
                    id: row.get(0)?,
                    addr: row.get(1)?,
                    role: match row.get::<_, i64>(2)? {
                        0 => NodeRole::VOTER,
                        1 => NodeRole::STAND_BY,
                        2 => NodeRole::SPARE,
                        _ => return Err(rusqlite::Error::InvalidColumnType(2, "role", rusqlite::types::Type::Integer)),
                    }
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()
            .map_err(|e| NodeStoreError::Store(e.to_string()))?;

        Ok(nodes)
    }
}