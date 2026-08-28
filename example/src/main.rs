use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use kv::Database;
use raft_rs::{
    NodeId, RaftConfig, RaftEnvelope, RaftError, RaftHandle, RaftMessage, RaftNode, RaftRole,
    RaftStateMachine, RaftStorage, RaftTransport, RaftTypeConfig,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

#[derive(Debug, Error)]
enum ExampleError {
    #[error(transparent)]
    Kv(#[from] kv::Error),

    #[error(transparent)]
    Raft(#[from] RaftError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("leader election timed out")]
    ElectionTimeout,

    #[error("key was not found")]
    KeyNotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Read { key: Vec<u8> },
}

#[derive(Debug, Clone)]
struct KvRaft;

impl RaftTypeConfig for KvRaft {
    type Command = Command;
    type Output = Option<Vec<u8>>;
}

struct KvStateMachine {
    database: Database,
}

impl RaftStateMachine<KvRaft> for KvStateMachine {
    fn apply(&mut self, command: &Command) -> Option<Vec<u8>> {
        match command {
            Command::Put { key, value } => {
                let transaction = self.database.begin_write().unwrap();
                transaction.insert(key, value).unwrap();
                transaction.commit().unwrap();
                None
            }
            Command::Read { key } => self.database.begin_read().unwrap().read(key).unwrap(),
        }
    }
}

#[derive(Default)]
struct MemoryStorage {
    state: Mutex<Option<Vec<u8>>>,
}

impl RaftStorage<KvRaft> for MemoryStorage {
    fn save_state(&self, bytes: Vec<u8>) -> raft_rs::Result<()> {
        *self.state.lock().unwrap() = Some(bytes);
        Ok(())
    }

    fn restore_state(&self) -> raft_rs::Result<Option<Vec<u8>>> {
        Ok(self.state.lock().unwrap().clone())
    }
}

#[derive(Clone, Default)]
struct MemoryNetwork {
    nodes: Arc<RwLock<HashMap<NodeId, RaftHandle<KvRaft>>>>,
}

impl MemoryNetwork {
    fn add_node(&self, id: NodeId, handle: RaftHandle<KvRaft>) {
        self.nodes.write().unwrap().insert(id, handle);
    }

    fn remove_node(&self, id: NodeId) {
        self.nodes.write().unwrap().remove(&id);
    }

    fn node(&self, id: NodeId) -> RaftHandle<KvRaft> {
        self.nodes.read().unwrap()[&id].clone()
    }

    fn all_nodes(&self) -> Vec<(NodeId, RaftHandle<KvRaft>)> {
        self.nodes
            .read()
            .unwrap()
            .iter()
            .map(|(&id, handle)| (id, handle.clone()))
            .collect()
    }
}

#[async_trait::async_trait]
impl RaftTransport<KvRaft> for MemoryNetwork {
    async fn send(
        &self,
        from: NodeId,
        to: NodeId,
        message: RaftMessage<KvRaft>,
    ) -> raft_rs::Result<()> {
        let destination = self
            .nodes
            .read()
            .unwrap()
            .get(&to)
            .cloned()
            .ok_or_else(|| RaftError::TransportError(format!("node {to} is unavailable")))?;

        destination
            .send_message(RaftEnvelope { from, message })
            .await
    }
}

#[tokio::main]
async fn main() -> Result<(), ExampleError> {
    let directory = tempfile::tempdir()?;
    let node_ids = [1, 2, 3];
    let network = MemoryNetwork::default();
    let mut tasks = HashMap::new();

    for &id in &node_ids {
        let database =
            Database::open_new(directory.path().join(format!("node-{id}.kv")), 4096, 128)?;
        let (node, handle) = RaftNode::new(
            id,
            node_ids.to_vec(),
            KvStateMachine { database },
            network.clone(),
            MemoryStorage::default(),
            RaftConfig::default(),
        );

        network.add_node(id, handle);
        tasks.insert(id, tokio::spawn(node.launch()));
    }

    let leader = wait_for_leader(&network).await?;
    println!("node {leader} was elected leader");

    put(&network, leader, b"one", b"one").await?;
    let value = read(&network, leader, b"one").await?;
    println!("one={}", String::from_utf8_lossy(&value));

    put(&network, leader, b"one", b"two").await?;
    let value = read(&network, leader, b"one").await?;
    println!("one={}", String::from_utf8_lossy(&value));

    kill_node(&network, &tasks, leader);
    println!("killed node {leader}");
    let leader = wait_for_leader(&network).await?;
    println!("node {leader} was elected leader");

    let value = read(&network, leader, b"one").await?;
    println!("one={}", String::from_utf8_lossy(&value));

    put(&network, leader, b"one", b"three").await?;
    let value = read(&network, leader, b"one").await?;
    println!("one={}", String::from_utf8_lossy(&value));

    Ok(())
}

fn kill_node(network: &MemoryNetwork, tasks: &HashMap<NodeId, JoinHandle<()>>, id: NodeId) {
    tasks[&id].abort();
    network.remove_node(id);
}

async fn put(
    network: &MemoryNetwork,
    leader: NodeId,
    key: &[u8],
    value: &[u8],
) -> Result<(), ExampleError> {
    let _ = network
        .node(leader)
        .apply(Command::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
        .await?;

    Ok(())
}

async fn read(
    network: &MemoryNetwork,
    leader: NodeId,
    key: &[u8],
) -> Result<Vec<u8>, ExampleError> {
    network
        .node(leader)
        .apply(Command::Read { key: key.to_vec() })
        .await?
        .ok_or(ExampleError::KeyNotFound)
}

async fn wait_for_leader(network: &MemoryNetwork) -> Result<NodeId, ExampleError> {
    timeout(Duration::from_secs(5), async {
        loop {
            for (id, handle) in network.all_nodes() {
                let Ok(status) = handle.status().await else {
                    continue;
                };

                if matches!(status.role, RaftRole::Leader { .. }) {
                    return Ok(id);
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| ExampleError::ElectionTimeout)?
}
