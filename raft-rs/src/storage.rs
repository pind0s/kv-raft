use crate::error::Result;
use crate::message::LogEntry;
use crate::type_config::RaftTypeConfig;
use crate::types::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistentState<RaftType: RaftTypeConfig> {
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) log: Vec<LogEntry<RaftType::Command>>,
}

pub trait RaftStorage<RaftType: RaftTypeConfig> {
    fn save_state(&self, bytes: Vec<u8>) -> Result<()>;
    fn restore_state(&self) -> Result<Option<Vec<u8>>>;
}
