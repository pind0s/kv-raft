use crate::types::NodeId;
use thiserror::Error;

// todo improve errors
#[derive(Error, Debug)]
pub enum RaftError {
    #[error("node encountered an error: {0}")]
    Error(String),

    #[error("error serializing state machine state: {0}")]
    SerializationError(#[from] postcard::Error),

    #[error("current leader is {0:?}")]
    NotLeader(NodeId),

    #[error("node has no leader")]
    NoLeader,

    #[error("raft-rs node command channel is closed")]
    CommandChannelClosed,

    #[error("raft-rs node dropped the command response")]
    CommandResponseDropped,

    #[error("failed to persist state on storage: {0}")]
    StorageError(String),

    #[error("error during transport: {0}")]
    TransportError(String),
}

pub type Result<T> = std::result::Result<T, RaftError>;
