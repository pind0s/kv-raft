use crate::type_config::RaftTypeConfig;
use crate::types::NodeId;
use serde::{Deserialize, Serialize};

// just a helper struct to wrap messages with info about senders node id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftEnvelope<RaftType: RaftTypeConfig> {
    pub from: NodeId,
    pub message: RaftMessage<RaftType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage<RaftType: RaftTypeConfig> {
    AppendRequest(AppendRequest<RaftType>),
    AppendResponse(AppendResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogCommand<Command> {
    NoOp,
    Command(Command),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<Command> {
    pub term: u64,
    pub command: LogCommand<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendRequest<RaftType: RaftTypeConfig> {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry<RaftType::Command>>,
    pub leader_commit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendResponse {
    pub term: u64,
    pub match_index: usize,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}
