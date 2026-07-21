use crate::error::{RaftError, Result};
use crate::message::{LogEntry, RaftEnvelope};
use crate::type_config::RaftTypeConfig;
use crate::types::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct RaftHandle<RaftType: RaftTypeConfig> {
    raft_messages: mpsc::Sender<RaftEnvelope<RaftType>>,
    client_commands: mpsc::Sender<ClientCommands<RaftType>>,
}

// todo add timeout to client commands
impl<RaftType: RaftTypeConfig> RaftHandle<RaftType> {
    pub(crate) fn new(
        raft_messages: mpsc::Sender<RaftEnvelope<RaftType>>,
        client_commands: mpsc::Sender<ClientCommands<RaftType>>,
    ) -> Self {
        Self {
            raft_messages,
            client_commands,
        }
    }

    pub async fn send_message(&self, message: RaftEnvelope<RaftType>) -> Result<()> {
        if self.raft_messages.send(message).await.is_err() {
            return Err(RaftError::CommandChannelClosed);
        }
        Ok(())
    }

    async fn send_command<Response>(
        &self,
        command: ClientCommands<RaftType>,
        response_channel: oneshot::Receiver<Result<Response>>,
    ) -> Result<Response> {
        if self.client_commands.send(command).await.is_err() {
            return Err(RaftError::CommandChannelClosed);
        }

        response_channel
            .await
            .unwrap_or_else(|_| Err(RaftError::CommandResponseDropped))
    }

    pub async fn apply(&self, command: RaftType::Command) -> Result<RaftType::Output> {
        let (reply, response_channel) = oneshot::channel();
        let command = ClientCommands::Apply { command, reply };
        self.send_command(command, response_channel).await
    }

    pub async fn is_leader(&self) -> Result<()> {
        let (reply, response_channel) = oneshot::channel();
        let command = ClientCommands::IsLeader { reply };
        self.send_command(command, response_channel).await
    }

    pub async fn log(&self) -> Result<Vec<LogEntry<RaftType::Command>>> {
        let (reply, response_channel) = oneshot::channel();
        let command = ClientCommands::GetLog { reply };
        self.send_command(command, response_channel).await
    }

    pub async fn log_entry(&self, index: usize) -> Result<Option<LogEntry<RaftType::Command>>> {
        let (reply, response_channel) = oneshot::channel();
        let command = ClientCommands::GetLogEntry { index, reply };
        self.send_command(command, response_channel).await
    }

    pub async fn status(&self) -> Result<RaftStatus> {
        let (reply, response_channel) = oneshot::channel();
        let command = ClientCommands::GetStatus { reply };
        self.send_command(command, response_channel).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftStatus {
    pub id: NodeId,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub role: RaftRole,
    pub leader_id: Option<NodeId>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub log_len: usize,
    pub peer_ids: Vec<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RaftRole {
    Follower,
    Candidate {
        votes_received: usize,
    },
    Leader {
        next_index: HashMap<NodeId, usize>,
        match_index: HashMap<NodeId, usize>,
    },
}

// todo add more commands like uncommitted_command (sending command without applying it to state machine)
#[derive(Debug)]
pub(crate) enum ClientCommands<RaftType: RaftTypeConfig> {
    Apply {
        command: RaftType::Command,
        reply: oneshot::Sender<Result<RaftType::Output>>,
    },

    IsLeader {
        reply: oneshot::Sender<Result<()>>,
    },
    GetLog {
        reply: oneshot::Sender<Result<Vec<LogEntry<RaftType::Command>>>>,
    },
    GetLogEntry {
        index: usize,
        reply: oneshot::Sender<Result<Option<LogEntry<RaftType::Command>>>>,
    },
    GetStatus {
        reply: oneshot::Sender<Result<RaftStatus>>,
    },
}
