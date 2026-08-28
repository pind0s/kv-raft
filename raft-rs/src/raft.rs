use crate::config::RaftConfig;
use crate::error::{RaftError, Result};
use crate::message::{
    AppendRequest, AppendResponse, LogCommand, LogEntry, RaftEnvelope, RaftMessage, VoteRequest,
    VoteResponse,
};
use crate::raft_handle::{ClientCommands, RaftHandle, RaftRole, RaftStatus};
use crate::role::Role;
use crate::state_machine::RaftStateMachine;
use crate::storage::{PersistentState, RaftStorage};
use crate::transport::RaftTransport;
use crate::type_config::RaftTypeConfig;
use crate::types::NodeId;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant, interval, sleep_until, timeout};
use tracing::{error, info, warn};

pub struct RaftNode<RaftType, StateMachine, Transport, Storage>
where
    RaftType: RaftTypeConfig,
    StateMachine: RaftStateMachine<RaftType>,
    Transport: RaftTransport<RaftType>,
    Storage: RaftStorage<RaftType>,
{
    //persistent state
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<RaftType::Command>>,

    //volatile state
    id: NodeId,
    peer_ids: Vec<NodeId>,
    role: Role,

    commit_index: usize,
    last_applied: usize,

    config: RaftConfig,

    state_machine: StateMachine,
    transport: Arc<Transport>,
    storage: Storage,

    election_deadline: Instant,

    message_receiver: mpsc::Receiver<RaftEnvelope<RaftType>>, // channel to receive raft-rs messages from other nodes
    command_receiver: mpsc::Receiver<ClientCommands<RaftType>>, // channel to receive client commands like is_leader or apply_command
    pending_applies: HashMap<usize, oneshot::Sender<Result<RaftType::Output>>>, // channel to send replies for client apply commands

    transport_work: HashMap<NodeId, mpsc::Sender<RaftMessage<RaftType>>>, // each peer has a dedicated task that will handle sending messages
}

impl<RaftType, StateMachine, Transport, Storage>
    RaftNode<RaftType, StateMachine, Transport, Storage>
where
    RaftType: RaftTypeConfig,
    StateMachine: RaftStateMachine<RaftType>,
    Transport: RaftTransport<RaftType>,
    Storage: RaftStorage<RaftType>,
{
    pub fn new(
        id: NodeId,
        peer_ids: Vec<NodeId>,
        state_machine: StateMachine,
        transport: Transport,
        storage: Storage,
        config: RaftConfig,
    ) -> (
        RaftNode<RaftType, StateMachine, Transport, Storage>,
        RaftHandle<RaftType>,
    ) {
        let (command_sender, command_receiver) = mpsc::channel(128);
        let (raft_message_sender, message_receiver) = mpsc::channel(128);

        let handle = RaftHandle::new(raft_message_sender, command_sender);

        let mut node = RaftNode {
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                command: LogCommand::NoOp,
            }],

            id,
            peer_ids: peer_ids
                .into_iter()
                .filter(|&peer_id| peer_id != id)
                .collect(), // filter out self id from peer ids

            role: Role::Follower {
                current_leader_id: None,
            },
            commit_index: 0,
            last_applied: 0,
            state_machine,
            transport: Arc::new(transport),
            storage,
            config,
            election_deadline: Instant::now(),
            message_receiver,
            command_receiver,
            pending_applies: HashMap::new(),
            transport_work: HashMap::new(),
        };

        node.restore_state().unwrap();

        // todo this will call spawn_task in new, maybe not ideal?
        node.init_transport_workers();

        (node, handle)
    }

    pub async fn launch(mut self) {
        info!("launching node#{}", self.id);
        self.reset_election_timer();

        let mut heartbeat_timer =
            interval(Duration::from_millis(self.config.heartbeat_interval_ms));

        loop {
            tokio::select! {
                _ = sleep_until(self.election_deadline), if !self.role.is_leader() => {
                    self.become_candidate().await;
                }

                _ = heartbeat_timer.tick(), if self.role.is_leader() => {
                    self.on_heartbeat();
                }

                msg = self.message_receiver.recv() => {
                    if let Some(envelope) = msg {
                        self.handle_message(envelope).await;
                    }
                }

                command = self.command_receiver.recv() => {
                    if let Some(command) = command {
                        self.client_command(command).await;
                    }
                }
            }
        }
    }

    // spawn a dedicated task for each peer that will handle sending messages to that peer
    // this way we can avoid blocking main loop when sending messages
    fn init_transport_workers(&mut self) {
        for &peer_id in &self.peer_ids {
            let (tx, mut rx) = mpsc::channel(128);
            self.transport_work.insert(peer_id, tx);

            let from_id = self.id;
            let transport = Arc::clone(&self.transport);
            let timeout_ms = self.config.transport_timeout_ms;

            tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    match timeout(
                        Duration::from_millis(timeout_ms),
                        transport.send(from_id, peer_id, message),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            warn!("failed to send message to node {peer_id:?}: {err:?}");
                        }
                        Err(_) => {
                            warn!("timed out sending message to node {peer_id:?}");
                        }
                    }
                }
            });
        }
    }

    fn reset_election_timer(&mut self) {
        self.election_deadline = Instant::now() + self.config.random_election_timeout();
    }

    async fn handle_message(&mut self, envelope: RaftEnvelope<RaftType>) {
        match envelope.message {
            RaftMessage::AppendResponse(msg) => self.append_response(envelope.from, &msg),
            RaftMessage::AppendRequest(msg) => self.append_request(envelope.from, msg),
            RaftMessage::VoteRequest(msg) => self.vote_request(envelope.from, msg),
            RaftMessage::VoteResponse(msg) => self.vote_response(msg).await,
        }
    }

    fn become_follower(&mut self, term: u64) {
        info!("become follower for term {}", term);

        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }

        self.role = Role::Follower {
            current_leader_id: None,
        };

        self.persist_state();
        self.reset_election_timer();
    }

    async fn become_candidate(&mut self) {
        info!("become candidate for term {}", self.current_term + 1);

        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate { votes_received: 1 };
        self.reset_election_timer();

        // single node cluster will become leader immediately
        if has_majority(1, self.peer_ids.len() + 1) {
            self.become_leader().await;
            return;
        }

        let last_log_index = self.log.len() - 1;
        let last_log_term = self.log.last().unwrap().term;

        let request = RaftMessage::VoteRequest(VoteRequest {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        });

        self.persist_state();
        self.broadcast_message(request);
    }

    async fn become_leader(&mut self) {
        info!("become leader for term {}", self.current_term);

        self.log.push(LogEntry {
            term: self.current_term,
            command: LogCommand::NoOp,
        });

        let next = self.log.len();
        let next_index: HashMap<NodeId, usize> = self
            .peer_ids
            .iter()
            .map(|&peer_id| (peer_id, next))
            .collect();

        let match_index: HashMap<NodeId, usize> =
            self.peer_ids.iter().map(|&peer_id| (peer_id, 0)).collect();

        self.role = Role::Leader {
            next_index,
            match_index,
        };

        self.persist_state();
        // immediately send heartbeat
        self.on_heartbeat();
    }

    fn append_response(&mut self, from: NodeId, msg: &AppendResponse) {
        if msg.term < self.current_term {
            // stale response, ignore
            return;
        }

        if msg.term > self.current_term {
            self.become_follower(msg.term);
            return;
        }

        let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        else {
            return;
        };

        if msg.success {
            match_index.insert(from, msg.match_index);
            next_index.insert(from, msg.match_index + 1);
        } else {
            next_index.insert(from, msg.match_index.saturating_sub(1).max(1));
        }

        self.advance_commit();
        self.apply_committed();
    }

    fn append_request(&mut self, from: NodeId, msg: AppendRequest<RaftType>) {
        if msg.term < self.current_term {
            info!(
                "stale AppendRequest with term {}, while current term is {}, ignoring",
                msg.term, self.current_term
            );
            let response = AppendResponse {
                term: self.current_term,
                match_index: 0,
                success: false,
            };

            self.send_message(from, RaftMessage::AppendResponse(response));

            return;
        }

        if msg.term > self.current_term {
            self.become_follower(msg.term);
        }

        if self.role.is_leader() && msg.term == self.current_term {
            unreachable!("two leaders detected in the same term");
        }

        if self.role.is_candidate() && msg.term == self.current_term {
            self.become_follower(msg.term);
        }

        if let Role::Follower { current_leader_id } = &mut self.role
            && current_leader_id.is_none()
        {
            info!("setting current leader to {}", msg.leader_id);
            *current_leader_id = Some(msg.leader_id);
        }

        self.reset_election_timer();

        let match_index = msg.prev_log_index;
        if match_index >= self.log.len() {
            let response = AppendResponse {
                term: self.current_term,
                match_index: self.log.len() - 1,
                success: false,
            };

            self.send_message(from, RaftMessage::AppendResponse(response));

            return;
        }

        if self.log[match_index].term != msg.prev_log_term {
            let response = AppendResponse {
                term: self.current_term,
                match_index,
                success: false,
            };

            self.send_message(from, RaftMessage::AppendResponse(response));

            return;
        }

        let last_match_index = msg.prev_log_index + msg.entries.len();

        for (i, entry) in msg.entries.into_iter().enumerate() {
            let index = msg.prev_log_index + 1 + i;

            if index < self.log.len() {
                if self.log[index].term != entry.term {
                    self.log.truncate(index);
                    self.log.push(entry);
                }
            } else {
                self.log.push(entry);
            }
        }

        if msg.leader_commit > self.commit_index {
            self.commit_index = min(msg.leader_commit, self.log.len() - 1);
        }

        self.persist_state();
        self.apply_committed();

        let response = AppendResponse {
            term: self.current_term,
            match_index: last_match_index,
            success: true,
        };

        self.send_message(from, RaftMessage::AppendResponse(response));
    }

    fn vote_request(&mut self, from: NodeId, msg: VoteRequest) {
        info!("received VoteRequest from {}: {:?}", from, msg);

        if msg.term < self.current_term {
            info!(
                "stale VoteRequest with term {}, current term is {}, ignoring",
                msg.term, self.current_term
            );

            let response = VoteResponse {
                term: self.current_term,
                vote_granted: false,
            };

            self.send_message(from, RaftMessage::VoteResponse(response));

            return;
        }

        if msg.term > self.current_term {
            self.become_follower(msg.term);
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(msg.candidate_id);

        let last_log_index = self.log.len() - 1;
        let last_log_term = self.log.last().unwrap().term; // we always have at least one dummy log entry, so unwrap should be safe

        let log_up_to_date = msg.last_log_term > last_log_term
            || (msg.last_log_term == last_log_term && msg.last_log_index >= last_log_index);

        if can_vote && log_up_to_date {
            info!(
                "granting vote to {} for term {}",
                msg.candidate_id, msg.term
            );

            self.voted_for = Some(msg.candidate_id);
            self.reset_election_timer();

            let response = VoteResponse {
                term: self.current_term,
                vote_granted: true,
            };

            self.persist_state();
            self.send_message(from, RaftMessage::VoteResponse(response));
        } else {
            info!(
                "rejecting vote to {} for term {}: can_vote={}, log_up_to_date={}",
                msg.candidate_id, msg.term, can_vote, log_up_to_date
            );

            let response = VoteResponse {
                term: self.current_term,
                vote_granted: false,
            };

            self.send_message(from, RaftMessage::VoteResponse(response));
        }
    }

    async fn vote_response(&mut self, msg: VoteResponse) {
        info!("received VoteResponse from {}: {:?}", msg.term, msg);

        if msg.term < self.current_term {
            info!(
                "stale VoteResponse with term {}, current term is {}, ignoring",
                msg.term, self.current_term
            );
            return;
        }

        if msg.term > self.current_term {
            info!("stepping down to follower since received higher term in VoteResponse");
            self.become_follower(msg.term);
            return;
        }

        if let Role::Candidate { votes_received } = &mut self.role
            && msg.vote_granted
        {
            *votes_received += 1;

            if has_majority(*votes_received, self.peer_ids.len() + 1) {
                self.become_leader().await;
            }
        }
    }

    fn on_heartbeat(&self) {
        for peer_id in &self.peer_ids {
            let Role::Leader { next_index, .. } = &self.role else {
                unreachable!("handling heartbeat while not a leader");
            };

            // Each peer has its own next_index. If a peer is behind, it rejects
            // the request and the leader backs next_index up until logs match.
            let node_next_index = next_index.get(peer_id).copied().unwrap();
            let node_prev_log_index = node_next_index - 1;
            let prev_log_term = self.log[node_prev_log_index].term;

            let entries = self.log[node_next_index..].to_vec();

            let msg = RaftMessage::AppendRequest(AppendRequest {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: node_prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            });

            self.send_message(*peer_id, msg);
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = &self.log[self.last_applied];

            info!(
                "applying command at index {}: {:?}",
                self.last_applied, entry.command
            );

            match &entry.command {
                LogCommand::NoOp => {}

                LogCommand::Command(cmd) => {
                    let output = self.state_machine.apply(cmd);

                    if let Some(reply) = self.pending_applies.remove(&self.last_applied) {
                        let _ = reply.send(Ok(output));
                    }
                }
            }
        }
    }

    fn advance_commit(&mut self) {
        let Role::Leader { match_index, .. } = &self.role else {
            return;
        };

        // trying to find the highest index N where N > commit_index, and the majority of match_index[i] >= N
        for index in ((self.commit_index + 1)..self.log.len()).rev() {
            if self.log[index].term != self.current_term {
                continue;
            }

            let replicated_count = match_index
                .values()
                .filter(|&&match_idx| match_idx >= index)
                .count()
                + 1; // +1 for leader itself

            if has_majority(replicated_count, self.peer_ids.len() + 1) {
                info!(
                    "advancing commit index from {} to {}",
                    self.commit_index, index
                );
                self.commit_index = index;
                break;
            }
        }
    }

    fn persist_state(&self) {
        let state: PersistentState<RaftType> = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.clone(),
        };

        // todo we should return result from here indicating something went wrong, but for now just log and continue
        let bytes = match postcard::to_stdvec(&state) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("failed to serialize state for persistence: {:?}", e);
                return;
            }
        };

        match self.storage.save_state(bytes) {
            Ok(()) => {}
            Err(e) => {
                error!("failed to save state to storage: {:?}", e);
            }
        }
    }

    fn restore_state(&mut self) -> Result<()> {
        let bytes = self.storage.restore_state()?;
        if bytes.is_none() {
            return Ok(());
        }

        let state: PersistentState<RaftType> = postcard::from_bytes(&bytes.unwrap())?;
        self.current_term = state.current_term;
        self.voted_for = state.voted_for;
        self.log = state.log;
        Ok(())
    }

    fn send_message(&self, to_id: NodeId, message: RaftMessage<RaftType>) {
        let Some(sender) = self.transport_work.get(&to_id) else {
            warn!("no worker sender for node {to_id:?}");
            return;
        };

        // if node becomes unreachable, the channel will fill up and try_send will start failing, just ignore these errors
        let _ = sender.try_send(message);
    }

    fn broadcast_message(&self, message: RaftMessage<RaftType>) {
        for peer_id in &self.peer_ids {
            self.send_message(*peer_id, message.clone());
        }
    }

    // commands that we receive from clients, client -> raft_handler -> raft_node
    async fn client_command(&mut self, command: ClientCommands<RaftType>) {
        // helper function that returns correct error if the command requires us to be leader
        fn require_leader(role: &Role) -> Result<()> {
            match role {
                Role::Leader { .. } => Ok(()),
                Role::Follower {
                    current_leader_id: Some(leader),
                } => Err(RaftError::NotLeader(*leader)),
                Role::Follower {
                    current_leader_id: None,
                }
                | Role::Candidate { .. } => Err(RaftError::NoLeader),
            }
        }

        match command {
            ClientCommands::Apply { command, reply } => {
                info!("received apply command from client: {:?}", command);
                match require_leader(&self.role) {
                    Ok(()) => {
                        let index = self.log.len();
                        self.log.push(LogEntry {
                            term: self.current_term,
                            command: LogCommand::Command(command),
                        });
                        self.pending_applies.insert(index, reply);
                        self.persist_state();

                        // immediately try to replicate new command to followers
                        self.on_heartbeat();
                    }

                    Err(err) => {
                        let _ = reply.send(Err(err));
                    }
                }
            }

            ClientCommands::IsLeader { reply } => {
                info!("received is_leader command from client");
                let _ = reply.send(require_leader(&self.role));
            }

            ClientCommands::GetLog { reply } => {
                info!("received get_log command from client");
                let _ = reply.send(Ok(self.log.clone()));
            }

            ClientCommands::GetLogEntry { index, reply } => {
                info!(
                    "received get_log_entry command for index {} from client",
                    index
                );
                let _ = reply.send(Ok(self.log.get(index).cloned()));
            }

            ClientCommands::GetStatus { reply } => {
                info!("received get_status command from client");
                let _ = reply.send(Ok(self.status()));
            }
        }
    }

    fn status(&self) -> RaftStatus {
        let (role, leader_id) = match &self.role {
            Role::Follower { current_leader_id } => (RaftRole::Follower, *current_leader_id),
            Role::Candidate { votes_received } => (
                RaftRole::Candidate {
                    votes_received: *votes_received,
                },
                None,
            ),
            Role::Leader {
                next_index,
                match_index,
            } => (
                RaftRole::Leader {
                    next_index: next_index.clone(),
                    match_index: match_index.clone(),
                },
                Some(self.id),
            ),
        };

        RaftStatus {
            id: self.id,
            current_term: self.current_term,
            voted_for: self.voted_for,
            role,
            leader_id,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            log_len: self.log.len(),
            peer_ids: self.peer_ids.clone(),
        }
    }
}

fn has_majority(count: usize, total: usize) -> bool {
    count > total / 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::assert_matches;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
    use tokio::time::timeout;

    #[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
    enum TestCommand {
        Inc(i64),
        Dec(i64),
        Set(i64),
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestOutput {
        Value(i64),
    }

    #[derive(Default, Serialize, Deserialize)]
    struct TestStateMachine {
        value: i64,
    }

    #[derive(Debug, Clone, Copy)]
    struct TestTypeConfig;

    impl RaftTypeConfig for TestTypeConfig {
        type Command = TestCommand;
        type Output = TestOutput;
    }

    impl RaftStateMachine<TestTypeConfig> for TestStateMachine {
        fn apply(&mut self, command: &TestCommand) -> TestOutput {
            match command {
                TestCommand::Inc(n) => {
                    self.value += n;
                    TestOutput::Value(self.value)
                }
                TestCommand::Dec(n) => {
                    self.value -= n;
                    TestOutput::Value(self.value)
                }
                TestCommand::Set(n) => {
                    self.value = *n;
                    TestOutput::Value(self.value)
                }
            }
        }
    }

    type TestNode =
        RaftNode<TestTypeConfig, TestStateMachine, TestTransport<TestTypeConfig>, TestStorage>;

    type TestSentRx = UnboundedReceiver<RaftEnvelope<TestTypeConfig>>;

    struct TestStorage {
        path: PathBuf,
    }

    impl Default for TestStorage {
        fn default() -> Self {
            static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "raft-rs-test-state-{}-{id}.raft-rs",
                std::process::id()
            ));

            Self { path }
        }
    }

    impl Drop for TestStorage {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    impl RaftStorage<TestTypeConfig> for TestStorage {
        fn save_state(&self, bytes: Vec<u8>) -> Result<()> {
            fs::write(&self.path, bytes).unwrap();
            Ok(())
        }

        fn restore_state(&self) -> Result<Option<Vec<u8>>> {
            if self.path.exists() {
                let bytes = fs::read(&self.path).unwrap();
                Ok(Some(bytes))
            } else {
                Ok(None)
            }
        }
    }

    struct TestTransport<RaftType: RaftTypeConfig> {
        sent_tx: UnboundedSender<RaftEnvelope<RaftType>>,
    }

    impl<RaftType: RaftTypeConfig> TestTransport<RaftType> {
        pub fn new(sent_tx: UnboundedSender<RaftEnvelope<RaftType>>) -> Self {
            Self { sent_tx }
        }
    }

    #[async_trait::async_trait]
    impl<RaftType: RaftTypeConfig> RaftTransport<RaftType> for TestTransport<RaftType> {
        async fn send(
            &self,
            from: NodeId,
            _to: NodeId,
            message: RaftMessage<RaftType>,
        ) -> Result<()> {
            self.sent_tx
                .send(RaftEnvelope { from, message })
                .map_err(|_| RaftError::Error("test transport receiver dropped".to_string()))
        }
    }

    fn test_node(
        id: NodeId,
        peer_ids: Vec<NodeId>,
    ) -> (TestNode, RaftHandle<TestTypeConfig>, TestSentRx) {
        let (sent_tx, sent_rx) = mpsc::unbounded_channel();
        let (node, handle) = RaftNode::new(
            id,
            peer_ids,
            TestStateMachine::default(),
            TestTransport::new(sent_tx),
            TestStorage::default(),
            RaftConfig::default(),
        );

        (node, handle, sent_rx)
    }

    async fn recv_sent_messages(
        sent_rx: &mut TestSentRx,
        expected: usize,
    ) -> Vec<RaftEnvelope<TestTypeConfig>> {
        timeout(Duration::from_secs(2), async {
            let mut sent = Vec::with_capacity(expected);
            for _ in 0..expected {
                sent.push(sent_rx.recv().await.expect("test transport sender dropped"));
            }

            sent
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {expected} sent messages"))
    }

    fn command_entry(term: u64, command: TestCommand) -> LogEntry<TestCommand> {
        LogEntry {
            term,
            command: LogCommand::Command(command),
        }
    }

    async fn send_raft_message(
        node: &mut TestNode,
        handle: &RaftHandle<TestTypeConfig>,
        from: NodeId,
        message: RaftMessage<TestTypeConfig>,
    ) {
        handle
            .send_message(RaftEnvelope { from, message })
            .await
            .unwrap();

        let envelope = node.message_receiver.recv().await.unwrap();
        node.handle_message(envelope).await;
    }

    fn assert_noop(entry: &LogEntry<TestCommand>, term: u64) {
        assert_eq!(entry.term, term);
        assert_matches!(entry.command, LogCommand::NoOp);
    }

    fn assert_command(entry: &LogEntry<TestCommand>, term: u64, expected: TestCommand) {
        assert_eq!(entry.term, term);

        match &entry.command {
            LogCommand::Command(actual) => assert_eq!(actual, &expected),
            LogCommand::NoOp => panic!("expected command entry, got NoOp"),
        }
    }

    #[tokio::test]
    async fn new_initializes_as_follower_with_dummy_log() {
        let (node, _, _) = test_node(1, vec![2, 3]);

        assert_eq!(node.current_term, 0);
        assert_eq!(node.voted_for, None);

        assert_matches!(
            node.role,
            Role::Follower {
                current_leader_id: None
            }
        );

        assert_eq!(node.commit_index, 0);
        assert_eq!(node.last_applied, 0);
        assert_eq!(node.log.len(), 1);
        assert_noop(&node.log[0], 0);
    }

    #[tokio::test]
    async fn become_candidate_votes_for_self_and_sends_vote_requests() {
        let (mut node, _, mut sent_rx) = test_node(1, vec![2, 3]);

        node.become_candidate().await;

        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(1));

        match &node.role {
            Role::Candidate { votes_received } => assert_eq!(*votes_received, 1),
            other => panic!("expected candidate, got {other:?}"),
        }

        let sent = recv_sent_messages(&mut sent_rx, 2).await;
        assert_eq!(sent.len(), 2);

        for envelope in &sent {
            assert_eq!(envelope.from, 1);

            match &envelope.message {
                RaftMessage::VoteRequest(request) => {
                    assert_eq!(request.term, 1);
                    assert_eq!(request.candidate_id, 1);
                    assert_eq!(request.last_log_index, 0);
                    assert_eq!(request.last_log_term, 0);
                }
                other => panic!("expected VoteRequest, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn candidate_becomes_leader_after_majority_vote() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        node.become_candidate().await;
        let _ = recv_sent_messages(&mut sent_rx, 2).await;

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::VoteResponse(VoteResponse {
                term: 1,
                vote_granted: true,
            }),
        )
        .await;

        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(1));
        assert_eq!(node.log.len(), 2);
        assert_noop(&node.log[1], 1);

        match &node.role {
            Role::Leader {
                next_index,
                match_index,
            } => {
                assert_eq!(next_index.get(&2), Some(&2));
                assert_eq!(next_index.get(&3), Some(&2));
                assert_eq!(match_index.get(&2), Some(&0));
                assert_eq!(match_index.get(&3), Some(&0));
            }
            other => panic!("expected leader, got {other:?}"),
        }

        let sent = recv_sent_messages(&mut sent_rx, 2).await;
        assert_eq!(sent.len(), 2);

        for envelope in &sent {
            match &envelope.message {
                RaftMessage::AppendRequest(request) => {
                    assert_eq!(request.term, 1);
                    assert_eq!(request.leader_id, 1);
                    assert_eq!(request.prev_log_index, 1);
                    assert_eq!(request.prev_log_term, 1);
                    assert!(request.entries.is_empty());
                    assert_eq!(request.leader_commit, 0);
                }
                other => panic!("expected AppendRequest, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn higher_term_vote_response_steps_candidate_down() {
        let (mut node, handle, _) = test_node(1, vec![2, 3]);

        node.become_candidate().await;

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::VoteResponse(VoteResponse {
                term: 7,
                vote_granted: true,
            }),
        )
        .await;

        assert_eq!(node.current_term, 7);
        assert_eq!(node.voted_for, None);

        assert_matches!(
            node.role,
            Role::Follower {
                current_leader_id: None
            }
        );
    }

    #[tokio::test]
    async fn vote_request_grants_first_vote_for_up_to_date_candidate() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        node.current_term = 1;

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::VoteRequest(VoteRequest {
                term: 2,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        )
        .await;

        assert_eq!(node.current_term, 2);
        assert_eq!(node.voted_for, Some(2));
        let sent = recv_sent_messages(&mut sent_rx, 1).await;
        assert_eq!(sent.len(), 1);

        let envelope = &sent[0];
        assert_eq!(envelope.from, 1);

        match &envelope.message {
            RaftMessage::VoteResponse(response) => {
                assert_eq!(response.term, 2);
                assert!(response.vote_granted);
            }
            other => panic!("expected VoteResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn vote_request_rejects_candidate_with_stale_log() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        node.current_term = 3;
        node.log.push(command_entry(3, TestCommand::Set(9)));

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::VoteRequest(VoteRequest {
                term: 3,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        )
        .await;

        assert_eq!(node.voted_for, None);
        let sent = recv_sent_messages(&mut sent_rx, 1).await;
        assert_eq!(sent.len(), 1);

        let envelope = &sent[0];
        assert_eq!(envelope.from, 1);

        match &envelope.message {
            RaftMessage::VoteResponse(response) => {
                assert_eq!(response.term, 3);
                assert!(!response.vote_granted);
            }
            other => panic!("expected VoteResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_request_rejects_lower_term() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        node.current_term = 4;
        node.log.push(command_entry(4, TestCommand::Inc(1)));

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::AppendRequest(AppendRequest {
                term: 3,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        )
        .await;

        assert_eq!(node.current_term, 4);
        assert_eq!(node.log.len(), 2);
        let sent = recv_sent_messages(&mut sent_rx, 1).await;
        assert_eq!(sent.len(), 1);

        let envelope = &sent[0];
        assert_eq!(envelope.from, 1);

        match &envelope.message {
            RaftMessage::AppendResponse(response) => {
                assert_eq!(response.term, 4);
                assert_eq!(response.match_index, 0);
                assert!(!response.success);
            }
            other => panic!("expected AppendResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_request_from_current_term_leader_steps_candidate_down() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        node.current_term = 2;
        node.voted_for = Some(1);
        node.role = Role::Candidate { votes_received: 1 };

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::AppendRequest(AppendRequest {
                term: 2,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        )
        .await;

        assert_eq!(node.current_term, 2);

        match &node.role {
            Role::Follower { current_leader_id } => assert_eq!(*current_leader_id, Some(2)),
            other => panic!("expected follower, got {other:?}"),
        }

        let sent = recv_sent_messages(&mut sent_rx, 1).await;
        assert_eq!(sent.len(), 1);

        let envelope = &sent[0];
        assert_eq!(envelope.from, 1);

        match &envelope.message {
            RaftMessage::AppendResponse(response) => {
                assert_eq!(response.term, 2);
                assert_eq!(response.match_index, 0);
                assert!(response.success);
            }
            other => panic!("expected AppendResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_request_truncates_conflicting_entries_and_appends_new_entries() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        node.current_term = 3;
        node.log.push(command_entry(1, TestCommand::Inc(1)));
        node.log.push(command_entry(2, TestCommand::Inc(2)));
        node.log.push(command_entry(2, TestCommand::Dec(1)));

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::AppendRequest(AppendRequest {
                term: 3,
                leader_id: 2,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![
                    command_entry(3, TestCommand::Set(7)),
                    command_entry(3, TestCommand::Inc(1)),
                ],
                leader_commit: 0,
            }),
        )
        .await;

        assert_eq!(node.log.len(), 4);
        assert_command(&node.log[1], 1, TestCommand::Inc(1));
        assert_command(&node.log[2], 3, TestCommand::Set(7));
        assert_command(&node.log[3], 3, TestCommand::Inc(1));

        assert_eq!(node.commit_index, 0);
        assert_eq!(node.last_applied, 0);
        assert_eq!(node.state_machine.value, 0);
        let sent = recv_sent_messages(&mut sent_rx, 1).await;
        assert_eq!(sent.len(), 1);

        let envelope = &sent[0];
        assert_eq!(envelope.from, 1);

        match &envelope.message {
            RaftMessage::AppendResponse(response) => {
                assert_eq!(response.term, 3);
                assert_eq!(response.match_index, 3);
                assert!(response.success);
            }
            other => panic!("expected AppendResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_request_commits_and_applies_up_to_leader_commit() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::AppendRequest(AppendRequest {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    command_entry(1, TestCommand::Inc(5)),
                    command_entry(1, TestCommand::Dec(2)),
                    command_entry(1, TestCommand::Set(10)),
                ],
                leader_commit: 2,
            }),
        )
        .await;

        assert_eq!(node.log.len(), 4);
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.last_applied, 2);
        assert_eq!(node.state_machine.value, 3);
        assert_command(&node.log[3], 1, TestCommand::Set(10));
        let sent = recv_sent_messages(&mut sent_rx, 1).await;
        assert_eq!(sent.len(), 1);

        let envelope = &sent[0];
        assert_eq!(envelope.from, 1);

        match &envelope.message {
            RaftMessage::AppendResponse(response) => {
                assert_eq!(response.term, 1);
                assert_eq!(response.match_index, 3);
                assert!(response.success);
            }
            other => panic!("expected AppendResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn advance_commit_requires_majority_on_current_term_entry() {
        let (mut node, _, _) = test_node(1, vec![2, 3]);

        node.current_term = 2;
        node.log.push(command_entry(1, TestCommand::Inc(10)));
        node.log.push(command_entry(2, TestCommand::Inc(20)));

        node.role = Role::Leader {
            next_index: HashMap::from([(2, 3), (3, 3)]),
            match_index: HashMap::from([(2, 1), (3, 1)]),
        };

        node.advance_commit();
        assert_eq!(node.commit_index, 0);

        match &mut node.role {
            Role::Leader { match_index, .. } => {
                match_index.insert(2, 2);
            }
            other => panic!("expected leader, got {other:?}"),
        }

        node.advance_commit();
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.last_applied, 0);
    }

    #[tokio::test]
    async fn node_commands_test() {
        let (mut node, _, _) = test_node(1, vec![2, 3]);

        let (reply, rx) = oneshot::channel();
        node.client_command(ClientCommands::IsLeader { reply })
            .await;
        let err = rx.await.unwrap().unwrap_err();

        assert_matches!(err, RaftError::NoLeader);

        if let Role::Follower { current_leader_id } = &mut node.role {
            *current_leader_id = Some(2);
        }

        let (reply, rx) = oneshot::channel();
        node.client_command(ClientCommands::IsLeader { reply })
            .await;
        let err = rx.await.unwrap().unwrap_err();

        assert_matches!(err, RaftError::NotLeader(2));

        node.become_leader().await;
        let (reply, rx) = oneshot::channel();
        node.client_command(ClientCommands::IsLeader { reply })
            .await;
        assert!(rx.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn append_response_success_commits_and_replies_to_pending_client_command() {
        let (mut node, handle, mut sent_rx) = test_node(1, vec![2, 3]);

        node.current_term = 1;
        node.become_leader().await;
        let _ = recv_sent_messages(&mut sent_rx, 2).await;

        let (reply, rx) = oneshot::channel();

        node.client_command(ClientCommands::Apply {
            command: TestCommand::Inc(4),
            reply,
        })
        .await;

        assert_eq!(node.log.len(), 3);
        assert_noop(&node.log[1], 1);
        assert_command(&node.log[2], 1, TestCommand::Inc(4));
        assert_eq!(node.commit_index, 0);
        assert_eq!(node.last_applied, 0);
        assert_eq!(recv_sent_messages(&mut sent_rx, 2).await.len(), 2);

        send_raft_message(
            &mut node,
            &handle,
            2,
            RaftMessage::AppendResponse(AppendResponse {
                term: 1,
                match_index: 2,
                success: true,
            }),
        )
        .await;

        assert_eq!(rx.await.unwrap().unwrap(), TestOutput::Value(4));
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.last_applied, 2);
        assert_eq!(node.state_machine.value, 4);

        match &node.role {
            Role::Leader {
                next_index,
                match_index,
            } => {
                assert_eq!(next_index.get(&2), Some(&3));
                assert_eq!(match_index.get(&2), Some(&2));
            }
            other => panic!("expected leader, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn storage_persists_and_restores_state() {
        let storage = TestStorage::default();

        let state: PersistentState<TestTypeConfig> = PersistentState {
            current_term: 5,
            voted_for: Some(3),
            log: vec![
                command_entry(1, TestCommand::Inc(1)),
                command_entry(2, TestCommand::Dec(2)),
                command_entry(3, TestCommand::Set(7)),
            ],
        };

        storage
            .save_state(postcard::to_stdvec(&state).unwrap())
            .unwrap();

        // todo unwrap unwrap is a bit sad
        let restored_bytes = storage.restore_state().unwrap().unwrap();
        let restored_state: PersistentState<TestTypeConfig> =
            postcard::from_bytes(&restored_bytes).unwrap();

        assert_eq!(state.current_term, restored_state.current_term);
        assert_eq!(state.voted_for, restored_state.voted_for);
        assert_eq!(state.log.len(), restored_state.log.len());
    }
}
