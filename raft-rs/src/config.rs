use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub transport_timeout_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 75,
            transport_timeout_ms: 350, // idk what a good default is
        }
    }
}

impl RaftConfig {
    pub fn new(
        election_timeout_min_ms: u64,
        election_timeout_max_ms: u64,
        heartbeat_interval_ms: u64,
        transport_timeout_ms: u64,
    ) -> Self {
        assert!(
            election_timeout_min_ms < election_timeout_max_ms,
            "Election timeout min must be less than max"
        );

        RaftConfig {
            election_timeout_min_ms,
            election_timeout_max_ms,
            heartbeat_interval_ms,
            transport_timeout_ms,
        }
    }

    pub(crate) fn random_election_timeout(&self) -> Duration {
        Duration::from_millis(rand::random_range(
            self.election_timeout_min_ms..self.election_timeout_max_ms,
        ))
    }
}
