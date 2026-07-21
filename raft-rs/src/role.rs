use crate::types::NodeId;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) enum Role {
    Follower {
        current_leader_id: Option<NodeId>,
    },
    Candidate {
        votes_received: usize,
    },
    Leader {
        next_index: HashMap<NodeId, usize>,
        match_index: HashMap<NodeId, usize>,
    },
}

impl Role {
    pub(crate) fn is_leader(&self) -> bool {
        matches!(self, Role::Leader { .. })
    }

    pub(crate) fn is_candidate(&self) -> bool {
        matches!(self, Role::Candidate { .. })
    }

    #[allow(dead_code)]
    pub(crate) fn is_follower(&self) -> bool {
        matches!(self, Role::Follower { .. })
    }
}
