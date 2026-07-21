use crate::type_config::RaftTypeConfig;

pub trait RaftStateMachine<RaftType: RaftTypeConfig> {
    fn apply(&mut self, command: &RaftType::Command) -> RaftType::Output;
}
