use crate::error::Result;
use crate::message::RaftMessage;
use crate::type_config::RaftTypeConfig;
use crate::types::NodeId;

#[async_trait::async_trait]
pub trait RaftTransport<RaftType: RaftTypeConfig>: Send + Sync + 'static {
    async fn send(&self, from: NodeId, to: NodeId, message: RaftMessage<RaftType>) -> Result<()>;
}
