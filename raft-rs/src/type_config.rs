use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

pub trait RaftTypeConfig: Debug + Clone + Send + Sync + 'static {
    type Command: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static; // State machine command
    type Output: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static; // State machine output
}
