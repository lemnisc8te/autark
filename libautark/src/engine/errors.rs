use thiserror::Error;

use crate::model::{
    DataKind,
    flow::{NodeID, socket::SocketID},
};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Track was not found")]
    TrackNotFound,
    #[error("Node {0:?} was not found")]
    NodeNotFound(NodeID),
    #[error("Socket {0:?} was not found")]
    SocketNotFound(SocketID),
    #[error("Cannot create link between {from:?} and {to:?}")]
    IncompatibleSockets { from: DataKind, to: DataKind },
    #[error("Attempting this would create a cycle")]
    WouldCreateCycle,
}
