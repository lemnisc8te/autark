use thiserror::Error;

use crate::model::{
    DataKind,
    flow::{NodeID, socket::InputSocketID},
};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Track was not found")]
    TrackNotFound,
    #[error("Node {0:?} was not found")]
    NodeNotFound(NodeID),
    #[error("Socket {0:?} was not found")]
    SocketNotFound(InputSocketID),
    #[error("Cannot create link between {from:?} and {to:?}")]
    IncompatibleSockets { from: DataKind, to: DataKind },
    #[error("Attempting this would create a cycle")]
    WouldCreateCycle,
}
