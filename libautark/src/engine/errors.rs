use thiserror::Error;

use crate::model::{
    DataKind,
    flow::{NodeID, socket::SocketID},
};

#[derive(Debug, Error)]
pub enum EngineError {
    TrackNotFound,
    NodeNotFound(NodeID),
    SocketNotFound(SocketID),
    IncompatibleSockets { from: DataKind, to: DataKind },
    WouldCreateCycle,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
