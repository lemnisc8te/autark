use serde::{Deserialize, Serialize};
use slotmap::new_key_type;

use crate::model::{DataKind, flow::NodeID};

new_key_type! {pub struct SocketID;}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SocketDirection {
    Input,
    Output,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocketMeta {
    pub owner: NodeID,
    pub direction: SocketDirection,
    pub kind: DataKind,
    pub name: String,
    pub visible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Socket {
    pub kind: DataKind,
    pub name: String,
    pub visible: bool,
}

impl Socket {
    pub fn new(kind: DataKind, name: impl Into<String>, visible: bool) -> Self {
        Self {
            kind,
            name: name.into(),
            visible,
        }
    }
}

impl SocketMeta {
    pub fn new(
        owner: NodeID,
        direction: SocketDirection,
        name: impl Into<String>,
        kind: DataKind,
        visible: bool,
    ) -> SocketMeta {
        Self {
            owner,
            direction,
            kind,
            name: name.into(),
            visible,
        }
    }
}
