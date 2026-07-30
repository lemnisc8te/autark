use std::{any::Any, sync::Arc};

use slotmap::SecondaryMap;

use crate::{
    engine::{CompiledGraph, constants::MAX_NODES},
    model::{flow::NodeID, project::ProjectData},
};

/// Everything a topology change implies, computed entirely off the audio
/// thread and handed over as one atomic unit so the schedule and the state
/// pool additions it depends on can never arrive out of sync.
#[derive(Default)]
pub struct GraphUpdate {
    pub project: Arc<ProjectData>,
    pub schedule: CompiledGraph,
    pub state_additions: Vec<(NodeID, Box<dyn Any + Send>)>,
    pub state_removals: Vec<NodeID>,
}

pub enum Garbage {
    Update(GraphUpdate),
    NodeState(Box<dyn Any + Send>),
}

/// Per-node mutable DSP state (Tier 3). Lives exclusively on the audio
/// thread; never appears in `ProjectData`, so undo/redo and cloning never
/// touch it and it never needs to be `Sync`.
#[derive(Default)]
pub struct NodeStatePool {
    states: SecondaryMap<NodeID, Box<dyn Any + Send>>,
}

impl NodeStatePool {
    /// Creates a new [`NodeStatePool`].
    pub(crate) fn new() -> Self {
        Self {
            states: SecondaryMap::with_capacity(MAX_NODES),
        }
    }

    pub fn get_mut(&mut self, id: NodeID) -> &mut dyn Any {
        self.states
            .get_mut(id)
            .expect("node processed without a reconciled state entry")
            .as_mut()
    }

    /// Applies a structural update: inserts new nodes' pre-built state,
    /// removes stale entries (routing them to `garbage` instead of dropping
    /// them here). No allocation: `SecondaryMap` was pre-sized to
    /// `MAX_NODES`, so inserting keys under that bound never reallocates.
    pub fn apply(&mut self, update: &mut GraphUpdate, garbage: &mut rtrb::Producer<Garbage>) {
        for (id, state) in update.state_additions.drain(..) {
            self.states.insert(id, state);
        }
        for id in update.state_removals.drain(..) {
            if let Some(old) = self.states.remove(id) {
                let _ = garbage.push(Garbage::NodeState(old));
            }
        }
    }
}
