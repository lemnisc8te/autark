use std::{any::Any, collections::HashSet};

use slotmap::SecondaryMap;

use crate::{
    engine::{CompiledGraph, constants::MAX_NODES},
    model::flow::NodeID,
};

#[derive(Default)]
pub struct GraphUpdate {
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

pub trait DiffProvider {
    type Element;
    // Iterators must be returned by value, not by reference
    type Additions<'a>: Iterator<Item = &'a Self::Element>
    where
        Self: 'a;
    type Removals<'a>: Iterator<Item = &'a Self::Element>
    where
        Self: 'a;

    fn additions(&self) -> Self::Additions<'_>;
    fn removals(&self) -> Self::Removals<'_>;
}

pub trait CanDiff {
    type Element;
    type Provider: DiffProvider<Element = Self::Element>;

    // Elements must be yielded by reference if you plan to keep the collection intact
    type Elements<'a>: Iterator<Item = &'a Self::Element>
    where
        Self: 'a;
    fn elements(&self) -> Self::Elements<'_>;

    // Concrete implementation handles mutating the collection using the provider
    fn apply_diff(&mut self, diff: &Self::Provider);
}

// 1. Define a concrete Diff Provider
pub struct VecDiff<T> {
    pub added: Vec<T>,
    pub removed: Vec<T>,
}

impl<T> DiffProvider for VecDiff<T> {
    type Element = T;
    type Additions<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;
    type Removals<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    fn additions(&self) -> Self::Additions<'_> {
        self.added.iter()
    }

    fn removals(&self) -> Self::Removals<'_> {
        self.removed.iter()
    }
}

// 2. Implement CanDiff for Vec<T>
impl<T: Eq + std::hash::Hash + Clone> CanDiff for Vec<T> {
    type Element = T;
    type Provider = VecDiff<T>;
    type Elements<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    fn elements(&self) -> Self::Elements<'_> {
        self.iter()
    }

    fn apply_diff(&mut self, diff: &Self::Provider) {
        // Step A: Remove items
        let to_remove: HashSet<&T> = diff.removals().collect();
        self.retain(|item| !to_remove.contains(item));

        // Step B: Add items
        for item in diff.additions() {
            self.push(item.clone());
        }
    }
}
