use std::sync::Arc;

use crate::model::flow::{ErasedNode, NodeID};

pub type SlotIndex = usize;

#[derive(Clone)]
pub struct ScheduleStep {
    pub node: Arc<dyn ErasedNode>,
    pub node_id: NodeID,
    pub input_slots: Vec<SlotIndex>,
    pub output_slots: Vec<SlotIndex>,
}

#[derive(Default, Clone)]
pub struct CompiledGraph {
    pub steps: Vec<ScheduleStep>,
    pub buffer_count: usize,
    pub capture_slot: SlotIndex,
}
