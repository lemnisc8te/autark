//! Schedule-related types

use std::sync::Arc;

use crate::model::flow::{ErasedNode, NodeID};

/// Alias for a [`usize`] that is intended to reference a buffer in an [`AudioBufferPool`](crate::engine::util::abp::AudioBufferPool)
pub type SlotIndex = usize;

#[derive(Clone)]
/// A [`ScheduleStep`] defines one transformation on a block in the compiled schedule.
pub struct ScheduleStep {
    /// The node defining the behavior for this step.
    pub node: Arc<dyn ErasedNode>,
    /// The ID of this step's `node`.
    pub node_id: NodeID,
    /// The input slots used in this step.
    pub input_slots: Vec<SlotIndex>,
    /// The output slots used in this step.
    pub output_slots: Vec<SlotIndex>,
}

#[derive(Default, Clone)]
/// This [`CompiledSchedule`] defines the order in which each [`ScheduleStep`] should execute per block.
pub struct CompiledSchedule {
    /// The order of steps per block.
    pub steps: Vec<ScheduleStep>,
    /// The maximum number of slots
    pub buffer_count: usize,
    /// The final audio slot, fed to the audio callback
    pub capture_slot: SlotIndex,
}
