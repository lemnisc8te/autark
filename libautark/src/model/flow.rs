//! Module for Flow related types
use core::any::Any;

use dyn_clone::DynClone;
use slotmap::new_key_type;

use crate::{
    engine::{schedule::SlotIndex, tick::Tick, util::abp::PoolExecutor},
    model::flow::socket::{InputSocketID, Socket},
};

pub mod graph;
pub mod nodes;
pub mod socket;

new_key_type! {
    pub struct NodeID;
}

// mod param {
//     use core::sync::atomic::Ordering;

//     use core::sync::atomic::AtomicU32;

//     use std::sync::Arc;
//     use std::sync::atomic::AtomicBool;

//     #[derive(Debug, Clone)]
//     pub struct ParamF32(Arc<AtomicU32>);

//     impl ParamF32 {
//         #[must_use]
//         pub fn new(v: f32) -> Self {
//             Self(Arc::new(AtomicU32::new(v.to_bits())))
//         }
//         #[inline]
//         #[must_use]
//         pub fn get(&self) -> f32 {
//             f32::from_bits(self.0.load(Ordering::Relaxed))
//         }
//         #[inline]
//         pub fn set(&self, v: f32) {
//             self.0.store(v.to_bits(), Ordering::Relaxed);
//         }
//     }

//     #[derive(Debug, Clone)]
//     pub struct ParamBool(Arc<AtomicBool>);

//     impl ParamBool {
//         #[must_use]
//         pub fn new(v: f32) -> Self {
//             Self(Arc::new(AtomicU32::new(v.to_bits())))
//         }
//         #[inline]
//         #[must_use]
//         pub fn get(&self) -> f32 {
//             f32::from_bits(self.0.load(Ordering::Relaxed))
//         }
//         #[inline]
//         pub fn set(&self, v: f32) {
//             self.0.store(v.to_bits(), Ordering::Relaxed);
//         }
//     }
// }

pub trait Node: core::fmt::Debug + DynClone + Send + Sync + 'static {
    type State: Send + 'static;

    fn spec_in(&self) -> Vec<Socket>;
    fn spec_out(&self) -> Vec<Socket>;

    /// Fresh runtime state for a new instance of this node, built off the
    /// audio thread (control thread, inside `publish_current`) and handed
    /// over pre-built.
    fn init_state(&self) -> Self::State;

    fn process(
        &self,
        pool: &mut PoolExecutor,
        state: &mut Self::State,
        block_start: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    );

    fn grow_input(&mut self) -> anyhow::Result<Socket> {
        anyhow::bail!("Tried growing input arity on a fixed-arity node")
    }
    fn shrink_input(&mut self, _socket: InputSocketID) -> bool {
        false
    } // true = safe to actually remove
}

pub trait MultiInputNode: ErasedNode {}

pub trait ErasedNode: core::fmt::Debug + DynClone + Send + Sync + 'static {
    fn spawn_state(&self) -> Box<dyn Any + Send>;
    fn process_erased(
        &self,
        pool: &mut PoolExecutor,
        state: &mut dyn Any,
        block_start: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    );
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any(&self) -> &dyn Any;
}

dyn_clone::clone_trait_object!(ErasedNode);

impl<N: Node> ErasedNode for N {
    fn spawn_state(&self) -> Box<dyn Any + Send> {
        Box::new(self.init_state())
    }

    fn process_erased(
        &self,
        pool: &mut PoolExecutor,
        state: &mut dyn Any,
        block_start: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        let state = state.downcast_mut::<N::State>().unwrap();
        self.process(pool, state, block_start, inputs, outputs);
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
