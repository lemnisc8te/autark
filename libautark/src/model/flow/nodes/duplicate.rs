use core::marker::PhantomData;

use crate::{
    engine::{schedule::SlotIndex, tick::Tick, util::abp::PoolExecutor},
    model::{
        Audio, DataKind, Kind,
        flow::{MultiInputNode, Node, Socket},
    },
};

/// Duplicates an incoming signal into multiple output copies
///
///  # Spec
/// ## Inputs
/// 0) Main Input: K
/// ## Outputs
/// 0) Variadic Output: K
#[derive(Debug, Clone, Default)]
pub struct Duplicate<K: Kind> {
    kind: PhantomData<K>,
}

impl<K: Kind> Duplicate<K> {
    #[must_use]
    pub fn new() -> Self {
        Self { kind: PhantomData }
    }
}

impl Duplicate<Audio> {}

impl Node for Duplicate<Audio> {
    type State = ();

    fn init_state(&self) -> Self::State {}

    fn process(
        &self,
        pool: &mut PoolExecutor,
        _state: &mut Self::State,
        _: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        let input = pool.get_input(inputs[0]);
        for output_slot in outputs {
            let output_buf = pool.get_output(outputs[*output_slot]);
            for sample_idx in 0..pool.block_size {
                output_buf[sample_idx] += input[sample_idx];
            }
        }
    }

    fn spec_in(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "out", true)]
    }

    fn spec_out(&self) -> Vec<Socket> {
        vec![]
    }
}

impl MultiInputNode for Duplicate<Audio> {}
