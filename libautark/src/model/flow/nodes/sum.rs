use std::marker::PhantomData;

use crate::{
    engine::{SlotIndex, bbp::PoolExecutor, tick::Tick},
    model::{
        Audio, DataKind, Kind,
        flow::{MultiInputNode, Node, Socket},
    },
};

#[derive(Debug, Clone, Default)]
pub struct Sum<K: Kind> {
    kind: PhantomData<K>,
}

impl<K: Kind> Sum<K> {
    #[must_use]
    pub fn new() -> Self {
        Self { kind: PhantomData }
    }
}

impl Sum<Audio> {}

pub struct SumState {}

impl Node for Sum<Audio> {
    type State = SumState;

    fn init_state(&self) -> Self::State {
        SumState {}
    }

    fn process(
        &self,
        pool: &mut PoolExecutor,
        _state: &mut Self::State,
        _: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        let output_buf = pool.get_output(outputs[0]);
        for input_index in inputs {
            let source = pool.get_input(*input_index);
            // Simple additive loop: easily optimized by hardware auto-vectorization
            for sample_idx in 0..pool.block_size {
                output_buf[sample_idx] += source[sample_idx];
            }
        }
    }

    fn spec_in(&self) -> Vec<Socket> {
        vec![]
    }

    fn spec_out(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "out", true)]
    }
}

impl MultiInputNode for Sum<Audio> {}
