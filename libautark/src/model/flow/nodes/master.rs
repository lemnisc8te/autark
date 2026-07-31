use crate::{
    engine::{SlotIndex, bbp::PoolExecutor, tick::Tick},
    model::{
        DataKind,
        flow::{Node, Socket},
    },
};

#[derive(Debug, Clone)]
pub struct Master;

impl Node for Master {
    type State = ();

    fn init_state(&self) -> Self::State {}

    fn process(
        &self,
        pool: &mut PoolExecutor,
        (): &mut (),
        _: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        let input_buf = pool.get_input(inputs[0]);
        let output_buf = pool.get_output(outputs[0]);

        output_buf.copy_from_slice(input_buf);
    }

    fn spec_in(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "in", true)]
    }

    fn spec_out(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "out", false)]
    }
}
