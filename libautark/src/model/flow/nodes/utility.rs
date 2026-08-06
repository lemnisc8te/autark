//! Utility Node
use crate::{
    engine::{
        Tick,
        schedule::SlotIndex,
        util::{
            abp::PoolExecutor,
            math::{F32_EQ_ERR_MARGIN, INV_SQRT_2},
        },
    },
    model::{
        DataKind,
        flow::{Node, socket::Socket},
    },
};

use itertools::Itertools;

#[derive(Debug, Clone, Default)]
/// Utility offers a few primitive signal operations.
///
/// # Spec
/// ## Inputs
/// 0) Main Input: K
/// ## Outputs
/// 0) Variadic Output: K
pub struct Utility;

impl Utility {
    /// Creates a new [`Utility`].
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

pub struct UtilityState {
    pub gain: f32,
    pub phase_invert: bool, // If true, inverts the phase (multiplies by -1)
    pub stereo_width: f32,  // 0.0 = mono, 1.0 = normal, >1.0 = extra wide}
}
impl Node for Utility {
    type State = UtilityState;

    fn init_state(&self) -> Self::State {
        UtilityState {
            gain: 1.0,
            phase_invert: false,
            stereo_width: 1.0,
        }
    }

    fn process(
        &self,
        pool: &mut PoolExecutor,
        state: &mut Self::State,
        _: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        let phase_mult = if state.phase_invert { -1.0 } else { 1.0 };
        let output_buf = pool.get_output(outputs[0]);
        let input_buf = pool.get_input(inputs[0]);

        for (sample_idx_l, sample_idx_r) in (0..(pool.block_size / 2)).tuple_windows() {
            let mut left = input_buf[sample_idx_l];
            let mut right = input_buf[sample_idx_r];

            // 1. Phase Inversion
            left *= phase_mult;
            right *= phase_mult;

            // 2. Stereo Width (Mid-Side Processing)
            if (state.stereo_width - 1.0).abs() > F32_EQ_ERR_MARGIN {
                let mid = (left + right) * INV_SQRT_2;
                let side = (left - right) * INV_SQRT_2 * state.stereo_width;

                left = (mid + side) * INV_SQRT_2;
                right = (mid - side) * INV_SQRT_2;
            }

            // 3. Gain Application
            output_buf[sample_idx_l] = left * state.gain;
            output_buf[sample_idx_r] = right * state.gain;
        }
    }

    fn spec_in(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "audio in", true)]
    }

    fn spec_out(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "audio out", true)]
    }
}
