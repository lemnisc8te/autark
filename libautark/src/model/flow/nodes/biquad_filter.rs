use std::f32::consts::PI;

use crate::{
    engine::{SlotIndex, tick::Tick},
    model::{
        DataKind,
        flow::{Node, Socket},
        project::RtProjectData,
    },
};

// =========================================================================
// 1. FILTER TYPE ENUM & BIQUAD ENGINE
// =========================================================================

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FilterType {
    LowShelf,
    Peaking,
    HighShelf,
    LowPass,
    HighPass,
}

#[derive(Clone, Debug)]
pub struct BiquadFilter {
    channels: usize,
    filter_type: FilterType,
    // Coefficients normalized by a0
    b0: f32,
    b1: f32,
    b2: f32,
    a1: f32,
    a2: f32,
}

pub struct BiquadFilterState {
    // Delay lines (history buffers)
    s1: f32,
    s2: f32,
}

impl BiquadFilter {
    pub const BUTTERWORTH_Q: f32 = 0.707;

    #[must_use]
    pub fn new(
        channels: u16,
        filter_type: FilterType,
        sample_rate: u32,
        freq: f32,
        q: f32,
        db_gain: f32,
    ) -> Self {
        let mut f = Self {
            channels: channels as usize,
            filter_type,
            b0: 1.0,
            b1: 0.0,
            b2: 0.0,
            a1: 0.0,
            a2: 0.0,
        };
        f.cook_coefficients(sample_rate, freq, q, db_gain);
        f
    }

    /// Computes coefficients based on the Audio EQ Cookbook
    pub fn cook_coefficients(&mut self, sample_rate: u32, frequency: f32, q: f32, db_gain: f32) {
        let a = 10.0f32.powf(db_gain / 40.0);
        let omega = 2.0 * PI * frequency / sample_rate as f32;
        let alpha = omega.sin() / (2.0 * q);
        let cos_omega = omega.cos();
        // let beta = (a.powf(2.0) + 1.0).sqrt();

        let (raw_b0, raw_b1, raw_b2, raw_a0, raw_a1, raw_a2) = match self.filter_type {
            FilterType::Peaking => (
                1.0 + alpha * a,
                -2.0 * cos_omega,
                1.0 - alpha * a,
                1.0 + alpha / a,
                -2.0 * cos_omega,
                1.0 - alpha / a,
            ),
            FilterType::LowShelf => (
                a * ((a + 1.0) - (a - 1.0) * cos_omega + 2.0 * a.sqrt() * alpha),
                2.0 * a * ((a - 1.0) - (a + 1.0) * cos_omega),
                a * ((a + 1.0) - (a - 1.0) * cos_omega - 2.0 * a.sqrt() * alpha),
                (a + 1.0) + (a - 1.0) * cos_omega + 2.0 * a.sqrt() * alpha,
                -2.0 * ((a - 1.0) + (a + 1.0) * cos_omega),
                (a + 1.0) + (a - 1.0) * cos_omega - 2.0 * a.sqrt() * alpha,
            ),
            FilterType::HighShelf => (
                a * ((a + 1.0) + (a - 1.0) * cos_omega + 2.0 * a.sqrt() * alpha),
                -2.0 * a * ((a - 1.0) + (a + 1.0) * cos_omega),
                a * ((a + 1.0) + (a - 1.0) * cos_omega - 2.0 * a.sqrt() * alpha),
                (a + 1.0) - (a - 1.0) * cos_omega + 2.0 * a.sqrt() * alpha,
                2.0 * ((a - 1.0) - (a + 1.0) * cos_omega),
                (a + 1.0) - (a - 1.0) * cos_omega - 2.0 * a.sqrt() * alpha,
            ),
            FilterType::LowPass => {
                let b = (1.0 - cos_omega) * 0.5;
                (b, b * 2.0, b, 1.0 + alpha, -2.0 * cos_omega, 1.0 - alpha)
            }
            FilterType::HighPass => {
                let b = f32::midpoint(1.0, cos_omega);
                (b, -b * 2.0, b, 1.0 + alpha, -2.0 * cos_omega, 1.0 - alpha)
            }
        };

        // Pre-divide to eliminate division overhead in real-time processing loop
        let a0_inv = 1.0 / raw_a0;
        self.b0 = raw_b0 * a0_inv;
        self.b1 = raw_b1 * a0_inv;
        self.b2 = raw_b2 * a0_inv;
        self.a1 = raw_a1 * a0_inv;
        self.a2 = raw_a2 * a0_inv;
    }
}

impl Node for BiquadFilter {
    type State = BiquadFilterState;

    fn init_state(&self) -> Self::State {
        BiquadFilterState { s1: 0.0, s2: 0.0 }
    }

    fn process(
        &self,
        pool: &mut crate::engine::bbp::PoolExecutor,
        state: &mut Self::State,
        _: &RtProjectData,
        _: Tick,
        inputs: &[SlotIndex],
        outputs: &[SlotIndex],
    ) {
        let input_buf = pool.get_input(inputs[0]);
        let output_buf = pool.get_output(outputs[0]);

        for (frame, chunk) in input_buf.chunks(self.channels).enumerate() {
            let out_start = frame * self.channels;
            for (ch, &x) in chunk.iter().enumerate() {
                let y = (self.b0 * x) + state.s1;

                // Step 2: Update the s1 accumulator for the next sample pass
                state.s1 = (self.b1 * x) - (self.a1 * y) + state.s2;

                // Step 3: Update the s2 accumulator
                state.s2 = (self.b2 * x) - (self.a2 * y);

                output_buf[out_start + ch] = y;
            }
        }
    }

    fn spec_in(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "in", true)]
    }

    fn spec_out(&self) -> Vec<Socket> {
        vec![Socket::new(DataKind::Audio, "out", true)]
    }
}
