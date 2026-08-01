//! Module for the `Tick` primitive.

use serde::{Deserialize, Serialize};
/// Atomic unit of time within the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Tick(pub u64);

impl Tick {
    #[must_use]
    pub fn from_secs(secs: f64, sample_rate: u32) -> Self {
        Self((secs * f64::from(sample_rate)).round() as u64)
    }
    #[must_use]
    pub fn as_secs(self, sample_rate: u32) -> f64 {
        self.0 as f64 / f64::from(sample_rate)
    }
}

impl From<u64> for Tick {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<usize> for Tick {
    fn from(value: usize) -> Self {
        Self(value as u64)
    }
}

impl std::ops::Add for Tick {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Sub for Tick {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}
