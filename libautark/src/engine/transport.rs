use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone)]
#[repr(u8)]
pub enum TransportState {
    Stopped,
    Playing,
    Recording,
    Paused,
}

#[derive(Debug, Default)]
pub struct Transport(AtomicU8);

impl Transport {
    /// Creates a new [`Transport`].
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicU8::new(0))
    }

    #[inline]
    fn transport(&self, to: TransportState) {
        self.0.store(to as u8, Ordering::Relaxed);
    }

    pub fn play(&self) {
        self.transport(TransportState::Playing);
    }

    pub fn stop(&self) {
        self.transport(TransportState::Stopped);
    }

    #[inline]
    pub fn is_playing(&self) -> bool {
        self.0.load(Ordering::Relaxed) == TransportState::Playing as u8
    }

    pub fn replace(&self, other: Self) {
        self.0.swap(other.0.into_inner(), Ordering::Relaxed);
    }
}
