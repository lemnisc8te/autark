//! Constant definitions for the `Engine`.

/// Defines the maximum number of nodes in the graph.
pub const MAX_NODES: usize = 1024;

/// Defines the maximum capacity of the `rtrb::RingBuffer` used to communicate with the audio thread.
pub const UPDATE_RING_CAPACITY: usize = 32;

/// Defines the maximum capacity of the `rtrb::RingBuffer` used by the audio thread to send garbage to the main thread to be dropped.
pub const GARBAGE_RING_CAPACITY: usize = 64;

pub const DEFAULT_MANAGER_CAPACITY: usize = 64;

/// Defines the maximum amount of slots in the `BlockBufferPool` manipulated by the audio thread
pub const MAX_BUFFER_SLOTS: usize = 4096;
