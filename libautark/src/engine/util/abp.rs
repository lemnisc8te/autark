//! Home of the `BlockBufferPool`, a pool of memory created for the exclusive usage of the audio thread.
use crate::engine::SlotIndex;

/// Contiguous memory used during `execute_block` in the audio callback.
///
/// The length of [`memory`] depends on the maximum number of slots in the scheduler.  Each slot is [`block_size`]d.
///
/// Can be transformed into a [`PoolExecutor`] for access to slots via their index.
pub struct AudioBufferPool {
    /// Contiguous pre-allocated block: (`buffer_count` * `block_size`)
    memory: Vec<f32>,
    pub block_size: usize,
}

impl AudioBufferPool {
    #[must_use]
    pub fn new(buffer_count: usize, block_size: usize) -> Self {
        Self {
            memory: vec![0.0f32; buffer_count * block_size],
            block_size,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.memory.fill(0.0f32);
    }

    /// Creates an execution context that allows unsafe, arbitrary slot slicing
    /// while keeping the unsafe code safely isolated.
    #[inline]
    pub const fn executor(&mut self) -> PoolExecutor {
        PoolExecutor {
            ptr: self.memory.as_mut_ptr(),
            block_size: self.block_size,
            // Track length to avoid out-of-bounds memory access in debug mode
            total_len: self.memory.len(),
        }
    }
}

/// A short-lived handle for zero-allocation, arbitrary buffer slicing
///
/// # Safety
/// This is the most unsafe part of the [`Engine`]. However, it can be used safely provided the invariants are held:
///
/// ONLY 1 thread may access each slot in the `PoolExecutor` at a time. Currently, this is possible because the audio callback is single threaded, and therefore no cross-thread aliasing can happen.
///
/// #
pub struct PoolExecutor {
    ptr: *mut f32,
    pub block_size: usize,
    total_len: usize,
}

impl PoolExecutor {
    /// Get a read-only view of a slot.
    /// Safety: Assumes no other code is actively writing to this slot right now.
    #[inline]
    #[must_use]
    pub fn get_input<'a>(&self, slot: SlotIndex) -> &'a [f32] {
        // SAFETY: Assumes no other code is actively writing to this slot.
        // This should be the case unless you somehow alias the same `PoolExecutor` across threads
        unsafe {
            let offset = slot * self.block_size;
            debug_assert!(offset + self.block_size <= self.total_len);
            std::slice::from_raw_parts(self.ptr.add(offset), self.block_size)
        }
    }

    /// Get a mutable view of a slot.
    /// Safety: Assumes no other code is actively reading or writing to this slot right now.
    #[inline]
    pub fn get_output<'a>(&mut self, slot: SlotIndex) -> &'a mut [f32] {
        // SAFETY: Assumes no other code is actively writing to this slot.
        // This should be the case unless you somehow alias the same `PoolExecutor` across threads
        unsafe {
            let offset = slot * self.block_size;
            debug_assert!(offset + self.block_size <= self.total_len);
            std::slice::from_raw_parts_mut(self.ptr.add(offset), self.block_size)
        }
    }
}
