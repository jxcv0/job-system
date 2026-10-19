use std::{cell::UnsafeCell, mem::MaybeUninit};

use crate::{CACHE_LINE, jobs::JobHeader, sync::AtomicU32};

const fn closure_offset<F>() -> usize {
    size_of::<JobHeader>().next_multiple_of(align_of::<F>())
}

const fn lines_for<F>() -> u32 {
    (closure_offset::<F>() + size_of::<F>()).div_ceil(CACHE_LINE) as u32
}

#[repr(align(128))]
struct Line(UnsafeCell<MaybeUninit<[u8; CACHE_LINE]>>);
const _: () = assert!(size_of::<Line>() == CACHE_LINE);

/// A bump allocator for use by a scope to allocate tasks to
pub(crate) struct ScopeArena {
    lines: Box<[Line]>,
    cursor: AtomicU32,
}

enum ScopeArenaError {
    OutOfMemory,
}

impl ScopeArena {
    fn new(capacity: u32) -> Self {
        todo!()
    }

    /// Allocate n cache lines
    fn alloc(&self, n: u32) -> Result<u32, ScopeArenaError> {
        todo!()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn first_allocation_starts_at_line_zero() {
        let arena = ScopeArena::new(4);
        assert!(matches!(arena.alloc(1), Ok(0)));
    }

    #[test]
    fn consecutive_allocations_do_not_overlap() {
        let arena = ScopeArena::new(8);
        assert!(matches!(arena.alloc(2), Ok(0)));
        assert!(matches!(arena.alloc(3), Ok(2)));
        assert!(matches!(arena.alloc(1), Ok(5)));
    }

    #[test]
    fn allocation_can_fill_the_arena_exactly() {
        let arena = ScopeArena::new(4);
        assert!(matches!(arena.alloc(4), Ok(0)));
    }

    #[test]
    fn allocation_past_capacity_is_out_of_memory() {
        let arena = ScopeArena::new(4);
        assert!(matches!(arena.alloc(3), Ok(0)));
        assert!(matches!(arena.alloc(2), Err(ScopeArenaError::OutOfMemory)));
    }

    #[test]
    fn allocation_larger_than_the_arena_is_out_of_memory() {
        let arena = ScopeArena::new(4);
        assert!(matches!(arena.alloc(5), Err(ScopeArenaError::OutOfMemory)));
    }
}
