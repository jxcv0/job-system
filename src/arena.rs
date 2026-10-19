use std::mem::MaybeUninit;

use crate::{
    CACHE_LINE,
    jobs::{JobHeader, MAX_SLOTS},
    sync::{AtomicU32, Ordering, UnsafeCell},
};

const fn closure_offset<F>() -> usize {
    size_of::<JobHeader>().next_multiple_of(align_of::<F>())
}

const fn lines_for<F>() -> u32 {
    (closure_offset::<F>() + size_of::<F>()).div_ceil(CACHE_LINE) as u32
}

#[repr(align(128))]
struct Line(UnsafeCell<MaybeUninit<[u8; CACHE_LINE]>>);
// loom's UnsafeCell carries tracking state, so only the real cell is exactly one line
#[cfg(not(loom))]
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
        assert!(capacity <= MAX_SLOTS, "capacity exceeds WorkItem slot bits");
        #[cfg(not(loom))]
        let lines = {
            let lines = Box::new_uninit_slice(capacity as usize);
            // SAFETY: a Line is only MaybeUninit bytes, so uninitialised memory is a valid Line
            unsafe { lines.assume_init() }
        };
        // loom's UnsafeCell has tracking state that must be constructed, not left uninitialised
        #[cfg(loom)]
        let lines = (0..capacity)
            .map(|_| Line(UnsafeCell::new(MaybeUninit::uninit())))
            .collect();
        Self {
            lines,
            cursor: AtomicU32::new(0),
        }
    }

    /// Allocate n cache lines
    fn alloc(&self, n: u32) -> Result<u32, ScopeArenaError> {
        let start = self.cursor.fetch_add(n, Ordering::Relaxed);
        match start.checked_add(n) {
            Some(end) if end as usize <= self.lines.len() => Ok(start),
            // TODO: Consider resizing in future
            _ => Err(ScopeArenaError::OutOfMemory),
        }
    }

    /// Calls `f` with a pointer to the first byte of line `index`
    fn with_line_mut<R>(&self, index: u32, f: impl FnOnce(*mut u8) -> R) -> R {
        self.lines[index as usize].0.with_mut(|p| f(p.cast()))
    }
}

// SAFETY: alloc hands each line range to exactly one caller, so lines are never written concurrently
unsafe impl Sync for ScopeArena {}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::{sync::Arc, thread};

    #[test]
    #[should_panic(expected = "Causality violation")]
    fn loom_sees_unsynchronised_writes_to_a_line() {
        loom::model(|| {
            let arena = Arc::new(ScopeArena::new(1));
            let other = arena.clone();
            let writer = thread::spawn(move || other.with_line_mut(0, |p| unsafe { p.write(1) }));
            arena.with_line_mut(0, |p| unsafe { p.write(2) });
            writer.join().unwrap();
        });
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
