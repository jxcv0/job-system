use std::mem::MaybeUninit;

use crate::{
    CACHE_LINE,
    jobs::{JobHeader, MAX_SLOTS},
    sync::{AtomicU32, Ordering, UnsafeCell},
};

pub(crate) const fn closure_offset<F>() -> usize {
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

#[derive(Debug)]
pub(crate) enum ScopeArenaError {
    OutOfMemory,
}

impl ScopeArena {
    pub(crate) fn new(capacity: u32) -> Self {
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
    pub(crate) fn alloc(&self, n: u32) -> Result<u32, ScopeArenaError> {
        let start = self.cursor.fetch_add(n, Ordering::Relaxed);
        match start.checked_add(n) {
            Some(end) if end as usize <= self.lines.len() => Ok(start),
            // TODO: Consider resizing in future
            _ => Err(ScopeArenaError::OutOfMemory),
        }
    }

    pub(crate) fn alloc_job<F: FnOnce() + Send>(&self, f: F) -> Result<u32, ScopeArenaError> {
        let line = self.alloc(lines_for::<F>())?;
        assert!(align_of::<F>() <= CACHE_LINE);
        // SAFETY: alloc gave this call sole use of lines_for::<F>() lines from `line`, all in
        // bounds, so both writes fit and nothing else touches them. Lines are 128-aligned and
        // the assert above caps F's alignment, so the header and closure are aligned
        self.with_line_mut(line, |p| unsafe {
            // Write the header into the slot
            p.cast::<JobHeader>().write(JobHeader::new(&f));
            // Write the function into the offset
            p.add(closure_offset::<F>()).cast::<F>().write(f);
        });
        Ok(line)
    }

    /// Calls `f` with a pointer to the first byte of line `index`
    #[cfg(loom)]
    fn with_line_mut<R>(&self, index: u32, f: impl FnOnce(*mut u8) -> R) -> R {
        self.lines[index as usize].0.with_mut(|p| f(p.cast()))
    }

    /// Calls `f` with a pointer to the first byte of line `index`
    #[cfg(not(loom))]
    fn with_line_mut<R>(&self, index: u32, f: impl FnOnce(*mut u8) -> R) -> R {
        assert!((index as usize) < self.lines.len());
        // Derived from the whole slice so a job may run on into the following lines
        let p = self.lines.as_ptr().cast::<u8>().cast_mut();
        f(unsafe { p.add(index as usize * CACHE_LINE) })
    }

    /// Runs the job that `alloc_job` wrote at `line`
    ///
    /// # Safety
    ///
    /// `line` must have come from `alloc_job`, and each job must be run exactly once
    pub(crate) unsafe fn run_job(&self, line: u32) {
        self.with_line_mut(line, |p| unsafe {
            let header = p.cast::<JobHeader>().read();
            (header.run)(p)
        });
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn allocated_job_runs_its_closure() {
        let arena = ScopeArena::new(4);
        let ran = AtomicUsize::new(0);
        let line = arena.alloc_job(|| ran.store(1, Ordering::Relaxed)).unwrap();
        unsafe { arena.run_job(line) };
        assert_eq!(ran.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn allocated_job_receives_its_captured_values() {
        let arena = ScopeArena::new(4);
        let sum = AtomicUsize::new(0);
        let values = vec![1, 2, 3];
        let line = arena
            .alloc_job(|| sum.store(values.into_iter().sum(), Ordering::Relaxed))
            .unwrap();
        unsafe { arena.run_job(line) };
        assert_eq!(sum.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn job_spanning_several_lines_does_not_overlap_the_next_job() {
        let arena = ScopeArena::new(8);
        let big_sum = &AtomicUsize::new(0);
        let small_ran = AtomicUsize::new(0);
        let big = [1u8; 3 * CACHE_LINE];
        let big_line = arena
            .alloc_job(move || {
                let total = big.iter().map(|&b| b as usize).sum();
                big_sum.store(total, Ordering::Relaxed)
            })
            .unwrap();
        let small_line = arena
            .alloc_job(|| small_ran.store(1, Ordering::Relaxed))
            .unwrap();
        unsafe {
            arena.run_job(small_line);
            arena.run_job(big_line);
        }
        assert_eq!(small_ran.load(Ordering::Relaxed), 1);
        assert_eq!(big_sum.load(Ordering::Relaxed), 3 * CACHE_LINE);
    }

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
    fn reset_makes_the_whole_arena_available_again() {
        let arena = ScopeArena::new(4);
        assert!(matches!(arena.alloc(4), Ok(0)));
        // SAFETY: nothing was written to the lines, so no job is left unrun
        unsafe { arena.reset() };
        assert!(matches!(arena.alloc(4), Ok(0)));
    }

    #[test]
    fn allocation_larger_than_the_arena_is_out_of_memory() {
        let arena = ScopeArena::new(4);
        assert!(matches!(arena.alloc(5), Err(ScopeArenaError::OutOfMemory)));
    }
}
