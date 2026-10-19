// Job types:
// - Scoped. Finish at user defined scope (sync point). only need allocations to last until the end
// of that scope so can use a bump allocator that is reset at the end of the scope
// - Unscoped. Continue until they are complete so require an allocation that lasts as long as the
// job does
// - IoSubmission Jobs that submit tasks to the OS.
// - IoCompletion jobs that finalize IO Submissions after the OS is done.

use bitfield_struct::bitfield;

#[derive(Debug)]
#[repr(u8)]
enum JobKind {
    Scoped,
    Unscoped,
    IoSubmission,
    IoCompletion,
}
impl JobKind {
    const fn into_bits(self) -> u8 {
        self as _
    }

    const fn from_bits(value: u8) -> Self {
        match value {
            0 => Self::Scoped,
            1 => Self::Unscoped,
            2 => Self::IoSubmission,
            3 => Self::IoCompletion,
            _ => unreachable!(),
        }
    }
}

// A work item is a slot in an arena plus a job kind
#[bitfield(u32)]
pub(crate) struct WorkItem {
    #[bits(28)]
    slot: u32,
    #[bits(4)]
    kind: JobKind,
}

struct JobHeader {
    /// We store the function as a raw function pointer that accepts raw bytes
    run: unsafe fn(*mut u8),
}

impl JobHeader {
    fn new<F: FnOnce()>(_: &F) -> Self {
        Self {
            run: Self::call::<F>,
        }
    }

    /// # Safety
    ///
    /// `p` must point to an initialized `F`, and must be called exactly once as a second call
    /// would read a moved value
    unsafe fn call<F: FnOnce()>(p: *mut u8) {
        let f = unsafe { p.cast::<F>().read() };
        f();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, mem::ManuallyDrop};

    // Runs `f` through a JobHeader the way a worker would, by its type-erased pointer
    fn run_erased<F: FnOnce()>(f: F) {
        let mut f = ManuallyDrop::new(f);
        let header = JobHeader::new(&*f);
        unsafe { (header.run)((&raw mut *f).cast()) };
    }

    #[test]
    fn run_calls_the_closure() {
        let ran = Cell::new(false);
        run_erased(|| ran.set(true));
        assert!(ran.get());
    }

    #[test]
    fn run_passes_captured_values_to_the_closure() {
        let sum = Cell::new(0);
        let values = vec![1, 2, 3];
        run_erased(|| sum.set(values.into_iter().sum()));
        assert_eq!(sum.get(), 6);
    }
}
