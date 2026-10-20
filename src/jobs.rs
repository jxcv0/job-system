// Job types:
// - Scoped. Finish at user defined scope (sync point). only need allocations to last until the end
// of that scope so can use a bump allocator that is reset at the end of the scope
// - Unscoped. Continue until they are complete so require an allocation that lasts as long as the
// job does
// - IoSubmission Jobs that submit tasks to the OS.
// - IoCompletion jobs that finalize IO Submissions after the OS is done.

use bitfield_struct::bitfield;

use crate::arena::closure_offset;

#[derive(Debug)]
#[repr(u8)]
pub(crate) enum JobKind {
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
    pub(crate) slot: u32,
    #[bits(4)]
    pub(crate) kind: JobKind,
}

pub(crate) const MAX_SLOTS: u32 = 1 << WorkItem::SLOT_BITS;

pub(crate) struct JobHeader {
    /// We store the call as a raw function pointer that accepts the header as raw bytes, see [call]
    pub(crate) run: unsafe fn(*mut u8),
}

impl JobHeader {
    pub(crate) fn new<F: FnOnce()>(_: &F) -> Self {
        Self {
            run: Self::call::<F>,
        }
    }

    /// Set up the run function pointer so that it pulls the closure out of the memory after the
    /// header and calls the function
    ///
    /// # Safety
    ///
    /// `p` must point to an initialized `F`, and must be called exactly once as a second call
    /// would read a moved value
    unsafe fn call<F: FnOnce()>(header: *mut u8) {
        // Read the function from the offset past the header then call it
        let f = unsafe { header.add(closure_offset::<F>()).cast::<F>().read() };
        f();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, mem::ManuallyDrop};

    // A header followed by its closure, laid out as the arena lays out a job
    #[repr(C)]
    struct Job<F> {
        header: JobHeader,
        f: F,
    }

    // Runs `f` through a JobHeader the way a worker would, by the header's type-erased address
    fn run_erased<F: FnOnce()>(f: F) {
        assert_eq!(std::mem::offset_of!(Job<F>, f), closure_offset::<F>());
        let mut job = ManuallyDrop::new(Job {
            header: JobHeader::new(&f),
            f,
        });
        unsafe { (job.header.run)((&raw mut *job).cast()) };
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
