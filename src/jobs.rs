// Job types:
// - Scoped. Finish at user defined scope (sync point). only need allocations to last until the end
// of that scope so can use a bump allocator that is reset at the end of the scope
// - Unscoped. Continue until they are complete so require an allocation that lasts as long as the
// job does
// - IoSubmission Jobs that submit tasks to the OS.
// - IoCompletion jobs that finalize IO Submissions after the OS is done.

use std::sync::atomic::AtomicUsize;

use bitfield_struct::bitfield;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
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

struct ScopeGroup {
    completion_target: AtomicUsize,
}

// A work item is a slot in an arena plus a job kind
#[bitfield(u32)]
pub(crate) struct WorkItem {
    #[bits(20)]
    slot: u32,
    #[bits(8)]
    arena: u8,
    #[bits(4)]
    kind: JobKind,
}
