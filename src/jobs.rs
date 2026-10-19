// Job types:
// - Scoped. Finish at user defined scope (sync point). only need allocations to last until the end
// of that scope so can use a bump allocator that is reset at the end of the scope
// - Unscoped. Continue until they are complete so require an allocation that lasts as long as the
// job does
// - IoSubmission Jobs that submit tasks to the OS.
// - IoCompletion jobs that finalize IO Submissions after the OS is done.

use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
pub(crate) enum JobError {
    UnknownType { unknown_value: u32 },
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum JobType {
    Scoped = 0,
    Unscoped = 1,
    IoSubmission = 2,
    IoCompletion = 3,
    Max,
}
impl TryFrom<u32> for JobType {
    type Error = JobError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value < JobType::Max as u32 {
            let table = [
                Self::Scoped,
                Self::Unscoped,
                Self::IoSubmission,
                Self::IoCompletion,
            ];
            Ok(table[value as usize])
        } else {
            Err(JobError::UnknownType {
                unknown_value: value,
            })
        }
    }
}

struct ScopeGroup {
    completion_target: AtomicUsize,
}

// scheduler.par_for() {
// }

// A work item is a reference to an allocation that contains the actual Job
// The top 4 bits are used to discriminate between job types
pub(crate) struct WorkItem(u32);
impl WorkItem {
    fn job_type(&self) -> JobType {
        JobType::try_from(self.0 >> 28).expect("invalid job type")
    }

    // Unchecked conversion to underlying type
    pub(crate) fn into_raw(self) -> u32 {
        self.0
    }

    // Unchecked conversion from underlying type
    pub(crate) fn from_raw(raw: u32) -> Self {
        Self(raw)
    }
}
impl TryFrom<u32> for WorkItem {
    type Error = JobError;
    fn try_from(value: u32) -> Result<Self, JobError> {
        let _ = JobType::try_from(value >> 28)?;
        Ok(Self(value))
    }
}
