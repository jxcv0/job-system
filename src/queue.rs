use std::{
    cell::Cell,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicIsize, AtomicU32},
    },
    thread::{JoinHandle, available_parallelism},
};

// Job types:
// - Scoped. Finish at user defined scope (sync point). only need allocations to last until the end
// of that scope so can use a bump allocator that is reset at the end of the scope
// - Unscoped. Continue until they are complete so require an allocation that lasts as long as the
// job does
// - IoSubmission Jobs that submit tasks to the kernel.
// - IoCompletion jobs that finalize IO Submissions after the kernel is done.

#[repr(align(64))]
struct CacheAligned<T>(T);

/// The maximum number of tasks that can be enqueued in a [WorkQueue]
const MAX_TASKS_PER_WORKER: usize = 4096;
const _: () = assert!(MAX_TASKS_PER_WORKER.is_power_of_two());

pub(crate) struct WorkItem(u32);

// Chase-Lev circular work-stealing queue
// Work items are indexes into the buffer that stores the actual allocation for the function and
// its arguments.
// The owner pushes and pops at the bottom (LIFO). Stealers take from the top (FIFO).
// TODO: Add resizing by the owner of the queue
pub(crate) struct WorkQueue {
    top: CacheAligned<AtomicIsize>,
    bottom: CacheAligned<AtomicIsize>,
    slots: Box<[AtomicU32]>,
}
impl WorkQueue {
    pub(crate) fn new() -> Self {
        todo!()
    }

    // Split into a channel for pushing and popping tasks and a stealer that can be used by non-owners to steal
    // tasks from this queue
    pub(crate) fn split(self) -> (WorkChannel, StealReceiver) {
        todo!()
    }
}

pub(crate) struct WorkChannel {
    inner: Arc<WorkQueue>,
    // This is how the rustonomicon recommends disabling Sync but not Send
    // TODO: Change this once `impl !Sync` is available
    _not_sync: PhantomData<Cell<()>>,
}
impl WorkChannel {
    pub(crate) fn push(&self, item: WorkItem) {
        todo!()
    }

    pub(crate) fn pop(&self) -> Option<WorkItem> {
        todo!()
    }
}

#[derive(Clone)]
pub(crate) struct StealReceiver {
    inner: Arc<WorkQueue>,
}
impl StealReceiver {
    pub(crate) fn steal(&self) -> Option<WorkItem> {
        todo!()
    }
}

struct Scheduler {
    threads: Vec<JoinHandle<()>>,
    stealers: Arc<[StealReceiver]>,
}
impl Scheduler {
    pub fn new() -> Self {
        let np = available_parallelism().unwrap().get();

        let (workers, stealers): (Vec<_>, Vec<_>) =
            (0..np).map(|_| WorkQueue::new().split()).unzip();

        todo!()
    }
}
