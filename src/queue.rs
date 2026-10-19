use std::{
    cell::Cell,
    marker::PhantomData,
    ops::Deref,
    thread::{JoinHandle, available_parallelism},
};

use crate::sync::{Arc, AtomicIsize, AtomicU32, Ordering, fence};

// Job types:
// - Scoped. Finish at user defined scope (sync point). only need allocations to last until the end
// of that scope so can use a bump allocator that is reset at the end of the scope
// - Unscoped. Continue until they are complete so require an allocation that lasts as long as the
// job does
// - IoSubmission Jobs that submit tasks to the kernel.
// - IoCompletion jobs that finalize IO Submissions after the kernel is done.

#[repr(align(64))]
struct CacheAligned<T>(T);
impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The maximum number of tasks that can be enqueued in a [WorkQueue]
#[cfg(not(loom))]
const MAX_TASKS_PER_WORKER: usize = 4096;
// loom tracks every atomic it models, so keep the buffer tiny under it.
#[cfg(loom)]
const MAX_TASKS_PER_WORKER: usize = 4;
const _: () = assert!(MAX_TASKS_PER_WORKER.is_power_of_two());

pub(crate) struct WorkItem(u32);

// Chase-Lev circular work-stealing queue
// Work items are indexes into the buffer that stores the actual allocation for the function and
// its arguments.
// The owner pushes and pops at the bottom (LIFO). Stealers take from the top (FIFO).
// TODO: Add resizing by the owner of the queue
struct WorkQueue {
    top: CacheAligned<AtomicIsize>,
    bottom: CacheAligned<AtomicIsize>,
    slots: Box<[AtomicU32]>,
}
impl WorkQueue {
    fn new() -> Self {
        let top = CacheAligned(AtomicIsize::new(0));
        let bottom = CacheAligned(AtomicIsize::new(0));
        let slots: Box<_> = (0..MAX_TASKS_PER_WORKER)
            .map(|_| AtomicU32::new(0))
            .collect();
        Self { top, bottom, slots }
    }

    // Split into a channel for pushing and popping tasks and a stealer that can be used by non-owners to steal
    // tasks from this queue
    fn split(self) -> (WorkChannel, StealReceiver) {
        let inner = Arc::new(self);
        (WorkChannel::new(inner.clone()), StealReceiver::new(inner))
    }

    fn mask(index: usize) -> usize {
        index & (MAX_TASKS_PER_WORKER - 1)
    }

    fn pop_from_bottom(&self) -> Option<WorkItem> {
        let b = self.bottom.load(Ordering::Relaxed) - 1;
        self.bottom.store(b, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let t = self.top.load(Ordering::Relaxed);

        // Check if queue is empty
        if t > b {
            self.bottom.store(b + 1, Ordering::Relaxed);
            return None;
        }

        let index = Self::mask(b as usize);
        let slot = &self.slots[index];
        let item = WorkItem(slot.load(Ordering::Relaxed));

        if t < b {
            Some(item)
        } else {
            debug_assert!(t == b); // sanity
            // this is the last item so we are racing with thieves
            let won = self
                .top
                .compare_exchange(b, b + 1, Ordering::SeqCst, Ordering::Relaxed);
            self.bottom.store(b + 1, Ordering::Relaxed);
            won.is_ok().then_some(item)
        }
    }

    fn push_to_bottom(&self, item: WorkItem) {
        let b = self.bottom.load(Ordering::Relaxed);
        self.slots[Self::mask(b as usize)].store(item.0, Ordering::Relaxed);
        self.bottom.store(b + 1, Ordering::Release);
    }

    fn steal_from_top(&self) -> Option<WorkItem> {
        let t = self.top.load(Ordering::Acquire);
        fence(Ordering::SeqCst);
        let b = self.bottom.load(Ordering::Acquire);

        // Check if queue is empty
        if t >= b {
            return None;
        }

        let item = self.slots[Self::mask(t as usize)].load(Ordering::Relaxed);
        self.top
            .compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed)
            .ok()?;
        Some(WorkItem(item))
    }
}

pub(crate) struct WorkChannel {
    inner: Arc<WorkQueue>,
    // This is how the rustonomicon recommends disabling Sync but not Send
    // TODO: Change this once `impl !Sync` is available
    _not_sync: PhantomData<Cell<()>>,
}
impl WorkChannel {
    fn new(inner: Arc<WorkQueue>) -> Self {
        Self {
            inner,
            _not_sync: Default::default(),
        }
    }
    fn push(&self, item: WorkItem) {
        self.inner.push_to_bottom(item);
    }

    fn pop(&self) -> Option<WorkItem> {
        self.inner.pop_from_bottom()
    }
}

#[derive(Clone)]
pub(crate) struct StealReceiver {
    inner: Arc<WorkQueue>,
}
impl StealReceiver {
    fn new(inner: Arc<WorkQueue>) -> Self {
        Self { inner }
    }

    fn steal(&self) -> Option<WorkItem> {
        self.inner.steal_from_top()
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

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    fn steal_all(stealer: &StealReceiver) -> Vec<u32> {
        std::iter::from_fn(|| stealer.steal().map(|item| item.0)).collect()
    }

    fn assert_each_once(mut seen: Vec<u32>, expected: impl IntoIterator<Item = u32>) {
        seen.sort_unstable();
        assert_eq!(seen, expected.into_iter().collect::<Vec<_>>());
    }

    // The owner and a thief race for the only item: exactly one of them gets it.
    #[test]
    fn loom_pop_and_steal_race_for_the_last_item() {
        loom::model(|| {
            let (channel, stealer) = WorkQueue::new().split();
            channel.push(WorkItem(7));

            let thief = thread::spawn(move || stealer.steal().map(|item| item.0));
            let popped = channel.pop().map(|item| item.0);
            let stolen = thief.join().unwrap();

            assert_each_once(popped.into_iter().chain(stolen).collect(), [7]);
        });
    }

    // A thief steals while the owner is still pushing, then the owner drains the rest.
    #[test]
    fn loom_steal_during_push_delivers_each_item_once() {
        loom::model(|| {
            let (channel, stealer) = WorkQueue::new().split();

            let thief = thread::spawn(move || steal_all(&stealer));
            channel.push(WorkItem(0));
            channel.push(WorkItem(1));
            let mut seen: Vec<u32> =
                std::iter::from_fn(|| channel.pop().map(|item| item.0)).collect();
            seen.extend(thief.join().unwrap());

            assert_each_once(seen, 0..2);
        });
    }

    // Two thieves and the owner all compete for two items.
    #[test]
    fn loom_two_thieves_and_owner_deliver_each_item_once() {
        loom::model(|| {
            let (channel, stealer) = WorkQueue::new().split();
            channel.push(WorkItem(0));
            channel.push(WorkItem(1));

            let thieves: Vec<_> = (0..2)
                .map(|_| {
                    let stealer = stealer.clone();
                    thread::spawn(move || stealer.steal().map(|item| item.0))
                })
                .collect();
            let mut seen: Vec<u32> = channel.pop().map(|item| item.0).into_iter().collect();
            for thief in thieves {
                seen.extend(thief.join().unwrap());
            }
            seen.extend(std::iter::from_fn(|| channel.pop().map(|item| item.0)));

            assert_each_once(seen, 0..2);
        });
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn new_channel() -> (WorkChannel, StealReceiver) {
        WorkQueue::new().split()
    }

    fn pop_all(channel: &WorkChannel) -> Vec<u32> {
        std::iter::from_fn(|| channel.pop().map(|item| item.0)).collect()
    }

    fn steal_all(stealer: &StealReceiver) -> Vec<u32> {
        std::iter::from_fn(|| stealer.steal().map(|item| item.0)).collect()
    }

    #[test]
    fn empty_queue_yields_nothing() {
        let (channel, stealer) = new_channel();
        assert!(channel.pop().is_none());
        assert!(stealer.steal().is_none());
    }

    #[test]
    fn owner_pops_newest_first() {
        let (channel, _stealer) = new_channel();
        for i in 0..4 {
            channel.push(WorkItem(i));
        }
        assert_eq!(pop_all(&channel), [3, 2, 1, 0]);
    }

    #[test]
    fn stealer_takes_oldest_first() {
        let (channel, stealer) = new_channel();
        for i in 0..4 {
            channel.push(WorkItem(i));
        }
        assert_eq!(steal_all(&stealer), [0, 1, 2, 3]);
    }

    #[test]
    fn pop_and_steal_take_from_opposite_ends() {
        let (channel, stealer) = new_channel();
        for i in 0..3 {
            channel.push(WorkItem(i));
        }
        assert_eq!(stealer.steal().map(|item| item.0), Some(0));
        assert_eq!(channel.pop().map(|item| item.0), Some(2));
        assert_eq!(channel.pop().map(|item| item.0), Some(1));
        assert!(stealer.steal().is_none());
        assert!(channel.pop().is_none());
    }

    #[test]
    fn indices_wrap_around_the_buffer() {
        let (channel, stealer) = new_channel();
        let capacity = MAX_TASKS_PER_WORKER as u32;
        for round in 0..3 {
            let items = round * capacity..(round + 1) * capacity;
            for i in items.clone() {
                channel.push(WorkItem(i));
            }
            assert_eq!(steal_all(&stealer), items.collect::<Vec<_>>());
        }
    }

    // The owner pushes in batches no larger than the capacity and drains each batch itself, while
    // thieves steal concurrently. Every item must come out exactly once.
    #[test]
    fn concurrent_pop_and_steal_deliver_each_item_once() {
        const BATCH: u32 = 1024;
        const TOTAL: u32 = 200 * BATCH;
        const THIEVES: usize = 4;

        let (channel, stealer) = new_channel();
        let done = AtomicBool::new(false);

        let mut seen = std::thread::scope(|scope| {
            let thieves: Vec<_> = (0..THIEVES)
                .map(|_| {
                    let stealer = stealer.clone();
                    let done = &done;
                    scope.spawn(move || {
                        let mut stolen = Vec::new();
                        while !done.load(Ordering::Acquire) {
                            stolen.extend(steal_all(&stealer));
                        }
                        stolen
                    })
                })
                .collect();

            let mut popped = Vec::new();
            for start in (0..TOTAL).step_by(BATCH as usize) {
                for i in start..start + BATCH {
                    channel.push(WorkItem(i));
                }
                popped.extend(pop_all(&channel));
            }
            done.store(true, Ordering::Release);

            for thief in thieves {
                popped.extend(thief.join().unwrap());
            }
            popped
        });

        seen.sort_unstable();
        assert_eq!(seen, (0..TOTAL).collect::<Vec<_>>());
    }
}
