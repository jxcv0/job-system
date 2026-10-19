use std::{cell::Cell, marker::PhantomData, ops::Deref};

use crate::{
    jobs::WorkItem,
    sync::{Arc, AtomicIsize, AtomicU32, Ordering, fence},
};

use crate::CACHE_LINE;

#[repr(align(128))]
struct CacheAligned<T>(T);
impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
// This is so we don't forget to change the align of [CacheAligned] in case we change the constant [CACHE_LINE]
const _: () = assert!(align_of::<CacheAligned<u8>>() == CACHE_LINE);

/// The maximum number of tasks that can be enqueued in a [WorkQueue]
#[cfg(not(loom))]
const MAX_TASKS_PER_WORKER: usize = 4096;
// loom tracks every atomic it models, so keep the buffer tiny under it.
#[cfg(loom)]
const MAX_TASKS_PER_WORKER: usize = 4;
const _: () = assert!(MAX_TASKS_PER_WORKER.is_power_of_two());

fn mask(index: usize) -> usize {
    index & (MAX_TASKS_PER_WORKER - 1)
}

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

        let index = mask(b as usize);
        let slot = &self.slots[index];
        let item = WorkItem::from_bits(slot.load(Ordering::Relaxed));

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

        // Check if queue is full
        // TODO: This is where we would decide to resize when we eventually implement that
        debug_assert!(
            b - self.top.load(Ordering::Relaxed) < MAX_TASKS_PER_WORKER as isize,
            "work queue full"
        );

        self.slots[mask(b as usize)].store(item.into_bits(), Ordering::Relaxed);
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

        let item = self.slots[mask(t as usize)].load(Ordering::Relaxed);
        self.top
            .compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed)
            .ok()?;
        Some(WorkItem::from_bits(item))
    }
}

// Create a workqueue and return its channels
pub(crate) fn channel() -> (Owner, Stealer) {
    let top = CacheAligned(AtomicIsize::new(0));
    let bottom = CacheAligned(AtomicIsize::new(0));
    let slots: Box<_> = (0..MAX_TASKS_PER_WORKER)
        .map(|_| AtomicU32::new(0))
        .collect();
    let queue = Arc::new(WorkQueue { top, bottom, slots });
    (Owner::new(queue.clone()), Stealer::new(queue))
}

pub(crate) struct Owner {
    inner: Arc<WorkQueue>,
    // This is how the rustonomicon recommends disabling Sync but not Send
    // TODO: Change this once `impl !Sync` is available
    _not_sync: PhantomData<Cell<()>>,
}
impl Owner {
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
pub(crate) struct Stealer {
    inner: Arc<WorkQueue>,
}
impl Stealer {
    fn new(inner: Arc<WorkQueue>) -> Self {
        Self { inner }
    }

    fn steal(&self) -> Option<WorkItem> {
        self.inner.steal_from_top()
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    fn steal_all(stealer: &Stealer) -> Vec<u32> {
        std::iter::from_fn(|| stealer.steal().map(WorkItem::into_bits)).collect()
    }

    fn assert_each_once(mut seen: Vec<u32>, expected: impl IntoIterator<Item = u32>) {
        seen.sort_unstable();
        assert_eq!(seen, expected.into_iter().collect::<Vec<_>>());
    }

    // The owner and a thief race for the only item: exactly one of them gets it.
    #[test]
    fn loom_pop_and_steal_race_for_the_last_item() {
        loom::model(|| {
            let (owner, stealer) = channel();
            owner.push(WorkItem::from_bits(7));

            let thief = thread::spawn(move || stealer.steal().map(WorkItem::into_bits));
            let popped = owner.pop().map(WorkItem::into_bits);
            let stolen = thief.join().unwrap();

            assert_each_once(popped.into_iter().chain(stolen).collect(), [7]);
        });
    }

    // A thief steals while the owner is still pushing, then the owner drains the rest.
    #[test]
    fn loom_steal_during_push_delivers_each_item_once() {
        loom::model(|| {
            let (owner, stealer) = channel();

            let thief = thread::spawn(move || steal_all(&stealer));
            owner.push(WorkItem::from_bits(0));
            owner.push(WorkItem::from_bits(1));
            let mut seen: Vec<u32> =
                std::iter::from_fn(|| owner.pop().map(WorkItem::into_bits)).collect();
            seen.extend(thief.join().unwrap());

            assert_each_once(seen, 0..2);
        });
    }

    // Two thieves and the owner all compete for two items.
    #[test]
    fn loom_two_thieves_and_owner_deliver_each_item_once() {
        loom::model(|| {
            let (owner, stealer) = channel();
            owner.push(WorkItem::from_bits(0));
            owner.push(WorkItem::from_bits(1));

            let thieves: Vec<_> = (0..2)
                .map(|_| {
                    let stealer = stealer.clone();
                    thread::spawn(move || stealer.steal().map(WorkItem::into_bits))
                })
                .collect();
            let mut seen: Vec<u32> = owner.pop().map(WorkItem::into_bits).into_iter().collect();
            for thief in thieves {
                seen.extend(thief.join().unwrap());
            }
            seen.extend(std::iter::from_fn(|| owner.pop().map(WorkItem::into_bits)));

            assert_each_once(seen, 0..2);
        });
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn pop_all(owner: &Owner) -> Vec<u32> {
        std::iter::from_fn(|| owner.pop().map(WorkItem::into_bits)).collect()
    }

    fn steal_all(stealer: &Stealer) -> Vec<u32> {
        std::iter::from_fn(|| stealer.steal().map(WorkItem::into_bits)).collect()
    }

    #[test]
    fn empty_queue_yields_nothing() {
        let (owner, stealer) = channel();
        assert!(owner.pop().is_none());
        assert!(stealer.steal().is_none());
    }

    #[test]
    fn owner_pops_newest_first() {
        let (owner, _stealer) = channel();
        for i in 0..4 {
            owner.push(WorkItem::from_bits(i));
        }
        assert_eq!(pop_all(&owner), [3, 2, 1, 0]);
    }

    #[test]
    fn stealer_takes_oldest_first() {
        let (owner, stealer) = channel();
        for i in 0..4 {
            owner.push(WorkItem::from_bits(i));
        }
        assert_eq!(steal_all(&stealer), [0, 1, 2, 3]);
    }

    #[test]
    fn pop_and_steal_take_from_opposite_ends() {
        let (owner, stealer) = channel();
        for i in 0..3 {
            owner.push(WorkItem::from_bits(i));
        }
        assert_eq!(stealer.steal().map(WorkItem::into_bits), Some(0));
        assert_eq!(owner.pop().map(WorkItem::into_bits), Some(2));
        assert_eq!(owner.pop().map(WorkItem::into_bits), Some(1));
        assert!(stealer.steal().is_none());
        assert!(owner.pop().is_none());
    }

    #[test]
    fn indices_wrap_around_the_buffer() {
        let (owner, stealer) = channel();
        let capacity = MAX_TASKS_PER_WORKER as u32;
        for round in 0..3 {
            let items = round * capacity..(round + 1) * capacity;
            for i in items.clone() {
                owner.push(WorkItem::from_bits(i));
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

        let (owner, stealer) = channel();
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
                    owner.push(WorkItem::from_bits(i));
                }
                popped.extend(pop_all(&owner));
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
