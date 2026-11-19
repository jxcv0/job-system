use std::sync::Mutex;

use crate::jobs::{JobKind, WorkItem};

const MAX_SLOTS: usize = 64;

#[derive(Debug)]
pub enum UnscopedSlabError {
    MaxSlotsReached,
}

#[derive(Default)]
struct Slot {
    job: Option<Box<dyn FnOnce() + Send>>,
    /// Bumped each time the job is taken, so items for earlier jobs in this slot no longer match.
    /// Wraps, so compare only for equality
    generation: u16,
}

#[derive(Default)]
struct Inner {
    slots: Vec<Slot>,
    free: Vec<u32>,
}

#[derive(Default)]
pub(crate) struct UnscopedSlab {
    inner: Mutex<Inner>,
}

impl UnscopedSlab {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    // Allocate work in the slab and return the WorkItem so it can be fetched later from a queue
    pub(crate) fn allocate<F>(&self, kind: JobKind, f: F) -> Result<WorkItem, UnscopedSlabError>
    where
        F: FnOnce() + Send + 'static,
    {
        let mut slab = self.inner.lock().unwrap();
        let slot = if let Some(slot) = slab.free.pop() {
            slot
        } else if slab.slots.len() < MAX_SLOTS {
            slab.slots.push(Slot::default());
            (slab.slots.len() - 1) as u32
        } else {
            return Err(UnscopedSlabError::MaxSlotsReached);
        };

        let entry = &mut slab.slots[slot as usize];
        entry.job = Some(Box::new(f));
        Ok(WorkItem::new()
            .with_slot(slot)
            .with_generation(entry.generation)
            .with_kind(kind))
    }

    // Fetch work from the slab. Returns None if the slot is empty or now holds a later job
    pub(crate) fn take(&self, item: WorkItem) -> Option<Box<dyn FnOnce() + Send>> {
        let mut slab = self.inner.lock().unwrap();
        let entry = slab.slots.get_mut(item.slot() as usize)?;
        if entry.generation != item.generation() {
            return None;
        }
        let job = entry.job.take()?;
        entry.generation = entry.generation.wrapping_add(1);
        slab.free.push(item.slot());
        Some(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    // Allocate a job that records `id` in `ran` when it runs
    fn allocate_recording(slab: &UnscopedSlab, ran: &Arc<AtomicUsize>, id: usize) -> WorkItem {
        let ran = ran.clone();
        slab.allocate(JobKind::Unscoped, move || ran.store(id, Ordering::Relaxed))
            .unwrap()
    }

    #[test]
    fn take_returns_the_job_stored_in_a_reused_slot() {
        let slab = UnscopedSlab::new();
        let ran = Arc::new(AtomicUsize::new(0));

        let first = allocate_recording(&slab, &ran, 1);
        slab.take(first).unwrap()();
        let second = allocate_recording(&slab, &ran, 2);

        assert_eq!(second.slot(), first.slot());
        slab.take(second).unwrap()();
        assert_eq!(ran.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn allocate_fails_when_every_slot_is_live() {
        let slab = UnscopedSlab::new();
        for _ in 0..MAX_SLOTS {
            slab.allocate(JobKind::Unscoped, || {}).unwrap();
        }

        assert!(matches!(
            slab.allocate(JobKind::Unscoped, || {}),
            Err(UnscopedSlabError::MaxSlotsReached)
        ));
    }

    #[test]
    fn allocate_succeeds_at_capacity_once_a_slot_is_taken() {
        let slab = UnscopedSlab::new();
        let items: Vec<WorkItem> = (0..MAX_SLOTS)
            .map(|_| slab.allocate(JobKind::Unscoped, || {}).unwrap())
            .collect();

        let _ = slab.take(items[0]).unwrap();

        assert!(slab.allocate(JobKind::Unscoped, || {}).is_ok());
    }

    #[test]
    fn take_on_an_empty_slot_returns_none() {
        let slab = UnscopedSlab::new();
        let item = slab.allocate(JobKind::Unscoped, || {}).unwrap();

        let _ = slab.take(item).unwrap();

        assert!(slab.take(item).is_none());
    }

    #[test]
    fn take_with_a_stale_item_returns_none_after_the_slot_is_reused() {
        let slab = UnscopedSlab::new();
        let ran = Arc::new(AtomicUsize::new(0));

        let first = allocate_recording(&slab, &ran, 1);
        slab.take(first).unwrap()();
        let second = allocate_recording(&slab, &ran, 2);
        assert_eq!(second.slot(), first.slot());
        assert_ne!(second.generation(), first.generation());

        assert!(slab.take(first).is_none());
        slab.take(second).unwrap()();
        assert_eq!(ran.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn generation_wraps_without_breaking_take() {
        let slab = UnscopedSlab::new();
        let item = slab.allocate(JobKind::Unscoped, || {}).unwrap();
        slab.inner.lock().unwrap().slots[item.slot() as usize].generation = u16::MAX;
        let item = item.with_generation(u16::MAX);

        slab.take(item).unwrap()();
        let next = slab.allocate(JobKind::Unscoped, || {}).unwrap();

        assert_eq!(next.slot(), item.slot());
        assert_eq!(next.generation(), 0);
        assert!(slab.take(item).is_none());
        assert!(slab.take(next).is_some());
    }
}
