use crate::{jobs::{JobKind, WorkItem,}, sync::Mutex};

const MAX_SLOTS: usize = 64;

#[derive(Debug)]
pub enum UnscopedSlabError {
    MaxSlotsReached,
}

#[derive(Default)]
struct Inner {
    slots: Vec<Option<Box<dyn FnOnce() + Send>>>,
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
            slab.slots[slot as usize] = Some(Box::new(f));
            slot
        } else if slab.slots.len() < MAX_SLOTS {
            let slot = slab.slots.len() as u32;
            slab.slots.push(Some(Box::new(f)));
            slot
        } else {
            return Err(UnscopedSlabError::MaxSlotsReached);
        };

        Ok(WorkItem::new().with_slot(slot).with_kind(kind))
    }

    // Fetch work from the slab
    pub(crate) fn take(&self, slot: u32) -> Option<Box<dyn FnOnce() + Send>> {
        let mut slab = self.inner.lock().unwrap();
        let maybe_fun = slab.slots[slot as usize].take();
        if maybe_fun.is_some() {
            slab.free.push(slot);
        }
        maybe_fun
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
    fn allocate_recording(slab: &mut UnscopedSlab, ran: &Arc<AtomicUsize>, id: usize) -> WorkItem {
        let ran = ran.clone();
        slab.allocate(JobKind::Unscoped, move || ran.store(id, Ordering::Relaxed))
            .unwrap()
    }

    #[test]
    fn take_returns_the_job_stored_in_a_reused_slot() {
        let mut slab = UnscopedSlab::new();
        let ran = Arc::new(AtomicUsize::new(0));

        let first = allocate_recording(&mut slab, &ran, 1);
        slab.take(first.slot()).unwrap()();
        let second = allocate_recording(&mut slab, &ran, 2);

        assert_eq!(second.slot(), first.slot());
        slab.take(second.slot()).unwrap()();
        assert_eq!(ran.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn allocate_fails_when_every_slot_is_live() {
        let mut slab = UnscopedSlab::new();
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
        let mut slab = UnscopedSlab::new();
        let items: Vec<WorkItem> = (0..MAX_SLOTS)
            .map(|_| slab.allocate(JobKind::Unscoped, || {}).unwrap())
            .collect();

        let _ = slab.take(items[0].slot()).unwrap();

        assert!(slab.allocate(JobKind::Unscoped, || {}).is_ok());
    }

    #[test]
    fn take_on_an_empty_slot_returns_none() {
        let mut slab = UnscopedSlab::new();
        let item = slab.allocate(JobKind::Unscoped, || {}).unwrap();

        let _ = slab.take(item.slot()).unwrap();

        assert!(slab.take(item.slot()).is_none());
    }
}
