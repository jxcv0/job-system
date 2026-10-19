// Synchronisation primitives, swapped for loom's model-checked versions under `--cfg loom`.

#[cfg(loom)]
pub(crate) use loom::sync::{
    Arc,
    atomic::{AtomicIsize, AtomicU32, Ordering, fence},
};

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;

#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc,
    atomic::{AtomicIsize, AtomicU32, Ordering, fence},
};

/// `std::cell::UnsafeCell` behind loom's closure-based API, so callers compile under both cfgs
#[cfg(not(loom))]
#[repr(transparent)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub(crate) const fn new(data: T) -> Self {
        Self(std::cell::UnsafeCell::new(data))
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
