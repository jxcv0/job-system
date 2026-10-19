// Synchronisation primitives, swapped for loom's model-checked versions under `--cfg loom`.

#[cfg(loom)]
pub(crate) use loom::sync::{
    Arc,
    atomic::{AtomicIsize, AtomicU32, Ordering, fence},
};

#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc,
    atomic::{AtomicIsize, AtomicU32, Ordering, fence},
};
