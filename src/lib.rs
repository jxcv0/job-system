mod arena;
mod jobs;
mod queue;
mod scheduler;
mod unscoped_slab;

#[cfg(not(target_arch = "x86_64"))]
compile_error!("job-system assumes x86_64 cache geometry");

/// Two 64-byte lines: Intel's spatial prefetcher pulls adjacent pairs, so false sharing spans 128
pub(crate) const CACHE_LINE: usize = 64 * 2;

#[repr(align(128))]
pub(crate) struct CacheAligned<T>(T);
impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> CacheAligned<T> {
    pub(crate) fn new(t: T) -> Self {
        Self(t)
    }
}
// This is so we don't forget to change the align of [CacheAligned] in case we change the constant [CACHE_LINE]
const _: () = assert!(align_of::<CacheAligned<u8>>() == CACHE_LINE);

use std::ops::Deref;

pub use scheduler::{Scheduler, Scope};
pub use unscoped_slab::UnscopedSlabError;
