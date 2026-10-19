use std::{marker::PhantomData, sync::Arc, thread::available_parallelism};

use crate::{
    arena::ScopeArena,
    queue,
    queue::Stealer,
    sync::{AtomicU32, Ordering},
};

struct Scope<'s> {
    /// The bump arena used for allocating jobs in the scope and its nested child scopes
    arena: &'s ScopeArena,
    /// Counts up on spawning of new tasks and down to track the progress of the scope, work is complete when pending == 0
    pending: AtomicU32,
    /// Stops the compiler shortening `'s`, so a job can't borrow a local that dies before the end of
    /// the scope
    _invariant: PhantomData<&'s mut &'s ()>,
}
impl<'s> Scope<'s> {
    fn spawn<F>(&'s self, f: F)
    where
        F: FnOnce(&'s Scope<'s>) + Send + 's,
    {
        self.pending.fetch_add(1, Ordering::Relaxed);
        let slot = self
            .arena
            .alloc_job(move || {
                f(self);
                self.pending.fetch_sub(1, Ordering::Release);
            })
            .expect("arena out of memory");
    }
}

struct Scheduler {
    // threads: Vec<JoinHandle<()>>,
    stealers: Arc<[Stealer]>,
}
impl Scheduler {
    pub fn new() -> Self {
        let np = available_parallelism().unwrap().get();

        let (owners, stealers): (Vec<_>, Vec<_>) = (0..np).map(|_| queue::channel()).unzip();

        Self {
            stealers: stealers.into(),
        }
    }
}
