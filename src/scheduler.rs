use std::{
    marker::PhantomData,
    sync::{Arc, atomic::AtomicUsize},
    thread::{JoinHandle, available_parallelism},
};

use crate::{arena::ScopeArena, queue, queue::Stealer};

struct Scope<'s> {
    /// The bump arena used for allocating jobs in the scope and its nested child scopes
    arena: &'s ScopeArena,
    /// Counts up on spawning of new tasks and down to track the progress of the scope, work is complete when pending == 0
    pending: AtomicUsize,
    /// Stops the compiler shortening `'s`, so a job can't borrow a local that dies before the end of
    /// the scope
    _invariant: PhantomData<&'s mut &'s ()>,
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
