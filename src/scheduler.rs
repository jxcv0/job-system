use std::{cell::RefCell, marker::PhantomData, sync::Arc, thread::available_parallelism};

use crate::{
    arena::ScopeArena,
    jobs::{JobKind, WorkItem},
    queue::{self, Owner, Stealer},
    sync::{AtomicU32, Ordering},
};

thread_local! {
    static LOCAL: RefCell<Option<Owner>> = const { RefCell::new(None) };
}

fn worker_loop(stealers: Arc<[Stealer]>) {
    loop {
        todo!()
    }
}

struct Scope<'s, 'env: 's> {
    /// The bump arena used for allocating jobs in the scope and its nested child scopes
    arena: &'s ScopeArena,
    /// Counts up on spawning of new tasks and down to track the progress of the scope, work is complete when pending == 0
    pending: AtomicU32,
    /// Stops the compiler shortening `'s`, so a job can't borrow a local that dies before the end of
    /// the scope
    _invariant: PhantomData<&'s mut &'s ()>,
    /// Data from outside the scope that jobs may borrow
    _env: PhantomData<&'env mut &'env ()>,
}
impl<'s, 'env> Scope<'s, 'env> {
    fn new(arena: &'s ScopeArena) -> Self {
        Self {
            arena,
            pending: AtomicU32::new(0),
            _invariant: Default::default(),
            _env: Default::default(),
        }
    }

    fn spawn<F>(&'s self, f: F)
    where
        F: FnOnce(&'s Scope<'s, 'env>) + Send + 's,
    {
        self.pending.fetch_add(1, Ordering::Relaxed);
        let slot = self
            .arena
            .alloc_job(move || {
                f(self);
                self.pending.fetch_sub(1, Ordering::Release);
            })
            .expect("arena out of memory");
        let item = WorkItem::new().with_slot(slot).with_kind(JobKind::Scoped);
        LOCAL.with(|l| {
            l.borrow()
                .as_ref()
                .expect("spawn called off a worker")
                .push(item)
        })
    }
}

struct Scheduler {
    // threads: Vec<JoinHandle<()>>,
    stealers: Arc<[Stealer]>,
    arena: ScopeArena,
    // Only one thread may own the Scheduler
    _not_send: PhantomData<*const ()>,
}
impl Scheduler {
    pub fn new() -> Self {
        let np = available_parallelism().unwrap().get();

        let (owners, stealers): (Vec<_>, Vec<_>) = (0..np).map(|_| queue::channel()).unzip();
        let mut owners = owners.into_iter();

        // store the main thread's Owner in TLS
        LOCAL.with(|l| {
            assert!(l.borrow().is_none(), "This thread already has a scheduler");
            *l.borrow_mut() = owners.next()
        });

        Self {
            stealers: stealers.into(),
            arena: ScopeArena::new(4096),
            _not_send: Default::default(),
        }
    }

    pub fn scope<'env, F>(&'env mut self, f: F)
    where
        F: for<'s> FnOnce(&'s Scope<'s, 'env>),
    {
        let scope = Scope::new(&self.arena);
        f(&scope);

        while scope.pending.load(Ordering::Acquire) > 0 {
            let item = LOCAL.with(|l| l.borrow().as_ref().unwrap().pop());
            match item {
                // SAFETY: every queued item came from alloc_job, and a popped item is never
                // queued again, so each job runs exactly once
                Some(item) => unsafe {
                    self.arena.run_job(item.slot());
                },
                None => std::hint::spin_loop(),
            }
        }
    }
}
impl Drop for Scheduler {
    fn drop(&mut self) {
        LOCAL.with(|l| *l.borrow_mut() = None);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn scope_with_no_jobs_returns() {
        let mut scheduler = Scheduler::new();
        scheduler.scope(|_| {});
    }

    #[test]
    fn scope_runs_every_spawned_job_before_returning() {
        let mut scheduler = Scheduler::new();
        let ran = AtomicUsize::new(0);
        scheduler.scope(|s| {
            for _ in 0..3 {
                s.spawn(|_| {
                    ran.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
        assert_eq!(ran.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn jobs_can_spawn_children_into_the_same_scope() {
        let mut scheduler = Scheduler::new();
        let ran = AtomicUsize::new(0);
        scheduler.scope(|s| {
            s.spawn(|s| {
                ran.fetch_add(1, Ordering::Relaxed);
                for _ in 0..2 {
                    s.spawn(|_| {
                        ran.fetch_add(1, Ordering::Relaxed);
                    });
                }
            });
        });
        assert_eq!(ran.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn scheduler_can_run_consecutive_scopes() {
        let mut scheduler = Scheduler::new();
        let ran = AtomicUsize::new(0);
        for _ in 0..2 {
            scheduler.scope(|s| {
                s.spawn(|_| {
                    ran.fetch_add(1, Ordering::Relaxed);
                });
            });
        }
        assert_eq!(ran.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn thread_can_create_a_new_scheduler_after_dropping_one() {
        drop(Scheduler::new());
        let mut scheduler = Scheduler::new();
        scheduler.scope(|_| {});
    }
}
