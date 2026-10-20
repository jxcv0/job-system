use std::{
    cell::RefCell,
    marker::PhantomData,
    sync::Arc,
    thread::{JoinHandle, available_parallelism},
};

use crate::{
    arena::ScopeArena,
    jobs::{JobKind, WorkItem},
    queue::{self, Owner, Stealer},
    sync::{AtomicU32, Ordering},
};

thread_local! {
    static LOCAL: RefCell<Option<Owner>> = const { RefCell::new(None) };
}

/// Set the owner in TLS
fn set_owner(owner: Owner) {
    LOCAL.with(move |l| {
        assert!(l.borrow().is_none(), "This thread already has a scheduler");
        *l.borrow_mut() = Some(owner)
    });
}

/// Get the next work item from the owner in TLS
fn next_work_item() -> Option<WorkItem> {
    LOCAL.with(|l| l.borrow().as_ref().unwrap().pop())
}

/// Steal some work from another thread
fn steal_work_item(stealers: &[Stealer]) -> Option<WorkItem> {
    stealers.iter().find_map(Stealer::steal)
}

fn worker_loop(owner: Owner, stealers: Arc<[Stealer]>, arena: Arc<ScopeArena>) {
    set_owner(owner);

    loop {
        if let Some(item) = next_work_item() {
            run_work_item(item, &arena);
        } else if let Some(item) = steal_work_item(&stealers) {
            run_work_item(item, &arena);
        } else {
            std::hint::spin_loop();
        }
    }
}

fn run_work_item(item: WorkItem, arena: &ScopeArena) {
    match item.kind() {
        // SAFETY: every queued item came from alloc_job, and a popped/stolen item is never
        // queued again, so each job runs exactly once
        JobKind::Scoped => unsafe { arena.run_job(item.slot()) },
        _ => unreachable!(),
    }
}

struct Scope<'s, 'env: 's> {
    /// The bump arena used for allocating jobs in the scope and its nested child scopes
    arena: &'s ScopeArena,
    /// Counts up on spawning of new tasks and down to track the progress of the scope, work is complete when pending == 0
    pending: AtomicU32,
    /// Stops the compiler shortening `'s`, so a job can't borrow a loca that dies before the end of
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
    threads: Vec<JoinHandle<()>>,
    stealers: Arc<[Stealer]>,
    arena: Arc<ScopeArena>,
    // Only one thread may own the Scheduler
    _not_send: PhantomData<*const ()>,
}
impl Scheduler {
    pub fn new() -> Self {
        let np = available_parallelism().unwrap().get();

        let (owners, stealers): (Vec<_>, Vec<_>) = (0..np).map(|_| queue::channel()).unzip();
        let mut owners = owners.into_iter();
        let stealers: Arc<[_]> = stealers.into();

        // store the main thread's Owner in TLS
        set_owner(owners.next().unwrap());

        let arena = Arc::new(ScopeArena::new(4096));

        // Spawn the threads
        let threads = owners
            .map(|owner| {
                let stealers = stealers.clone();
                let arena = arena.clone();
                std::thread::spawn(move || worker_loop(owner, stealers, arena))
            })
            .collect();

        Self {
            threads,
            stealers,
            // TODO: Work out how to size this
            arena,
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
            let item = next_work_item();
            match item {
                Some(item) => run_work_item(item, &self.arena),
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
