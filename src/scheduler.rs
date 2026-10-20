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
    sync::{AtomicBool, AtomicU32, Ordering},
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

fn worker_loop(
    owner: Owner,
    stealers: Arc<[Stealer]>,
    arena: Arc<ScopeArena>,
    stop_flag: Arc<AtomicBool>,
) {
    // Store the owner in our TLS
    set_owner(owner);

    while !stop_flag.load(Ordering::Relaxed) {
        tick(&arena, &stealers);
        std::hint::spin_loop();
    }
}

// Get the next work item from TLS owner or from a [Stealer] and then run it
fn tick(arena: &ScopeArena, stealers: &[Stealer]) {
    if let Some(item) = next_work_item() {
        run_work_item(item, arena);
    } else if let Some(item) = steal_work_item(stealers) {
        run_work_item(item, arena);
    }
}

// Run a work item
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

    /// Increment the pending count, allocate the work item then push to the queue
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
    stop_flag: Arc<AtomicBool>,
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

        // FIXME: Work out how to size this
        let arena = Arc::new(ScopeArena::new(4096));
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Spawn the threads
        let threads = owners
            .map(|owner| {
                let stealers = stealers.clone();
                let arena = arena.clone();
                let stop_flag = stop_flag.clone();
                std::thread::spawn(move || worker_loop(owner, stealers, arena, stop_flag))
            })
            .collect();

        Self {
            threads,
            stealers,
            arena,
            stop_flag,
            _not_send: Default::default(),
        }
    }

    /// Set up a scope and then help the worker threads out until the scope is complete
    pub fn scope<'env, F>(&'env mut self, f: F)
    where
        F: for<'s> FnOnce(&'s Scope<'s, 'env>),
    {
        let scope = Scope::new(&self.arena);
        f(&scope);

        while scope.pending.load(Ordering::Acquire) > 0 {
            tick(&self.arena, &self.stealers);
            std::hint::spin_loop();
        }
    }
}
impl Drop for Scheduler {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        let mut panic = None;

        for t in self.threads.drain(..) {
            if let Err(e) = t.join() {
                panic.get_or_insert(e);
            }
        }
        LOCAL.with(|l| *l.borrow_mut() = None);

        if let Some(e) = panic
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(e);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread,
        time::{Duration, Instant},
    };

    fn spin_until(done: impl Fn() -> bool) {
        while !done() {
            std::hint::spin_loop();
        }
    }

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
    fn main_thread_steals_children_spawned_by_busy_workers() {
        let mut scheduler = Scheduler::new();
        let workers = scheduler.threads.len();
        let main = thread::current().id();
        let started = AtomicUsize::new(0);
        let ran = AtomicUsize::new(0);
        let ran_on_main = AtomicUsize::new(0);
        scheduler.scope(|s| {
            for _ in 0..workers {
                s.spawn(|s| {
                    // Hold every worker here, so only the main thread is free to run the child
                    started.fetch_add(1, Ordering::Relaxed);
                    spin_until(|| started.load(Ordering::Relaxed) == workers);
                    s.spawn(|_| {
                        if thread::current().id() == main {
                            ran_on_main.fetch_add(1, Ordering::Relaxed);
                        }
                        ran.fetch_add(1, Ordering::Relaxed);
                    });
                    // Give up in the end, so a main thread that never steals fails rather than hangs
                    let deadline = Instant::now() + Duration::from_secs(2);
                    spin_until(|| {
                        ran.load(Ordering::Relaxed) == workers || Instant::now() > deadline
                    });
                });
            }
            // The main thread only starts taking jobs once every worker holds one
            spin_until(|| started.load(Ordering::Relaxed) == workers);
        });
        assert_eq!(ran_on_main.load(Ordering::Relaxed), workers);
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

    #[test]
    fn drop_waits_for_every_worker_to_exit() {
        let scheduler = Scheduler::new();
        // Each worker holds a clone of the arena until it exits
        let arena = Arc::downgrade(&scheduler.arena);
        drop(scheduler);
        assert!(arena.upgrade().is_none());
    }
}
