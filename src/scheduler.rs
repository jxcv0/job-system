use std::{
    any::Any,
    cell::RefCell,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{Arc, Mutex},
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
fn set_worker_tls(owner: Owner) {
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
    // Store local worker data
    set_worker_tls(owner);

    while !stop_flag.load(Ordering::Relaxed) {
        try_one_run(&arena, &stealers);
        std::hint::spin_loop();
    }
}

// Get the next work item from TLS owner or from a [Stealer] and then run it
fn try_one_run(arena: &ScopeArena, stealers: &[Stealer]) {
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
    /// Somewhere to store errors we get while executing jobs in the scope
    panic: Mutex<Option<Box<dyn Any + Send>>>,
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
            panic: Default::default(),
            _invariant: Default::default(),
            _env: Default::default(),
        }
    }

    /// Increment the pending count, allocate the work item then push to the queue
    fn spawn<F>(&'s self, f: F)
    where
        F: FnOnce(&'s Scope<'s, 'env>) + Send + 's,
    {
        let result = self.arena.alloc_job(move || {
            let result = catch_unwind(AssertUnwindSafe(|| f(self)));

            if let Err(e) = result {
                let mut panic = self.panic.lock().unwrap();
                panic.get_or_insert(e);
            }

            self.pending.fetch_sub(1, Ordering::Release);
        });

        match result {
            Ok(slot) => {
                // Only increment pending if we managed to allocate the slot
                self.pending.fetch_add(1, Ordering::Relaxed);
                let item = WorkItem::new().with_slot(slot).with_kind(JobKind::Scoped);
                LOCAL.with(|l| {
                    l.borrow()
                        .as_ref()
                        .expect("spawn called off a worker")
                        .push(item)
                })
            }
            Err(_) => panic!("arena out of memory"),
        }
    }
}

// Exclude a workers own stealer from the set of stealers it has access to
// Also rotates the stealers so that the search starts at the first queue after this workers stealer
fn stealers_for_worker(stealers: &[Stealer], index: usize) -> Arc<[Stealer]> {
    let (before, after) = stealers.split_at(index);
    after[1..].iter().chain(before).cloned().collect()
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
        let mut owners = owners.into_iter().enumerate();
        let stealers: Arc<[_]> = stealers.into();

        // store the main thread's worker info
        let (main_index, main_owner) = owners.next().unwrap();
        set_worker_tls(main_owner);

        // FIXME: Work out how to size this
        let arena = Arc::new(ScopeArena::new(4096));
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Spawn the threads
        let threads = owners
            .map(|(index, owner)| {
                let stealers = stealers_for_worker(&stealers, index);
                let arena = arena.clone();
                let stop_flag = stop_flag.clone();
                std::thread::spawn(move || worker_loop(owner, stealers, arena, stop_flag))
            })
            .collect();

        let stealers = stealers_for_worker(&stealers, main_index);

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
        let result = catch_unwind(AssertUnwindSafe(|| f(&scope)));

        while scope.pending.load(Ordering::Acquire) > 0 {
            try_one_run(&self.arena, &self.stealers);
            std::hint::spin_loop();
        }

        // SAFETY: We know there are no more pending jobs so it is safe to reset the arena
        unsafe { scope.arena.reset() };

        if let Err(e) = result {
            resume_unwind(e);
        }

        let mut panic = scope.panic.lock().unwrap();
        if let Some(e) = panic.take() {
            resume_unwind(e);
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
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::{Duration, Instant},
    };

    fn spin_until(done: impl Fn() -> bool) {
        while !done() {
            std::hint::spin_loop();
        }
    }

    // Runs `f` against a fresh Scheduler on its own thread and returns the message it panicked
    // with. Gives up after a deadline, so a scope that never returns fails rather than hangs.
    fn panic_message(f: impl FnOnce(&mut Scheduler) + Send + 'static) -> Option<String> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut scheduler = Scheduler::new();
            let payload = catch_unwind(AssertUnwindSafe(|| f(&mut scheduler))).err();
            let message = payload.map(|p| match p.downcast::<&str>() {
                Ok(s) => s.to_string(),
                Err(_) => "<not a &str payload>".to_string(),
            });
            tx.send(message).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("scope did not return")
    }

    #[test]
    fn panic_in_the_scope_closure_reaches_the_caller() {
        let message = panic_message(|scheduler| scheduler.scope(|_| panic!("boom")));
        assert_eq!(message.as_deref(), Some("boom"));
    }

    #[test]
    fn panic_in_a_job_reaches_the_caller() {
        let message = panic_message(|scheduler| {
            let started = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn(|_| {
                    started.store(true, Ordering::Relaxed);
                    panic!("boom");
                });
                // Keep the main thread out of the queue, so a worker runs the job
                spin_until(|| started.load(Ordering::Relaxed));
            });
        });
        assert_eq!(message.as_deref(), Some("boom"));
    }

    #[test]
    fn scope_closure_panic_waits_for_running_jobs() {
        let finished = Arc::new(AtomicBool::new(false));
        let job_finished = finished.clone();
        let message = panic_message(move |scheduler| {
            let started = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn(|_| {
                    started.store(true, Ordering::Relaxed);
                    thread::sleep(Duration::from_millis(100));
                    job_finished.store(true, Ordering::Relaxed);
                });
                spin_until(|| started.load(Ordering::Relaxed));
                panic!("boom");
            });
        });
        assert_eq!(message.as_deref(), Some("boom"));
        assert!(
            finished.load(Ordering::Relaxed),
            "scope unwound before its job finished"
        );
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
    fn consecutive_scopes_can_spawn_more_jobs_in_total_than_the_arena_holds() {
        let ran = Arc::new(AtomicUsize::new(0));
        let counted = ran.clone();
        // Each scope fits in the arena on its own, but together they overflow it
        let message = panic_message(move |scheduler| {
            for _ in 0..10 {
                scheduler.scope(|s| {
                    for _ in 0..1000 {
                        s.spawn(|_| {
                            counted.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                });
            }
        });
        assert_eq!(message, None);
        assert_eq!(ran.load(Ordering::Relaxed), 10_000);
    }

    #[test]
    fn spawning_past_the_end_of_the_arena_reaches_the_caller() {
        let message = panic_message(|scheduler| {
            scheduler.scope(|s| {
                for _ in 0..10_000 {
                    s.spawn(|_| {});
                }
            });
        });
        assert_eq!(message.as_deref(), Some("arena out of memory"));
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

    // Makes `n` queues, each holding the items listed for it, so a steal shows which queue it hit
    fn queues_holding(items: impl Fn(usize) -> Vec<u32>, n: usize) -> (Vec<Owner>, Vec<Stealer>) {
        let (owners, stealers): (Vec<_>, Vec<_>) = (0..n).map(|_| queue::channel()).unzip();
        for (i, owner) in owners.iter().enumerate() {
            for bits in items(i) {
                owner.push(WorkItem::from_bits(bits));
            }
        }
        (owners, stealers)
    }

    #[test]
    fn stealers_for_worker_start_after_the_worker_and_wrap_around() {
        let n = 4;
        for index in 0..n {
            // Each queue holds its own index
            let (_owners, stealers) = queues_holding(|i| vec![i as u32], n);
            let stolen: Vec<_> = stealers_for_worker(&stealers, index)
                .iter()
                .map(|s| s.steal().unwrap().into_bits())
                .collect();
            let expected: Vec<_> = (1..n).map(|k| ((index + k) % n) as u32).collect();
            assert_eq!(stolen, expected, "worker {index}");
        }
    }

    #[test]
    fn worker_never_steals_from_its_own_queue() {
        let n = 4;
        for index in 0..n {
            // Only this worker's own queue holds anything
            let (_owners, stealers) =
                queues_holding(|i| if i == index { vec![1, 2] } else { vec![] }, n);
            for stealer in stealers_for_worker(&stealers, index).iter() {
                assert!(
                    stealer.steal().is_none(),
                    "worker {index} stole its own job"
                );
            }
        }
    }
}
