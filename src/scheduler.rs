use std::{
    any::Any,
    cell::RefCell,
    marker::PhantomData,
    num::NonZero,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    range::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
    thread::{JoinHandle, available_parallelism},
};

use crate::{
    CacheAligned,
    arena::ScopeArena,
    jobs::{JobKind, WorkItem},
    queue::{self, Owner, Stealer},
    unscoped_slab::{UnscopedSlab, UnscopedSlabError},
};

const MAX_UNSCOPED_IN_FLIGHT: usize = 4;

thread_local! {
    /// This is the local queue that the thread owns
    static LOCAL_OWNER: RefCell<Option<Owner>> = const { RefCell::new(None) };
}

/// Set the owner in TLS
fn set_worker_tls(owner: Owner) {
    LOCAL_OWNER.with(move |l| {
        assert!(l.borrow().is_none(), "This thread already has a scheduler");
        *l.borrow_mut() = Some(owner)
    });
}

/// Get the next work item from the owner in TLS
fn next_work_item() -> Option<WorkItem> {
    LOCAL_OWNER.with(|l| l.borrow().as_ref().unwrap().pop())
}

/// Steal some work from another thread
fn steal_work_item(stealers: &[Stealer]) -> Option<WorkItem> {
    stealers.iter().find_map(Stealer::steal)
}

struct Shared {
    arena: ScopeArena,
    stop_flag: CacheAligned<AtomicBool>,
    unscoped: Unscoped,
}

struct Unscoped {
    /// The slab where unscoped work is stored
    slab: UnscopedSlab,
    /// The number of unscoped work items currently in flight
    in_flight: CacheAligned<AtomicUsize>,
    /// The number of unscoped work items waiting to be run
    pending: CacheAligned<AtomicUsize>,
    /// Storage for panics during unscoped work
    panic: Mutex<Option<Box<dyn Any + Send>>>,
}

fn worker_loop(
    owner: Owner,
    stealers: Arc<[Stealer]>,
    shared_stealer: Stealer,
    shared: Arc<Shared>,
) {
    // Store local worker data
    set_worker_tls(owner);

    while !shared.stop_flag.load(Ordering::Relaxed) {
        if try_scoped_run(&shared.arena, &stealers) {
            continue;
        }

        if try_unscoped_run(&shared.unscoped, &shared_stealer) {
            continue;
        }

        std::thread::yield_now();
    }
}

// Get the next work item from TLS owner or from a [Stealer] and then run it
// Returns true if it found a job to run otherwise false, this is used to work out if the thread
// should yield
fn try_scoped_run(arena: &ScopeArena, stealers: &[Stealer]) -> bool {
    if let Some(item) = next_work_item() {
        run_scoped_item(item, arena);
        return true;
    } else if let Some(item) = steal_work_item(stealers) {
        // TODO: Split the work item first before starting one of the splits
        run_scoped_item(item, arena);
        return true;
    }
    false
}

fn try_unscoped_run(unscoped: &Unscoped, shared_stealer: &Stealer) -> bool {
    if unscoped
        .in_flight
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            (n < MAX_UNSCOPED_IN_FLIGHT).then_some(n + 1)
        })
        .is_err()
    {
        return false;
    }

    let did_work = match shared_stealer.steal() {
        Some(item) => match item.kind() {
            JobKind::Unscoped => {
                let f = unscoped
                    .slab
                    .take(item.slot())
                    .expect("Expected work allocated in slab, pending counter out of sync");
                if let Err(e) = catch_unwind(AssertUnwindSafe(f)) {
                    unscoped.panic.lock().unwrap().get_or_insert(e);
                }
                true
            }
            _ => unreachable!(),
        },
        None => false,
    };

    unscoped.in_flight.fetch_sub(1, Ordering::Relaxed);
    if did_work {
        unscoped.pending.fetch_sub(1, Ordering::Release);
    }
    did_work
}

// Run a work item
fn run_scoped_item(item: WorkItem, arena: &ScopeArena) {
    match item.kind() {
        // SAFETY: every queued item came from alloc_job, and a popped/stolen item is never
        // queued again, so each job runs exactly once
        JobKind::Scoped => unsafe { arena.run_job(item.slot()) },
        _ => unreachable!(),
    }
}

pub struct Scope<'s, 'env: 's> {
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
    pub fn spawn_scoped<F>(&'s self, f: F)
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
                LOCAL_OWNER.with(|l| {
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

pub struct Scheduler {
    threads: Vec<JoinHandle<()>>,
    stealers: Arc<[Stealer]>,
    /// shared owner for enqueing unscoped work
    shared_owner: Owner,
    shared: Arc<Shared>,
    // Only one thread may own the Scheduler
    _not_send: PhantomData<*const ()>,
}
impl Scheduler {
    pub fn new() -> Self {
        let np = available_parallelism().unwrap().get();

        let (owners, stealers): (Vec<_>, Vec<_>) = (0..np).map(|_| queue::channel()).unzip();
        let mut owners = owners.into_iter().enumerate();
        let stealers: Arc<[_]> = stealers.into();

        let (shared_owner, shared_stealer) = queue::channel();

        // store the main thread's worker info
        let (main_index, main_owner) = owners.next().unwrap();
        set_worker_tls(main_owner);

        let shared = Arc::new(Shared {
            // FIXME: Work out how to size this arena
            arena: ScopeArena::new(4096),
            stop_flag: CacheAligned::new(AtomicBool::new(false)),
            unscoped: Unscoped {
                slab: UnscopedSlab::new(),
                in_flight: CacheAligned::new(AtomicUsize::new(0)),
                pending: CacheAligned::new(AtomicUsize::new(0)),
                panic: Mutex::new(None),
            },
        });

        // Spawn the threads
        let threads = owners
            .map(|(index, owner)| {
                let shared = shared.clone();
                let stealers = stealers_for_worker(&stealers, index);
                let shared_stealer = shared_stealer.clone();

                std::thread::spawn(move || {
                    worker_loop(owner, stealers, shared_stealer, shared);
                })
            })
            .collect();

        let stealers = stealers_for_worker(&stealers, main_index);

        Self {
            threads,
            stealers,
            shared_owner,
            shared,
            _not_send: Default::default(),
        }
    }

    /// Set up a scope and then help the worker threads out until the scope is complete
    pub fn scope<'env, F>(&'env mut self, f: F)
    where
        F: for<'s> FnOnce(&'s Scope<'s, 'env>),
    {
        let scope = Scope::new(&self.shared.arena);
        let result = catch_unwind(AssertUnwindSafe(|| f(&scope)));

        while scope.pending.load(Ordering::Acquire) > 0 {
            try_scoped_run(&self.shared.arena, &self.stealers);
            std::thread::yield_now();
        }

        // SAFETY: We know there are no more pending jobs (scope.pending == 0) so it is safe to reset the arena
        unsafe { scope.arena.reset() };

        if let Err(e) = result {
            resume_unwind(e);
        }

        let mut panic = scope.panic.lock().unwrap();
        if let Some(e) = panic.take() {
            resume_unwind(e);
        }
    }

    // Set up a task that a thread will pick up and run until it's complete or returns an error
    // This differs from Scheduler::scope in that the scheduler will not track the completion of the
    // task
    // Returns an error if too many unscoped tasks are already queued or running
    pub fn unscoped<F>(&mut self, f: F) -> Result<(), UnscopedSlabError>
    where
        F: FnOnce() + Send + 'static,
    {
        let unscoped = &self.shared.unscoped;
        let item = unscoped.slab.allocate(JobKind::Unscoped, f)?;
        unscoped.pending.fetch_add(1, Ordering::Relaxed);
        self.shared_owner.push(item);
        Ok(())
    }

    fn split_range<'s, 'env, F>(
        s: &'s Scope<'s, 'env>,
        range: Range<usize>,
        max_grain: Option<NonZero<usize>>,
        f: &'s F,
        max_depth: u32,
        current_depth: u32,
    ) where
        F: Fn(usize) + Sync + 's,
    {
        assert!(range.end >= range.start);
        let len = range.end - range.start;

        if current_depth >= max_depth {
            if let Some(max_grain) = max_grain
                && len >= max_grain.get()
            {
                range.iter().for_each(&f);
            }
            let depth = current_depth + 1;
            let halflen = len / 2;
            let mid = range.start + halflen;
            let (left, right) = (Range::from(range.start..mid), Range::from(mid..range.end));
            s.spawn_scoped(move |s| Self::split_range(s, left, max_grain, f, max_depth, depth));
            Self::split_range(s, right, max_grain, f, max_depth, depth);
        } else {
            range.iter().for_each(&f);
        }
    }

    pub fn par_for<F>(&mut self, range: Range<usize>, max_grain: Option<NonZero<usize>>, f: F)
    where
        F: Fn(usize) + Sync,
    {
        let max_depth = self.threads.len().ilog2();
        self.scope(|s| Self::split_range(s, range, max_grain, &f, max_depth, 0));
    }
}
impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for Scheduler {
    fn drop(&mut self) {
        // Wait for unscoped tasks to finish
        // Since only the main thread may push jobs we don't have to worry about more unscoped work
        // being enqueued before we set the stop flag
        while self.shared.unscoped.pending.load(Ordering::Acquire) > 0 {
            std::thread::yield_now();
        }

        self.shared.stop_flag.store(true, Ordering::Relaxed);
        let mut panic = self.shared.unscoped.panic.lock().unwrap().take();

        for t in self.threads.drain(..) {
            if let Err(e) = t.join() {
                panic.get_or_insert(e);
            }
        }
        LOCAL_OWNER.with(|l| *l.borrow_mut() = None);

        if let Some(e) = panic
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(e);
        }
    }
}

#[cfg(test)]
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

    // How much longer the tests' deadlines are under Miri, which interprets every step and so runs
    // far slower than native code
    const SLOWDOWN: u64 = if cfg!(miri) { 60 } else { 1 };

    // Turns a panic payload into its message
    fn payload_message(payload: Box<dyn Any + Send>) -> String {
        match payload.downcast::<&str>() {
            Ok(s) => s.to_string(),
            Err(payload) => match payload.downcast::<String>() {
                Ok(s) => *s,
                Err(_) => "<not a string payload>".to_string(),
            },
        }
    }

    // Runs `f` against a fresh Scheduler on its own thread and returns the message it panicked
    // with. Gives up after a deadline, so a scope that never returns fails rather than hangs.
    fn panic_message(f: impl FnOnce(&mut Scheduler) + Send + 'static) -> Option<String> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut scheduler = Scheduler::new();
            let payload = catch_unwind(AssertUnwindSafe(|| f(&mut scheduler))).err();
            tx.send(payload.map(payload_message)).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5 * SLOWDOWN))
            .expect("scope did not return")
    }

    // Like `panic_message`, but the Scheduler is dropped inside the catch, so a panic re-raised
    // by Drop is returned too
    fn panic_message_including_drop(
        f: impl FnOnce(&mut Scheduler) + Send + 'static,
    ) -> Option<String> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let payload = catch_unwind(AssertUnwindSafe(|| f(&mut Scheduler::new()))).err();
            tx.send(payload.map(payload_message)).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5 * SLOWDOWN))
            .expect("scheduler was not dropped")
    }

    #[test]
    fn panic_in_the_scope_closure_reaches_the_caller() {
        let message = panic_message(|scheduler| scheduler.scope(|_| panic!("boom")));
        assert_eq!(message.as_deref(), Some("boom"));
    }

    #[test]
    fn panic_in_a_job_reaches_the_caller() {
        let message = panic_message(|scheduler| {
            // If every worker is holding a slot, free one to steal the scoped job
            let _free_a_worker = scheduler.threads.len() <= MAX_UNSCOPED_IN_FLIGHT;
            let started = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn_scoped(|_| {
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
            // If every worker is holding a slot, free one to steal the scoped job
            let _free_a_worker = scheduler.threads.len() <= MAX_UNSCOPED_IN_FLIGHT;
            let started = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn_scoped(|_| {
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
                s.spawn_scoped(|_| {
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
            s.spawn_scoped(|s| {
                ran.fetch_add(1, Ordering::Relaxed);
                for _ in 0..2 {
                    s.spawn_scoped(|_| {
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
                s.spawn_scoped(|s| {
                    // Hold every worker here, so only the main thread is free to run the child
                    started.fetch_add(1, Ordering::Relaxed);
                    spin_until(|| started.load(Ordering::Relaxed) == workers);
                    s.spawn_scoped(|_| {
                        if thread::current().id() == main {
                            ran_on_main.fetch_add(1, Ordering::Relaxed);
                        }
                        ran.fetch_add(1, Ordering::Relaxed);
                    });
                    // Give up in the end, so a main thread that never steals fails rather than hangs
                    let deadline = Instant::now() + Duration::from_secs(2 * SLOWDOWN);
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
                s.spawn_scoped(|_| {
                    ran.fetch_add(1, Ordering::Relaxed);
                });
            });
        }
        assert_eq!(ran.load(Ordering::Relaxed), 2);
    }

    #[test]
    #[cfg_attr(miri, ignore = "spawns 10,000 jobs, too slow under Miri")]
    fn consecutive_scopes_can_spawn_more_jobs_in_total_than_the_arena_holds() {
        let ran = Arc::new(AtomicUsize::new(0));
        let counted = ran.clone();
        // Each scope fits in the arena on its own, but together they overflow it
        let message = panic_message(move |scheduler| {
            for _ in 0..10 {
                scheduler.scope(|s| {
                    for _ in 0..1000 {
                        s.spawn_scoped(|_| {
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
    #[cfg_attr(miri, ignore = "spawns thousands of jobs, too slow under Miri")]
    fn spawning_past_the_end_of_the_arena_reaches_the_caller() {
        let message = panic_message(|scheduler| {
            scheduler.scope(|s| {
                for _ in 0..10_000 {
                    s.spawn_scoped(|_| {});
                }
            });
        });
        assert_eq!(message.as_deref(), Some("arena out of memory"));
    }

    // Runs par_for over `range` and returns how many times each index in 0..len was visited
    fn visits(range: std::ops::Range<usize>, grain: usize, len: usize) -> Vec<usize> {
        let hits: Arc<Vec<AtomicUsize>> = Arc::new((0..len).map(|_| AtomicUsize::new(0)).collect());
        let counted = hits.clone();
        let message = panic_message(move |scheduler| {
            scheduler.par_for(
                Range::from(range),
                Some(NonZero::new(grain).unwrap()),
                |i| {
                    counted[i].fetch_add(1, Ordering::Relaxed);
                },
            );
        });
        assert_eq!(message, None);
        hits.iter().map(|h| h.load(Ordering::Relaxed)).collect()
    }

    #[test]
    fn par_for_visits_every_index_exactly_once() {
        assert_eq!(visits(0..1000, 7, 1000), vec![1; 1000]);
    }

    #[test]
    fn par_for_visits_only_the_indices_in_a_range_not_starting_at_zero() {
        let expected: Vec<_> = (0..1000).map(|i| usize::from(i >= 300)).collect();
        assert_eq!(visits(300..1000, 16, 1000), expected);
    }

    #[test]
    fn par_for_runs_a_range_smaller_than_the_grain() {
        assert_eq!(visits(0..5, 64, 5), vec![1; 5]);
    }

    #[test]
    fn par_for_over_an_empty_range_visits_nothing() {
        assert_eq!(visits(0..0, 1, 0), Vec::<usize>::new());
    }

    #[test]
    fn panic_in_a_par_for_body_reaches_the_caller() {
        let message = panic_message(|scheduler| {
            scheduler.par_for(Range::from(0..100), Some(NonZero::new(4).unwrap()), |i| {
                if i == 57 {
                    panic!("boom");
                }
            });
        });
        assert_eq!(message.as_deref(), Some("boom"));
    }

    const WAIT: Duration = Duration::from_secs(2 * SLOWDOWN);

    // Spins until `done`, failing rather than hanging if it takes too long
    fn wait_for(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::hint::spin_loop();
        }
    }

    // How many unscoped tasks can be running at once on this machine
    fn unscoped_capacity(scheduler: &Scheduler) -> usize {
        MAX_UNSCOPED_IN_FLIGHT.min(scheduler.threads.len())
    }

    // Fills every in-flight slot with an unscoped task that holds its worker until a permit is
    // added to the returned counter, and waits until they are all held. While they are held no
    // other unscoped task can start. Each permit frees one. A held task gives up after a deadline,
    // so a failing test can't leave it spinning forever.
    fn hold_unscoped_slots(scheduler: &mut Scheduler) -> Arc<AtomicUsize> {
        let slots = unscoped_capacity(scheduler);
        let permits = Arc::new(AtomicUsize::new(0));
        let held = Arc::new(AtomicUsize::new(0));
        for _ in 0..slots {
            let permits = permits.clone();
            let held = held.clone();
            scheduler
                .unscoped(move || {
                    held.fetch_add(1, Ordering::Relaxed);
                    let deadline = Instant::now() + WAIT;
                    while permits
                        .try_update(Ordering::Relaxed, Ordering::Relaxed, |p| p.checked_sub(1))
                        .is_err()
                        && Instant::now() < deadline
                    {
                        thread::yield_now();
                    }
                })
                .unwrap();
        }
        wait_for("every in-flight slot to be held", || {
            held.load(Ordering::Relaxed) == slots
        });
        permits
    }

    // Queues unscoped tasks until the scheduler refuses one, and returns how many it accepted and
    // the error it refused with. Each accepted task counts itself in `ran` when it runs.
    fn fill_unscoped(
        scheduler: &mut Scheduler,
        ran: &Arc<AtomicUsize>,
    ) -> (usize, Option<UnscopedSlabError>) {
        // Well past the slab's capacity, so a scheduler that never refuses fails rather than hangs
        for accepted in 0..1000 {
            let ran = ran.clone();
            if let Err(e) = scheduler.unscoped(move || {
                ran.fetch_add(1, Ordering::Relaxed);
            }) {
                return (accepted, Some(e));
            }
        }
        (1000, None)
    }

    #[test]
    fn unscoped_task_runs_on_a_worker_thread() {
        let message = panic_message_including_drop(|scheduler| {
            let (tx, rx) = mpsc::channel();
            scheduler
                .unscoped(move || tx.send(thread::current().id()).unwrap())
                .unwrap();
            let ran_on = rx.recv_timeout(WAIT).expect("unscoped task never ran");
            assert_ne!(
                ran_on,
                thread::current().id(),
                "ran on the scheduler's thread"
            );
        });
        assert_eq!(message, None);
    }

    #[test]
    fn drop_runs_every_queued_unscoped_task() {
        const TASKS: usize = 16;
        let ran = Arc::new(AtomicUsize::new(0));
        let counted = ran.clone();
        let message = panic_message_including_drop(move |scheduler| {
            let permits = hold_unscoped_slots(scheduler);
            for _ in 0..TASKS {
                let counted = counted.clone();
                scheduler
                    .unscoped(move || {
                        counted.fetch_add(1, Ordering::Relaxed);
                    })
                    .unwrap();
            }
            // Free the workers only once Drop has started, so every task is still queued then
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                permits.fetch_add(usize::MAX / 2, Ordering::Relaxed);
            });
        });
        assert_eq!(message, None);
        assert_eq!(ran.load(Ordering::Relaxed), TASKS);
    }

    #[test]
    fn drop_waits_for_a_running_unscoped_task() {
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = finished.clone();
        let message = panic_message_including_drop(move |scheduler| {
            let started = Arc::new(AtomicBool::new(false));
            let task_started = started.clone();
            scheduler
                .unscoped(move || {
                    task_started.store(true, Ordering::Relaxed);
                    thread::sleep(Duration::from_millis(100));
                    task_finished.store(true, Ordering::Relaxed);
                })
                .unwrap();
            wait_for("the unscoped task to start", || {
                started.load(Ordering::Relaxed)
            });
        });
        assert_eq!(message, None);
        assert!(
            finished.load(Ordering::Relaxed),
            "drop returned before the unscoped task finished"
        );
    }

    #[test]
    fn panic_in_an_unscoped_task_reaches_the_caller_on_drop() {
        let message = panic_message_including_drop(|scheduler| {
            scheduler.unscoped(|| panic!("boom")).unwrap();
        });
        assert_eq!(message.as_deref(), Some("boom"));
    }

    #[test]
    fn workers_keep_running_unscoped_tasks_after_one_panics() {
        let ran = Arc::new(AtomicBool::new(false));
        let ran_after = ran.clone();
        let message = panic_message_including_drop(move |scheduler| {
            // More panics than there are workers or in-flight slots. If a panic killed its worker
            // or leaked its slot, these would run out of one or the other before the last task
            let panics = scheduler.threads.len().max(MAX_UNSCOPED_IN_FLIGHT) + 1;
            for _ in 0..panics {
                scheduler.unscoped(|| panic!("boom")).unwrap();
            }
            scheduler
                .unscoped(move || ran_after.store(true, Ordering::Relaxed))
                .unwrap();
        });
        assert_eq!(message.as_deref(), Some("boom"));
        assert!(
            ran.load(Ordering::Relaxed),
            "no worker ran a task after a panic"
        );
    }

    #[test]
    fn no_more_than_the_in_flight_limit_of_unscoped_tasks_run_at_once() {
        const TASKS: usize = 32;
        let most = Arc::new(AtomicUsize::new(0));
        let most_seen = most.clone();
        let message = panic_message_including_drop(move |scheduler| {
            let running = Arc::new(AtomicUsize::new(0));
            for _ in 0..TASKS {
                let running = running.clone();
                let most_seen = most_seen.clone();
                scheduler
                    .unscoped(move || {
                        let now = running.fetch_add(1, Ordering::Relaxed) + 1;
                        most_seen.fetch_max(now, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(5));
                        running.fetch_sub(1, Ordering::Relaxed);
                    })
                    .unwrap();
            }
        });
        assert_eq!(message, None);
        assert!(most.load(Ordering::Relaxed) <= MAX_UNSCOPED_IN_FLIGHT);
    }

    #[test]
    fn unscoped_returns_an_error_when_too_many_tasks_are_queued() {
        let message = panic_message_including_drop(|scheduler| {
            let permits = hold_unscoped_slots(scheduler);
            let (_, refused) = fill_unscoped(scheduler, &Arc::new(AtomicUsize::new(0)));
            permits.fetch_add(usize::MAX / 2, Ordering::Relaxed);
            assert!(
                matches!(refused, Some(UnscopedSlabError::MaxSlotsReached)),
                "expected MaxSlotsReached, got {refused:?}"
            );
        });
        assert_eq!(message, None);
    }

    #[test]
    fn unscoped_accepts_tasks_again_once_queued_ones_have_run() {
        let message = panic_message_including_drop(|scheduler| {
            let permits = hold_unscoped_slots(scheduler);
            let ran = Arc::new(AtomicUsize::new(0));
            let (accepted, _) = fill_unscoped(scheduler, &ran);
            permits.fetch_add(usize::MAX / 2, Ordering::Relaxed);
            wait_for("every accepted task to run", || {
                ran.load(Ordering::Relaxed) == accepted
            });
            assert!(scheduler.unscoped(|| {}).is_ok());
        });
        assert_eq!(message, None);
    }

    #[test]
    fn scope_does_not_run_unscoped_tasks_on_the_main_thread() {
        const PROBES: usize = 8;
        let message = panic_message_including_drop(|scheduler| {
            let main = thread::current().id();
            let permits = hold_unscoped_slots(scheduler);
            let ran_on_main = Arc::new(AtomicUsize::new(0));
            for _ in 0..PROBES {
                let ran_on_main = ran_on_main.clone();
                scheduler
                    .unscoped(move || {
                        if thread::current().id() == main {
                            ran_on_main.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                    .unwrap();
            }
            // If every worker is holding a slot, free one to steal the scoped job
            let free_a_worker = scheduler.threads.len() <= MAX_UNSCOPED_IN_FLIGHT;
            let started = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn_scoped(|_| {
                    started.store(true, Ordering::Relaxed);
                    thread::sleep(Duration::from_millis(100));
                });
                // A worker steals the job, and the main thread then waits in the scope with its own
                // queue empty while the probes are still queued, so it could take them. If a held
                // worker had to be freed, this assumes it steals scoped work before taking from the
                // shared queue
                if free_a_worker {
                    permits.fetch_add(1, Ordering::Relaxed);
                }
                wait_for("a worker to steal the scoped job", || {
                    started.load(Ordering::Relaxed)
                });
            });
            permits.fetch_add(usize::MAX / 2, Ordering::Relaxed);
            assert_eq!(ran_on_main.load(Ordering::Relaxed), 0);
        });
        assert_eq!(message, None);
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
        let arena = Arc::downgrade(&scheduler.shared);
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
