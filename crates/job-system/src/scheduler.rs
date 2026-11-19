use std::{
    any::Any,
    cell::{Cell, RefCell},
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
    sleep::Sleep,
    unscoped_slab::{UnscopedSlab, UnscopedSlabError},
};

const MAX_UNSCOPED_IN_FLIGHT: usize = 4;

/// How many times an idle worker looks for work, yielding in between, before it parks. Long enough
/// to ride out the short gaps between jobs in a frame, short enough that a worker with nothing to
/// do soon stops using the CPU
const IDLE_ROUNDS_BEFORE_PARKING: u32 = 64;

thread_local! {
    /// This is the local queue that the thread owns
    static LOCAL_OWNER: RefCell<Option<Owner>> = const { RefCell::new(None) };
    /// Every other worker's queue, for stealing from. Starts at the next worker and wraps round,
    /// see [`stealers_for_worker`]
    static LOCAL_STEALERS: RefCell<Option<Arc<[Stealer]>>> = const { RefCell::new(None) };
    /// Which of the scheduler's threads this is: 0 for the thread that owns the `Scheduler`, then
    /// 1 up for the spawned workers. Lets a job tell whether it was stolen
    static WORKER_INDEX: Cell<usize> = const { Cell::new(0) };
}

/// Set the owner, stealers and worker index in TLS
fn set_worker_tls(owner: Owner, stealers: Arc<[Stealer]>, index: usize) {
    LOCAL_OWNER.with(move |l| {
        assert!(l.borrow().is_none(), "This thread already has a scheduler");
        *l.borrow_mut() = Some(owner)
    });
    LOCAL_STEALERS.set(Some(stealers));
    WORKER_INDEX.set(index);
}

/// The index of the scheduler thread this is running on, see [`WORKER_INDEX`]
fn worker_index() -> usize {
    WORKER_INDEX.get()
}

/// Get the next work item from the owner in TLS
fn next_work_item() -> Option<WorkItem> {
    LOCAL_OWNER.with(|l| l.borrow().as_ref().unwrap().pop())
}

/// Steal some work from another worker's queue
fn steal_work_item() -> Option<WorkItem> {
    LOCAL_STEALERS.with_borrow(|s| s.as_ref().unwrap().iter().find_map(Stealer::steal))
}

struct Shared {
    arena: ScopeArena,
    stop_flag: CacheAligned<AtomicBool>,
    unscoped: Unscoped,
    /// Where idle workers park until work is pushed
    sleep: Sleep,
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
    worker: usize,
    owner: Owner,
    stealers: Arc<[Stealer]>,
    shared_stealer: Stealer,
    shared: Arc<Shared>,
) {
    // Store local worker data; owner 0 is the main thread's, so spawned workers count from 1
    set_worker_tls(owner, stealers, worker + 1);
    shared.sleep.register(worker);

    let try_run =
        || try_scoped_run(&shared.arena) || try_unscoped_run(&shared.unscoped, &shared_stealer);

    let mut idle_rounds = 0;
    while !shared.stop_flag.load(Ordering::Relaxed) {
        if try_run() {
            idle_rounds = 0;
        } else if idle_rounds < IDLE_ROUNDS_BEFORE_PARKING {
            idle_rounds += 1;
            std::thread::yield_now();
        } else {
            idle_rounds = 0;
            // Announce the sleep before the last look for work, so a push that lands in between
            // sees the sleeper and wakes it, see the `sleep` module
            shared.sleep.prepare(worker);
            if try_run() {
                shared.sleep.cancel(worker);
            } else {
                shared.sleep.wait(worker, &shared.stop_flag);
            }
        }
    }
}

// Get the next work item from TLS owner or from a [Stealer] and then run it
// Returns true if it found a job to run otherwise false, this is used to work out if the thread
// should yield
fn try_scoped_run(arena: &ScopeArena) -> bool {
    if let Some(item) = next_work_item() {
        run_scoped_item(item, arena);
        return true;
    } else if let Some(item) = steal_work_item() {
        // Jobs that can split, such as par_for's, notice they were stolen and split again
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
                    .take(item)
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
    /// Woken after each spawn, in case every worker is parked
    sleep: &'s Sleep,
    /// How many threads the scheduler has, counting the one that owns it. par_for's split budget
    /// starts here
    threads: u32,
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
    fn new(arena: &'s ScopeArena, sleep: &'s Sleep, threads: u32) -> Self {
        Self {
            arena,
            sleep,
            threads,
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
                });
                self.sleep.wake_one();
            }
            Err(_) => panic!("arena out of memory"),
        }
    }

    /// Open a scope nested in this one, run `f` in it, and return once every job spawned in the
    /// nested scope has finished. Meanwhile this thread runs other scoped jobs, so it keeps
    /// working while it waits. Jobs in the nested scope can borrow from the code that opened it,
    /// such as a system's locals, because they all finish before this returns.
    ///
    /// Unlike [`Scheduler::scope`], this does not free the arena when it ends: jobs of the scopes
    /// around it may still be using it. Everything is freed when the outermost scope ends.
    pub fn scope<'env2, F>(&self, f: F)
    where
        F: for<'s2> FnOnce(&'s2 Scope<'s2, 'env2>),
    {
        let scope = Scope::new(self.arena, self.sleep, self.threads);
        let result = catch_unwind(AssertUnwindSafe(|| f(&scope)));
        scope.wait();
        scope.resume_panic(result);
    }

    /// Run `f` for every index in `range`, split across the scheduler's threads, and return once
    /// every call has finished. Ranges at or below `grain` long are never split, so set it to the
    /// smallest chunk worth a job. With no grain, ranges split down to single indices if needed.
    ///
    /// Callable from any job: the loop runs in a nested scope, so a job such as an ECS system can
    /// run its loop in parallel and know every index is done before it carries on.
    pub fn par_for<F>(&self, range: Range<usize>, grain: Option<NonZero<usize>>, f: F)
    where
        F: Fn(usize) + Sync,
    {
        let threads = self.threads;
        self.scope(|s| split_or_run(s, range, grain, &f, threads, threads, worker_index()));
    }

    /// Run scoped jobs on this thread, its own or stolen, until every job spawned in this scope
    /// has finished. The jobs it runs can belong to any scope, which keeps the thread busy
    fn wait(&self) {
        while self.pending.load(Ordering::Acquire) > 0 {
            if !try_scoped_run(self.arena) {
                std::thread::yield_now();
            }
        }
    }

    /// Re-raise a panic from the scope's closure, else the first one from its jobs
    fn resume_panic(&self, result: std::thread::Result<()>) {
        if let Err(e) = result {
            resume_unwind(e);
        }
        if let Some(e) = self.panic.lock().unwrap().take() {
            resume_unwind(e);
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
    /// shared owner for enqueing unscoped work
    shared_owner: Owner,
    shared: Arc<Shared>,
    // Only one thread may own the Scheduler
    _not_send: PhantomData<*const ()>,
}
impl Scheduler {
    /// A scheduler with one thread per CPU, counting the calling thread, and at least two
    pub fn new() -> Self {
        Self::with_threads(available_parallelism().map_or(2, NonZero::get).max(2))
    }

    /// A scheduler with `threads` threads, counting the calling thread, so it spawns
    /// `threads - 1` workers. Leave CPUs out to keep them free for other threads, such as a
    /// render thread.
    ///
    /// # Panics
    ///
    /// If `threads` is below 2. Unscoped jobs only run on spawned workers, so at least one is needed
    pub fn with_threads(threads: usize) -> Self {
        assert!(
            threads >= 2,
            "a Scheduler needs at least 2 threads, got {threads}"
        );
        let np = threads;

        let (owners, stealers): (Vec<_>, Vec<_>) = (0..np).map(|_| queue::channel()).unzip();
        let mut owners = owners.into_iter().enumerate();
        let stealers: Arc<[_]> = stealers.into();

        let (shared_owner, shared_stealer) = queue::channel();

        // store the main thread's worker info
        let (main_index, main_owner) = owners.next().unwrap();
        set_worker_tls(
            main_owner,
            stealers_for_worker(&stealers, main_index),
            main_index,
        );

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
            // One per spawned worker; the main thread never parks
            sleep: Sleep::new(np - 1),
        });

        // Spawn the threads
        let threads = owners
            .map(|(index, owner)| {
                let shared = shared.clone();
                let stealers = stealers_for_worker(&stealers, index);
                let shared_stealer = shared_stealer.clone();

                std::thread::spawn(move || {
                    // Owner 0 is the main thread's, so spawned workers count from 1
                    worker_loop(index - 1, owner, stealers, shared_stealer, shared);
                })
            })
            .collect();

        Self {
            threads,
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
        let threads = self.threads.len() as u32 + 1;
        let scope = Scope::new(&self.shared.arena, &self.shared.sleep, threads);
        let result = catch_unwind(AssertUnwindSafe(|| f(&scope)));
        scope.wait();

        // SAFETY: nothing in this scope is pending, and a job that opens a nested scope only
        // finishes once that scope has, so no job at any depth is left. Scheduler::scope is never
        // nested itself since it takes &mut Scheduler, so no other scope has jobs in the arena
        unsafe { scope.arena.reset() };

        scope.resume_panic(result);
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
        self.shared.sleep.wake_one();
        Ok(())
    }

    /// Run `f` for every index in `range`, split across the scheduler's threads, and return once
    /// every call has finished. See [`Scope::par_for`], which does the same from inside a job
    pub fn par_for<F>(&mut self, range: Range<usize>, grain: Option<NonZero<usize>>, f: F)
    where
        F: Fn(usize) + Sync,
    {
        self.scope(|s| s.par_for(range, grain, f));
    }
}

/// Run `f` over `range` as a job that splits itself. `splits` is the job's split budget and
/// `spawned_on` the worker that spawned it. A job running on another worker was stolen, which
/// means some thread ran out of work, so it gets a fresh budget and splits again to share its
/// range with the idle threads. Otherwise the budget halves with each split, so ranges no one
/// steals stop splitting after a few levels. This is rayon's adaptive splitting.
fn split_range<'s, 'env, F>(
    s: &'s Scope<'s, 'env>,
    range: Range<usize>,
    grain: Option<NonZero<usize>>,
    f: &'s F,
    splits: u32,
    threads: u32,
    spawned_on: usize,
) where
    F: Fn(usize) + Sync + 's,
{
    let worker = worker_index();
    let splits = if worker == spawned_on {
        splits
    } else {
        splits.max(threads)
    };
    split_or_run(s, range, grain, f, splits, threads, worker);
}

/// Halve `range` while the budget lasts and it is longer than `grain`, spawning the left half
/// and carrying on with the right, then run what is left
fn split_or_run<'s, 'env, F>(
    s: &'s Scope<'s, 'env>,
    mut range: Range<usize>,
    grain: Option<NonZero<usize>>,
    f: &'s F,
    mut splits: u32,
    threads: u32,
    worker: usize,
) where
    F: Fn(usize) + Sync + 's,
{
    assert!(range.end >= range.start);
    loop {
        let len = range.end - range.start;
        let can_split = splits > 0 && len > 1 && grain.is_none_or(|g| len > g.get());
        if !can_split {
            range.iter().for_each(f);
            return;
        }
        splits /= 2;
        let mid = range.start + len / 2;
        let left = Range::from(range.start..mid);
        s.spawn_scoped(move |s| split_range(s, left, grain, f, splits, threads, worker));
        range = Range::from(mid..range.end);
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
        // Parked workers only see the stop flag once woken
        self.shared.sleep.wake_all();
        let mut panic = self.shared.unscoped.panic.lock().unwrap().take();

        for t in self.threads.drain(..) {
            if let Err(e) = t.join() {
                panic.get_or_insert(e);
            }
        }
        LOCAL_OWNER.with(|l| *l.borrow_mut() = None);
        LOCAL_STEALERS.set(None);

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
        panic_message_with(Scheduler::new, f)
    }

    // Like `panic_message`, with the Scheduler made by `new`
    fn panic_message_with(
        new: impl FnOnce() -> Scheduler + Send + 'static,
        f: impl FnOnce(&mut Scheduler) + Send + 'static,
    ) -> Option<String> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut scheduler = new();
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
    fn nested_scope_waits_for_its_jobs() {
        let mut scheduler = Scheduler::new();
        let checked = AtomicBool::new(false);
        scheduler.scope(|s| {
            s.spawn_scoped(|s| {
                let ran = AtomicUsize::new(0);
                s.scope(|inner| {
                    for _ in 0..8 {
                        inner.spawn_scoped(|_| {
                            ran.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                });
                assert_eq!(ran.load(Ordering::Relaxed), 8);
                checked.store(true, Ordering::Relaxed);
            });
        });
        assert!(checked.load(Ordering::Relaxed));
    }

    #[test]
    fn nested_scope_opened_on_a_worker_finishes() {
        let message = panic_message(|scheduler| {
            let done = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn_scoped(|s| {
                    s.scope(|inner| {
                        for _ in 0..4 {
                            inner.spawn_scoped(|_| {});
                        }
                    });
                    done.store(true, Ordering::Relaxed);
                });
                // Keep the main thread out of the queue, so a worker opens and waits on the
                // nested scope
                wait_for("a worker to finish its nested scope", || {
                    done.load(Ordering::Relaxed)
                });
            });
        });
        assert_eq!(message, None);
    }

    // Each level opens a scope with two jobs, each of which recurses
    fn nest<'s, 'env>(s: &'s Scope<'s, 'env>, depth: u32, leaves: &'env AtomicUsize) {
        if depth == 0 {
            leaves.fetch_add(1, Ordering::Relaxed);
            return;
        }
        s.scope(|inner| {
            for _ in 0..2 {
                inner.spawn_scoped(move |inner| nest(inner, depth - 1, leaves));
            }
        });
    }

    #[test]
    fn deeply_nested_scopes_run_every_job() {
        let mut scheduler = Scheduler::new();
        let leaves = AtomicUsize::new(0);
        scheduler.scope(|s| nest(s, 8, &leaves));
        assert_eq!(leaves.load(Ordering::Relaxed), 1 << 8);
    }

    #[test]
    fn nested_scope_does_not_free_jobs_of_the_scope_around_it() {
        const JOBS: usize = 64;
        let mut scheduler = Scheduler::new();
        let sum = AtomicUsize::new(0);
        let sum = &sum;
        scheduler.scope(|s| {
            // Queued before and after the nested scope; if it reset the arena, the later
            // allocations would land on top of these closures
            for i in 0..JOBS {
                s.spawn_scoped(move |_| {
                    sum.fetch_add(i, Ordering::Relaxed);
                });
            }
            s.scope(|inner| {
                for _ in 0..JOBS {
                    inner.spawn_scoped(|_| {});
                }
            });
            for i in JOBS..2 * JOBS {
                s.spawn_scoped(move |_| {
                    sum.fetch_add(i, Ordering::Relaxed);
                });
            }
        });
        assert_eq!(sum.load(Ordering::Relaxed), (0..2 * JOBS).sum());
    }

    #[test]
    fn nested_scope_keeps_its_allocations_until_the_outermost_scope_ends() {
        let mut scheduler = Scheduler::new();
        let shared = scheduler.shared.clone();
        let arena = &shared.arena;
        scheduler.scope(|s| {
            let before = arena.used_lines();
            s.scope(|inner| {
                for _ in 0..4 {
                    inner.spawn_scoped(|_| {});
                }
            });
            assert!(
                arena.used_lines() >= before + 4,
                "the nested scope freed its jobs' lines"
            );
        });
        assert_eq!(arena.used_lines(), 0, "the outermost scope didn't reset");
    }

    #[test]
    fn panic_in_a_nested_job_reaches_the_caller() {
        let message = panic_message(|scheduler| {
            scheduler.scope(|s| {
                s.spawn_scoped(|s| {
                    s.scope(|inner| inner.spawn_scoped(|_| panic!("nested boom")));
                });
            });
        });
        assert_eq!(message.as_deref(), Some("nested boom"));
    }

    #[test]
    fn nested_jobs_can_borrow_locals_of_the_job_that_opened_the_scope() {
        let mut scheduler = Scheduler::new();
        let total = AtomicUsize::new(0);
        scheduler.scope(|s| {
            s.spawn_scoped(|s| {
                let values = vec![1, 2, 3, 4];
                s.scope(|inner| {
                    for v in &values {
                        let total = &total;
                        inner.spawn_scoped(move |_| {
                            total.fetch_add(*v, Ordering::Relaxed);
                        });
                    }
                });
                // `values` is dropped here, after every nested job has finished with it
            });
        });
        assert_eq!(total.into_inner(), 10);
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
        visits_with(Scheduler::new, range, NonZero::new(grain), len)
    }

    // Like `visits`, with the Scheduler made by `new` and an optional grain
    fn visits_with(
        new: impl FnOnce() -> Scheduler + Send + 'static,
        range: std::ops::Range<usize>,
        grain: Option<NonZero<usize>>,
        len: usize,
    ) -> Vec<usize> {
        let hits: Arc<Vec<AtomicUsize>> = Arc::new((0..len).map(|_| AtomicUsize::new(0)).collect());
        let counted = hits.clone();
        let message = panic_message_with(new, move |scheduler| {
            scheduler.par_for(Range::from(range), grain, |i| {
                counted[i].fetch_add(1, Ordering::Relaxed);
            });
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
    fn par_for_visits_every_index_exactly_once_on_small_machines() {
        for threads in 2..=5 {
            for grain in [None, NonZero::new(1), NonZero::new(7)] {
                assert_eq!(
                    visits_with(move || Scheduler::with_threads(threads), 0..100, grain, 100),
                    vec![1; 100],
                    "{threads} threads, grain {grain:?}"
                );
            }
        }
    }

    #[test]
    fn par_for_with_no_grain_visits_every_index_exactly_once() {
        let len = if cfg!(miri) { 100 } else { 1000 };
        assert_eq!(visits_with(Scheduler::new, 0..len, None, len), vec![1; len]);
    }

    #[test]
    #[should_panic(expected = "at least 2 threads")]
    fn scheduler_with_fewer_than_two_threads_panics() {
        Scheduler::with_threads(1);
    }

    #[test]
    fn par_for_runs_on_more_than_one_thread() {
        let message = panic_message_with(
            || Scheduler::with_threads(2),
            |scheduler| {
                // Each body waits for a second body to be running at the same time, which only
                // happens if the range was shared with the worker
                let running = AtomicUsize::new(0);
                scheduler.par_for(Range::from(0..2), None, |_| {
                    running.fetch_add(1, Ordering::Relaxed);
                    wait_for("a second thread to run part of the range", || {
                        running.load(Ordering::Relaxed) == 2
                    });
                });
            },
        );
        assert_eq!(message, None);
    }

    #[test]
    fn stolen_range_splits_again() {
        let message = panic_message_with(
            || Scheduler::with_threads(2),
            |scheduler| {
                let done: Vec<AtomicBool> = (0..16).map(|_| AtomicBool::new(false)).collect();
                let started = AtomicBool::new(false);
                let f = |i: usize| {
                    started.store(true, Ordering::Relaxed);
                    // Run as a single chunk, index 0 would wait on index 15 forever. Only a
                    // thief that splits the range runs the last part first
                    if i == 0 {
                        wait_for("index 15 to run before index 0", || {
                            done[15].load(Ordering::Relaxed)
                        });
                    }
                    done[i].store(true, Ordering::Relaxed);
                };
                let f = &f;
                let main = worker_index();
                scheduler.scope(|s| {
                    // No budget left, as if it had already split all it was allowed to
                    s.spawn_scoped(move |s| {
                        split_range(s, Range::from(0..16), None, f, 0, 2, main)
                    });
                    // Keep the main thread out of the queue, so the worker steals the job
                    wait_for("the worker to steal the range", || {
                        started.load(Ordering::Relaxed)
                    });
                });
            },
        );
        assert_eq!(message, None);
    }

    #[test]
    fn range_that_is_not_stolen_keeps_its_budget() {
        let mut scheduler = Scheduler::with_threads(2);
        let order = Mutex::new(Vec::new());
        let f = |i: usize| order.lock().unwrap().push(i);
        let f = &f;
        scheduler.scope(|s| {
            // Run on the thread that spawned it, with no budget, so it must not split
            split_range(s, Range::from(0..16), None, f, 0, 2, worker_index());
        });
        assert_eq!(order.into_inner().unwrap(), (0..16).collect::<Vec<_>>());
    }

    // Counts each index of 0..len, to check a par_for visited every one exactly once
    fn hit_counter(len: usize) -> Vec<AtomicUsize> {
        (0..len).map(|_| AtomicUsize::new(0)).collect()
    }

    fn assert_each_hit_once(hits: &[AtomicUsize]) {
        let hits: Vec<_> = hits.iter().map(|h| h.load(Ordering::Relaxed)).collect();
        assert_eq!(hits, vec![1; hits.len()]);
    }

    #[test]
    fn scope_par_for_in_a_job_visits_every_index_exactly_once() {
        let mut scheduler = Scheduler::new();
        let hits = hit_counter(500);
        scheduler.scope(|s| {
            s.spawn_scoped(|s| {
                s.par_for(Range::from(0..500), NonZero::new(8), |i| {
                    hits[i].fetch_add(1, Ordering::Relaxed);
                });
            });
        });
        assert_each_hit_once(&hits);
    }

    #[test]
    fn scope_par_for_on_a_worker_has_finished_when_it_returns() {
        let message = panic_message(|scheduler| {
            let hits = hit_counter(200);
            let done = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn_scoped(|s| {
                    s.par_for(Range::from(0..200), None, |i| {
                        hits[i].fetch_add(1, Ordering::Relaxed);
                    });
                    // Every index must be done here, before the job carries on
                    assert_each_hit_once(&hits);
                    done.store(true, Ordering::Relaxed);
                });
                // Keep the main thread out of the queue, so a worker runs the job and its loop
                wait_for("a worker to finish its par_for", || {
                    done.load(Ordering::Relaxed)
                });
            });
        });
        assert_eq!(message, None);
    }

    #[test]
    fn scope_par_for_in_two_jobs_at_once_visits_every_index_of_both() {
        let mut scheduler = Scheduler::new();
        let (a, b) = (hit_counter(300), hit_counter(300));
        scheduler.scope(|s| {
            // Like two independent systems, each running its own loop
            for hits in [&a, &b] {
                s.spawn_scoped(move |s| {
                    s.par_for(Range::from(0..300), NonZero::new(4), |i| {
                        hits[i].fetch_add(1, Ordering::Relaxed);
                    });
                });
            }
        });
        assert_each_hit_once(&a);
        assert_each_hit_once(&b);
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

    // Waits until every worker thread has parked
    fn wait_for_every_worker_to_park(scheduler: &Scheduler) {
        wait_for("every worker to park", || {
            scheduler.shared.sleep.parked() == scheduler.threads.len()
        });
    }

    #[test]
    fn idle_workers_park() {
        let scheduler = Scheduler::new();
        wait_for_every_worker_to_park(&scheduler);
    }

    #[test]
    fn parked_workers_wake_for_scoped_work() {
        let message = panic_message(|scheduler| {
            wait_for_every_worker_to_park(scheduler);
            let started = AtomicBool::new(false);
            scheduler.scope(|s| {
                s.spawn_scoped(|_| started.store(true, Ordering::Relaxed));
                // Keep the main thread out of the queue, so only a woken worker can run the job
                wait_for("a parked worker to wake and run the job", || {
                    started.load(Ordering::Relaxed)
                });
            });
        });
        assert_eq!(message, None);
    }

    #[test]
    fn parked_workers_wake_for_unscoped_work() {
        let message = panic_message_including_drop(|scheduler| {
            wait_for_every_worker_to_park(scheduler);
            let (tx, rx) = mpsc::channel();
            scheduler.unscoped(move || tx.send(()).unwrap()).unwrap();
            rx.recv_timeout(WAIT)
                .expect("no parked worker woke for the unscoped task");
        });
        assert_eq!(message, None);
    }

    #[test]
    fn drop_wakes_parked_workers() {
        let message = panic_message_including_drop(|scheduler| {
            wait_for_every_worker_to_park(scheduler);
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
