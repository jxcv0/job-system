# job-system

A work-stealing job system for a game engine, aimed at thousands of jobs per frame.

## Design

**One Chase-Lev deque per worker thread.** The owning worker pushes and pops at the bottom
(LIFO, so the cache stays warm). Other workers steal from the top (FIFO, so they take the
oldest and usually largest work). `queue::channel()` returns the two handles:

- `Owner`: `Send` but not `Sync`, so only one thread can ever push or pop.
- `Stealer`: `Clone`, one handed to every other worker.

**Queue items are `u32` indices, not boxed closures.** A job's function and arguments live in
a separate job store, so pushing a job costs no allocation. The deque only moves indices.

**Fixed capacity.** Each deque holds `MAX_TASKS_PER_WORKER` (4096) items. Overflow is caught by
a `debug_assert!` in `push_to_bottom`; release builds do not check it yet.

**`Scheduler` owns the workers.** It makes one deque per `available_parallelism()`. The thread
that creates it is worker 0; every other deque gets its own thread. Each worker gets the other
workers' `Stealer`s, rotated so the search starts at the next queue rather than all at the
first. The worker loop pops its own deque, else steals, else spins. Dropping the `Scheduler`
stops and joins the workers.

**`Scheduler::scope` runs scoped jobs.** `Scope::spawn` copies the closure into the
`ScopeArena` behind a type-erased `JobHeader` and pushes its line index. While the scope has
jobs pending, the calling thread runs and steals work instead of blocking. A panic in the scope
closure or in any job is carried back and re-raised on the caller once every job has finished.

## Job kinds

- **Scoped**: must finish by a user-defined sync point. Their allocations only need to live
  until then, so they come from a bump allocator reset at the end of the scope.
- **Unscoped**: run until done, possibly across frames. Their allocations live as long as the
  job, so they come from a slab with generation counters to catch stale indices.
- **IoSubmission**: start kernel IO, then let the worker move on to other jobs.
- **IoCompletion**: finish an IO submission once the kernel is done.

Unscoped jobs come first, then the others.

## TODO

- **Grow the deque instead of asserting.** Plan: hold the slots behind an `AtomicPtr<Buffer>`.
  When `push` finds the buffer full (load `top` with `Acquire`), it allocates one twice the
  size, copies indices `top..bottom` across at the same logical positions, and publishes it
  with `Release`. `steal` loads the pointer with `Acquire` after reading `bottom`. Old buffers
  may still be read by a thief, so they go on a retired list freed when the queue drops.
- **Reset the `ScopeArena` when a scope ends.** Its cursor only grows, so repeated scopes
  eventually fail with `OutOfMemory`. Its size (4096 lines) is also a placeholder.
- **Idle workers spin.** Sleep when there is nothing to pop or steal.
- **Unscoped and IO jobs.** The slab allocator with generation counters, and the IO job kinds.
  `run_work_item` only handles `JobKind::Scoped` today.

## Testing

```bash
cargo test
```

The queue is also model-checked with [loom](https://github.com/tokio-rs/loom). Under
`cfg(loom)`, `src/sync.rs` swaps in loom's atomics and the deque shrinks to 4 slots:

```bash
RUSTFLAGS="--cfg loom" cargo test --release --lib loom
```

CI runs both on every push to `main` and on pull requests.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
