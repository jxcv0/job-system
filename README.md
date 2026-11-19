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
first. The worker loop pops its own deque, else steals. Dropping the `Scheduler` stops and joins
the workers.

**Idle workers park.** A worker that finds nothing for `IDLE_ROUNDS_BEFORE_PARKING` (64) rounds,
yielding in between, sets its bit in a parked bitmask, looks for work once more, and parks.
Every push then checks the bitmask, and if a worker is parked it claims that bit and unparks the
worker. Claiming means each parked worker is woken once, not once per push. With nobody parked,
the check costs a `SeqCst` fence and one load, roughly 25 ns per spawn. The fence pairs with one
in the parking path, so a push and a worker going to sleep can't miss each other. The thread that
owns the `Scheduler` never parks, but it does spin while it waits for a scope to finish.

**`Scheduler::scope` runs scoped jobs.** `Scope::spawn` copies the closure into the
`ScopeArena` behind a type-erased `JobHeader` and pushes its line index. While the scope has
jobs pending, the calling thread runs and steals work instead of blocking. A panic in the scope
closure or in any job is carried back and re-raised on the caller once every job has finished.

## Job kinds

- **Scoped**: must finish by a user-defined sync point. Their allocations only need to live
  until then, so they come from a bump allocator reset at the end of the scope.
- **Unscoped**: run until done, possibly across frames. Their allocations live as long as the
  job, so they come from a slab with generation counters to catch stale indices.

There is no separate IO job kind. Asset loading and other blocking IO run as unscoped jobs.
A blocking job holds its worker for as long as the IO takes. At most `MAX_UNSCOPED_IN_FLIGHT` (4)
unscoped jobs run at once, which caps how many workers loading can tie up.

## TODO

- **Grow the deque instead of asserting.** Plan: hold the slots behind an `AtomicPtr<Buffer>`.
  When `push` finds the buffer full (load `top` with `Acquire`), it allocates one twice the
  size, copies indices `top..bottom` across at the same logical positions, and publishes it
  with `Release`. `steal` loads the pointer with `Acquire` after reading `bottom`. Old buffers
  may still be read by a thief, so they go on a retired list freed when the queue drops.
- **Size the `ScopeArena`.** It resets when each scope ends, but its size (4096 lines) is a
  placeholder, and a single scope that spawns past it panics with "arena out of memory".

## Testing

```bash
cargo test
```

The tests also run under [Miri](https://github.com/rust-lang/miri), which reports data races
and undefined behaviour, such as in the arena's `unsafe` code. Miri reports a single CPU by
default, which would leave the scheduler with no workers, so give it more:

```bash
MIRIFLAGS="-Zmiri-num-cpus=4" cargo +nightly miri test --lib
```

Tests that race threads repeat themselves, and scale down under `cfg(miri)` because Miri is far
slower. CI runs both on every push to `main` and on pull requests.

## Benchmarks

```bash
cargo bench
```

[Criterion](https://github.com/bheisler/criterion.rs) benches in `benches/scheduler.rs`: the cost
of an empty scope, 1000 flat spawns, a 1024-leaf spawn tree, and `par_for` at several grain sizes
against a sequential loop. Reports land in `target/criterion/`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
