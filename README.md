# Engine

A general-purpose game engine with a desktop application, built as a Cargo workspace. The game
comes later.

## Crates

| Crate | What it does |
|-------|--------------|
| [`job-system`](crates/job-system) | Work-stealing job system: scoped and unscoped jobs, `par_for`, idle workers park |

## Building

```bash
cargo build --workspace
cargo test --workspace
```

Each crate's README covers its own design, tests and benchmarks.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
