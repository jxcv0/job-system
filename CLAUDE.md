# CLAUDE.md

## How we work

- **The user writes the code.** They need to understand every line of it, so do not write or
  change implementation code unless they explicitly ask you to.
- **Your main job is unit tests.** When asked, write tests for the code the user has written.
  Tests live in a `#[cfg(test)] mod tests` at the bottom of the file under test.
- **Only do what is asked.** Don't make unrequested edits, refactors, fixes, or "improvements",
  even small ones. If you notice a bug or a problem while writing tests, point it out and
  explain it; don't fix it.
- **Explaining is welcome.** Answering questions, reviewing code, and explaining concepts or
  trade-offs are fine. Suggest code in chat when it helps, but leave applying it to the user.
- If a test fails because of a bug in the implementation, report the failure and what you
  think causes it rather than changing the implementation or weakening the test.
- **Git:** you may commit and push without asking, but only on branches named `claude/...`.
  Never commit or push to `main` or any other branch.

## Project

A Cargo workspace for a game engine. The only crate so far is `crates/job-system`, a
work-stealing job system; its README covers the design.

## Commands

```bash
cargo build --workspace
cargo test --workspace
MIRIFLAGS="-Zmiri-num-cpus=4" cargo +nightly miri test -p job-system --lib
```

CI runs both `cargo test` and the Miri tests.

## Test conventions

- Tests that race threads repeat themselves to catch rare interleavings.
- Scale down iteration counts under `cfg!(miri)` (e.g. `if cfg!(miri) { 4 } else { 1000 }`),
  and mark tests that are too slow for Miri with `#[cfg_attr(miri, ignore = "reason")]`.
