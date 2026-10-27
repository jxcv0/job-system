use std::{
    hint::black_box,
    num::NonZero,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use job_system::{Scheduler, Scope};

// Each job takes at least one arena line, so stay well under the 4096-line arena
const JOBS: u64 = 1000;

fn scope_overhead(c: &mut Criterion) {
    let mut scheduler = Scheduler::new();
    c.bench_function("scope/empty", |b| b.iter(|| scheduler.scope(|_| {})));
}

fn spawn_flat(c: &mut Criterion) {
    let mut scheduler = Scheduler::new();
    let mut group = c.benchmark_group("spawn");
    group.throughput(Throughput::Elements(JOBS));
    group.bench_function(BenchmarkId::new("flat", JOBS), |b| {
        b.iter(|| {
            let counter = AtomicU64::new(0);
            scheduler.scope(|s| {
                for _ in 0..JOBS {
                    s.spawn(|_| {
                        counter.fetch_add(1, Ordering::Relaxed);
                    });
                }
            });
            assert_eq!(counter.into_inner(), JOBS);
        })
    });
    group.finish();
}

// A binary tree of jobs, each spawning its two children: the fork-join shape stealing is built for
fn spawn_tree<'s, 'env>(s: &'s Scope<'s, 'env>, depth: u32, leaves: &'env AtomicU64) {
    if depth == 0 {
        leaves.fetch_add(1, Ordering::Relaxed);
    } else {
        s.spawn(move |s| spawn_tree(s, depth - 1, leaves));
        spawn_tree(s, depth - 1, leaves);
    }
}

fn spawn_nested(c: &mut Criterion) {
    const DEPTH: u32 = 10;
    let mut scheduler = Scheduler::new();
    let mut group = c.benchmark_group("spawn");
    group.throughput(Throughput::Elements(1 << DEPTH));
    group.bench_function(BenchmarkId::new("tree", 1 << DEPTH), |b| {
        b.iter(|| {
            let leaves = AtomicU64::new(0);
            scheduler.scope(|s| spawn_tree(s, DEPTH, &leaves));
            assert_eq!(leaves.into_inner(), 1 << DEPTH);
        })
    });
    group.finish();
}

// Enough work per index that splitting can pay for itself
fn work(i: usize) -> u64 {
    (0..64u64).fold(i as u64, |acc, x| black_box(acc.wrapping_mul(31).wrapping_add(x)))
}

fn par_for(c: &mut Criterion) {
    const LEN: usize = 1 << 16;
    let mut scheduler = Scheduler::new();
    let mut group = c.benchmark_group("par_for");
    group.throughput(Throughput::Elements(LEN as u64));

    group.bench_function("sequential", |b| {
        b.iter(|| {
            (0..LEN).for_each(|i| {
                black_box(work(i));
            })
        })
    });

    // LEN / grain leaves, so the smallest grain stays inside the arena
    for grain in [64, 256, 1024, 4096] {
        group.bench_with_input(BenchmarkId::new("grain", grain), &grain, |b, &grain| {
            let grain = NonZero::new(grain).unwrap();
            // black_box per index rather than a shared sum, so no atomic contention muddies it
            b.iter(|| {
                scheduler.par_for((0..LEN).into(), Some(grain), |i| {
                    black_box(work(i));
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, scope_overhead, spawn_flat, spawn_nested, par_for);
criterion_main!(benches);
