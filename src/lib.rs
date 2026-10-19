mod arena;
mod jobs;
mod queue;
mod scheduler;
mod sync;

#[cfg(not(target_arch = "x86_64"))]
compile_error!("job-system assumes x86_64 cache geometry");

/// Two 64-byte lines: Intel's spatial prefetcher pulls adjacent pairs, so false sharing spans 128
pub(crate) const CACHE_LINE: usize = 64 * 2;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
