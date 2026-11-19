//! Parking for idle workers.
//!
//! Each worker thread has a bit in `parked`. A worker that finds no work for a while sets its bit,
//! checks the queues one last time, and then parks until its bit is cleared. Anything that pushes
//! work checks `parked` after the push, and if a bit is set it clears it and unparks that worker.
//! Clearing the bit claims the sleeper, so later pushes don't wake it again while it is still
//! getting up. While nobody is parked the check is a fence and one load, so the push path stays
//! cheap.
//!
//! The race to avoid is a lost wakeup: a worker checks the queues and finds them empty, a job is
//! pushed, the pusher sees no sleepers, and then the worker parks with work queued. The worker sets
//! its bit before its last check of the queues, and the pusher reads the bits after its push, with
//! a `SeqCst` fence on both sides. So either the worker's last check sees the job, or the pusher
//! sees the bit. `thread::park` keeps an unpark that lands before the park, so a wake between the
//! last check and the park is not lost either.

use std::{
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering, fence},
    },
    thread::{self, Thread},
};

pub(crate) struct Sleep {
    /// One bit per worker, set while it is parked or about to park
    parked: Box<[AtomicU64]>,
    /// Each worker's thread, registered when it starts so a waker can unpark it
    threads: Box<[OnceLock<Thread>]>,
}

fn word_and_bit(worker: usize) -> (usize, u64) {
    (worker / 64, 1 << (worker % 64))
}

impl Sleep {
    pub(crate) fn new(workers: usize) -> Self {
        Self {
            parked: (0..workers.div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
            threads: (0..workers).map(|_| OnceLock::new()).collect(),
        }
    }

    /// Record the calling thread as `worker`. Must be called on that worker's thread before it
    /// first calls [`Sleep::prepare`]
    pub(crate) fn register(&self, worker: usize) {
        self.threads[worker]
            .set(thread::current())
            .expect("worker registered twice");
    }

    /// Announce that `worker` is about to park. The caller must then check for work one last time,
    /// and finish with either [`Sleep::cancel`] if it found some or [`Sleep::wait`]
    pub(crate) fn prepare(&self, worker: usize) {
        let (word, bit) = word_and_bit(worker);
        self.parked[word].fetch_or(bit, Ordering::SeqCst);
        // Pairs with the fence in `wake_one`: the caller's next check of the queues must not be
        // ordered before the bit is set
        fence(Ordering::SeqCst);
    }

    /// The last check found work, so stay awake. If a waker already claimed this worker, its
    /// unpark is left over and only makes the next park return early
    pub(crate) fn cancel(&self, worker: usize) {
        let (word, bit) = word_and_bit(worker);
        self.parked[word].fetch_and(!bit, Ordering::Relaxed);
    }

    /// Park until a waker claims `worker` or `stop` is set
    pub(crate) fn wait(&self, worker: usize, stop: &AtomicBool) {
        let (word, bit) = word_and_bit(worker);
        while self.parked[word].load(Ordering::Acquire) & bit != 0 && !stop.load(Ordering::Relaxed)
        {
            thread::park();
        }
        // Stopping leaves the bit set; clear it so the count stays right
        self.parked[word].fetch_and(!bit, Ordering::Relaxed);
    }

    /// Wake one parked worker, if there is any. Call after pushing work
    #[inline]
    pub(crate) fn wake_one(&self) {
        // Pairs with the fence in `prepare`: the push must not be ordered after this load
        fence(Ordering::SeqCst);
        for (index, word) in self.parked.iter().enumerate() {
            if word.load(Ordering::Relaxed) != 0 && self.claim_and_unpark(index, word) {
                return;
            }
        }
    }

    /// Claim and unpark one worker whose bit is set in `word`. Returns false if every bit was
    /// cleared before it could claim one
    #[cold]
    fn claim_and_unpark(&self, index: usize, word: &AtomicU64) -> bool {
        let mut bits = word.load(Ordering::Relaxed);
        while bits != 0 {
            let bit = bits.isolate_lowest_one();
            let before = word.fetch_and(!bit, Ordering::AcqRel);
            if before & bit != 0 {
                let worker = index * 64 + bit.trailing_zeros() as usize;
                self.threads[worker]
                    .get()
                    .expect("a parked worker registered its thread")
                    .unpark();
                return true;
            }
            // Another waker or a cancel cleared it first
            bits = before & !bit;
        }
        false
    }

    /// Wake every parked worker, such as when the scheduler stops. `stop` must already be set.
    ///
    /// This claims every parked bit, the same handshake as [`Sleep::wake_one`], rather than
    /// unparking each registered thread: claiming a bit makes that worker's registration visible
    /// here, and the fence means a worker that parks after this sees `stop` in [`Sleep::wait`]
    pub(crate) fn wake_all(&self) {
        // Pairs with the fence in `prepare`: either a worker going to sleep sees `stop`, or its
        // bit is set by the time of the swap below
        fence(Ordering::SeqCst);
        for (index, word) in self.parked.iter().enumerate() {
            let mut bits = word.swap(0, Ordering::AcqRel);
            while bits != 0 {
                let worker = index * 64 + bits.trailing_zeros() as usize;
                self.threads[worker]
                    .get()
                    .expect("a parked worker registered its thread")
                    .unpark();
                bits &= bits - 1;
            }
        }
    }

    /// How many workers are parked or about to park
    #[cfg(test)]
    pub(crate) fn parked(&self) -> usize {
        self.parked
            .iter()
            .map(|word| word.load(Ordering::Relaxed).count_ones() as usize)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, mpsc},
        time::Duration,
    };

    const WAIT: Duration = Duration::from_secs(if cfg!(miri) { 120 } else { 2 });

    // Parks a thread as `worker` and returns a receiver that gets a message when it wakes
    fn park(sleep: &Arc<Sleep>, stop: &Arc<AtomicBool>, worker: usize) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel();
        let (sleep, stop) = (sleep.clone(), stop.clone());
        thread::spawn(move || {
            sleep.register(worker);
            sleep.prepare(worker);
            sleep.wait(worker, &stop);
            tx.send(()).unwrap();
        });
        rx
    }

    fn wait_for_parked(sleep: &Sleep, n: usize) {
        while sleep.parked() != n {
            thread::yield_now();
        }
    }

    #[test]
    fn wake_one_wakes_a_single_parked_worker() {
        let sleep = Arc::new(Sleep::new(2));
        let stop = Arc::new(AtomicBool::new(false));
        let woke = [park(&sleep, &stop, 0), park(&sleep, &stop, 1)];
        wait_for_parked(&sleep, 2);

        sleep.wake_one();

        let first = loop {
            if let Some(i) = woke.iter().position(|rx| rx.try_recv().is_ok()) {
                break i;
            }
            thread::yield_now();
        };
        assert_eq!(sleep.parked(), 1, "wake_one claimed more than one worker");

        stop.store(true, Ordering::Relaxed);
        sleep.wake_all();
        woke[1 - first]
            .recv_timeout(WAIT)
            .expect("worker never woke");
    }

    #[test]
    fn wake_between_prepare_and_wait_is_not_lost() {
        let sleep = Sleep::new(1);
        sleep.register(0);
        sleep.prepare(0);
        sleep.wake_one();
        // Returns at once since the bit was claimed; a lost wake would block forever
        sleep.wait(0, &AtomicBool::new(false));
    }

    #[test]
    fn wait_returns_at_once_when_stopped() {
        let sleep = Sleep::new(1);
        sleep.register(0);
        sleep.prepare(0);
        sleep.wait(0, &AtomicBool::new(true));
        assert_eq!(sleep.parked(), 0);
    }

    #[test]
    fn wake_all_wakes_every_parked_worker() {
        let sleep = Arc::new(Sleep::new(3));
        let stop = Arc::new(AtomicBool::new(false));
        let woke: Vec<_> = (0..3).map(|w| park(&sleep, &stop, w)).collect();
        wait_for_parked(&sleep, 3);

        stop.store(true, Ordering::Relaxed);
        sleep.wake_all();

        for rx in woke {
            rx.recv_timeout(WAIT).expect("worker never woke");
        }
    }

    #[test]
    fn cancel_withdraws_the_worker() {
        let sleep = Sleep::new(1);
        sleep.prepare(0);
        sleep.cancel(0);
        assert_eq!(sleep.parked(), 0);
    }

    #[test]
    fn workers_past_the_first_64_park_and_wake() {
        let sleep = Sleep::new(65);
        sleep.register(64);
        sleep.prepare(64);
        assert_eq!(sleep.parked(), 1);
        sleep.wake_one();
        sleep.wait(64, &AtomicBool::new(false));
        assert_eq!(sleep.parked(), 0);
    }
}
