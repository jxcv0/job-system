use std::{
    sync::Arc,
    thread::{JoinHandle, available_parallelism},
};

use crate::{queue, queue::Stealer};

struct Scheduler {
    threads: Vec<JoinHandle<()>>,
    stealers: Arc<[Stealer]>,
}
impl Scheduler {
    pub fn new() -> Self {
        let np = available_parallelism().unwrap().get();

        let (owners, stealers): (Vec<_>, Vec<_>) = (0..np).map(|_| queue::channel()).unzip();

        todo!()
    }
}
