#![feature(test)]

extern crate test;

use async_std::sync::{Arc, Mutex};
use async_std::task;
use test::Bencher;

#[bench]
fn async_std_create(b: &mut Bencher) {
    b.iter(|| Arc::new(Mutex::new(())));
}

#[bench]
fn async_std_contention(b: &mut Bencher) {
    b.iter(|| task::block_on(run(10, 1000)));
}

#[bench]
fn async_std_no_contention(b: &mut Bencher) {
    b.iter(|| task::block_on(run(1, 10000)));
}

async fn run(task: usize, iter: usize) {
    let m = Arc::new(Mutex::new(()));
    let mut tasks = Vec::new();

    for _ in 0..task {
        let m = m.clone();
        tasks.push(task::spawn(async move {
            for _ in 0..iter {
                let _ = m.lock().await;
            }
        }));
    }

    for t in tasks {
        t.await;
    }
}
