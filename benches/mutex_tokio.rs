#![feature(test)]

extern crate test;

use std::sync::Arc;

use test::Bencher;
use tokio::sync::Mutex;

#[bench]
fn tokio_create(b: &mut Bencher) {
    b.iter(|| Arc::new(Mutex::new(())));
}

#[bench]
fn tokio_contention(b: &mut Bencher) {
    let rt = Arc::new(contention_rt());
    b.iter(|| {
        let rt_copy = rt.clone();
        rt.block_on(run(rt_copy, 10, 1000))
    });
}

fn contention_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap()
}

#[bench]
fn tokio_no_contention(b: &mut Bencher) {
    let rt = Arc::new(no_contention_rt());

    b.iter(|| {
        let rt_copy = rt.clone();
        rt.block_on(run(rt_copy, 1, 10000))
    });
}

fn no_contention_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

async fn run(rt: Arc<tokio::runtime::Runtime>, task: usize, iter: usize) {
    let m = Arc::new(Mutex::new(()));
    let mut tasks = Vec::new();

    for _ in 0..task {
        let m = m.clone();
        tasks.push(rt.spawn(async move {
            for _ in 0..iter {
                let _ = m.lock().await;
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }
}
