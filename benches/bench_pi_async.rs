#![feature(test)]
extern crate test;

use std::thread;
use std::sync::Arc;
use std::time::Duration;
use test::Bencher;

use crossbeam_channel::bounded;
use async_lock::Mutex;

use pi_async::rt::{startup_global_time_loop, AsyncRuntime,
                   multi_thread::{StealableTaskPool, MultiTaskRuntime, MultiTaskRuntimeBuilder}};

#[bench]
fn pi_async_bench_async_mutex(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let pool = StealableTaskPool::with(2,
                                       100000,
                                       [1, 254],
                                       3000);
    let rt0 = MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(2)
        .set_worker_limit(2, 2)
        .build();

    let pool = StealableTaskPool::with(2,
                                       100000,
                                       [1, 254],
                                       3000);
    let rt1 = MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(2)
        .set_worker_limit(2, 2)
        .build();

    let rt0_copy = rt0.clone();
    let rt1_copy = rt1.clone();
    let (s, r) = bounded(1);
    let shared = Arc::new(Mutex::new(0));
    b.iter(|| {
        for _ in 0..1 {
            let s0_copy = s.clone();
            let shared0_copy = shared.clone();
            rt0_copy
                .spawn(async move {
                    for _ in 0..500 {
                        let mut v = shared0_copy.lock().await;
                        if *v >= 999 {
                            *v += 1;
                            s0_copy.send(()).unwrap();
                        } else {
                            *v += 1;
                        }
                    }
                })
                .unwrap();
        }

        for _ in 0..1 {
            let s1_copy = s.clone();
            let shared1_copy = shared.clone();
            rt1_copy
                .spawn(async move {
                    for _ in 0..500 {
                        let mut v = shared1_copy.lock().await;
                        if *v >= 999 {
                            *v += 1;
                            s1_copy.send(()).unwrap();
                        } else {
                            *v += 1;
                        }
                    }
                })
                .unwrap();
        }

        if let Err(_) = r.recv_timeout(Duration::from_millis(10000)) {
            println!("!!!!!!recv timeout, len: {:?}", (rt0.len(), rt1.len()));
        }
    });
}
