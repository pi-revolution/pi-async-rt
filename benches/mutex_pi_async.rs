#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Sender, bounded};
use async_lock::Mutex;
use pi_async::{rt::{startup_global_time_loop,
                    AsyncRuntime, AsyncRuntimeExt,
                    single_thread::{SingleTaskRunner, SingleTaskRuntime},
                    multi_thread::{StealableTaskPool, MultiTaskRuntimeBuilder, MultiTaskRuntime}}};
use pi_async::rt::single_thread::SingleTaskPool;

#[derive(Clone)]
struct AtomicCounter(Sender<()>);
impl Drop for AtomicCounter {
    fn drop(&mut self) {
        self.0.send(()); //通知执行完成
    }
}

#[bench]
fn pi_async_create(b: &mut Bencher) {
    b.iter(|| Arc::new(Mutex::new(())));
}

#[bench]
fn pi_async_contention(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let rt = contention_runtime();

    b.iter(|| {
        let rt_copy = rt.clone();
        let (send, recv) = bounded(1);
        let counter = Arc::new(AtomicCounter(send));

        rt.spawn(contention_run(rt_copy, counter, 10, 1000));
        recv.recv().unwrap();
    });
}

fn contention_runtime() -> MultiTaskRuntime<(), StealableTaskPool<()>> {
    let pool = StealableTaskPool::with(4,
                                       100000,
                                       [1, 254],
                                       3000);
    MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(4)
        .set_worker_limit(4, 4)
        .build()
}

async fn contention_run(rt: MultiTaskRuntime<(), StealableTaskPool<()>>, counter: Arc<AtomicCounter>, task: usize, iter: usize) {
    let m = Arc::new(Mutex::new(()));

    for _ in 0..task {
        let m = m.clone();
        let counter_copy = counter.clone();
        let _ = rt.spawn_local(async move {
            for _ in 0..iter {
                let _ = m.lock().await;
            }
            counter_copy;
        });
    }
}

#[bench]
fn pi_async_no_contention(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let rt = no_contention_runtime();

    b.iter(|| {
        let rt_copy = rt.clone();
        let _ = rt.block_on(no_contention_run(rt_copy, 1, 10000));
    });
}

fn no_contention_runtime() -> SingleTaskRuntime {
    let pool = SingleTaskPool::new([1, 254]);
    let runner = SingleTaskRunner::new(pool);
    runner.startup().unwrap()
}

async fn no_contention_run(rt: SingleTaskRuntime, task: usize, iter: usize) {
    let m = Arc::new(Mutex::new(()));
    for _ in 0..task {
        let m = m.clone();
        let _ = rt.spawn_local( async move {
            for _ in 0..iter {
                let _ = m.lock().await;
            }
        });
    }
}
