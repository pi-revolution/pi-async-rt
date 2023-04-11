#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Sender, bounded};
use futures_util::TryFutureExt;

use pi_async::rt::{AsyncRuntime, AsyncRuntimeExt,
                   serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}};

#[bench]
fn pi_async_block_on(b: &mut Bencher) {
    let rt = LocalTaskRunner::<()>::new().into_local();

    b.iter(|| {
        let _ = rt.block_on(async move {});
    })
}

#[derive(Clone)]
struct AtomicCounter(Sender<()>);
impl Drop for AtomicCounter {
    fn drop(&mut self) {
        self.0.send(()); //通知执行完成
    }
}

fn rt() -> LocalTaskRuntime {
    LocalTaskRunner::new().startup("Local-RT", 2 * 1024 * 1024)
}

#[bench]
fn pi_async_local_spawn_many(b: &mut Bencher) {
    // use tracing_chrome::ChromeLayerBuilder;
    // use tracing_subscriber::{registry::Registry, prelude::*};
    //
    // let (chrome_layer, _guard) = ChromeLayerBuilder::new().build();
    // tracing_subscriber::registry().with(chrome_layer).init();

    thread::sleep(Duration::from_millis(10000));

    const COUNT: usize = 10000;

    let rt = LocalTaskRunner::<()>::new().into_local();

    b.iter(|| {
        let rt_copy = rt.clone();
        rt.block_on(async move {
            for _ in 0..COUNT {
                let _ = rt_copy.spawn(async move {});
            }
        });
    });
}

#[bench]
fn pi_async_local_send_many(b: &mut Bencher) {
    thread::sleep(Duration::from_millis(10000));

    const COUNT: usize = 10000;

    let rt = rt();
    let (sender, receiver) = bounded(1);

    b.iter(|| {
        for _ in 0..COUNT {
            let _ = rt.send(async move {

            });
        }

        let sender_copy = sender.clone();
        let _ = rt.send(async move {
            sender_copy.send(());
        });
        let _ = receiver.recv().unwrap();
    });
}

#[bench]
fn pi_async_local_run(b: &mut Bencher) {
    thread::sleep(Duration::from_millis(10000));

    const COUNT: usize = 10000;

    let runner = LocalTaskRunner::new();
    let rt = runner.get_runtime();

    b.iter(|| {
        for _ in 0..COUNT {
            rt.spawn(async move {});
            runner.run_once();
        }
    });
}
