#![feature(test)]
extern crate test;

use std::sync::Arc;
use std::time::Duration;
use test::Bencher;

use crossbeam_channel::bounded;
use tokio::{
    runtime::{self, Runtime},
    sync::Mutex,
};

fn rt() -> Runtime {
    runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

#[bench]
fn tokio_bench_async_mutex(b: &mut Bencher) {
    let rt0 = Arc::new(rt());

    let rt1 = Arc::new(rt());

    b.iter(|| {
        let rt0_copy = rt0.clone();
        let rt1_copy = rt1.clone();

        let (s, r) = bounded(1);

        let shared = Arc::new(Mutex::new(0));

        for _ in 0..1 {
            let s0_copy = s.clone();
            let shared0_copy = shared.clone();
            rt0_copy.spawn(async move {
                for _ in 0..500 {
                    let mut v = shared0_copy.lock().await;
                    if *v >= 999 {
                        *v += 1;
                        s0_copy.send(()).unwrap();
                    } else {
                        *v += 1;
                    }
                }
            });
        }

        for _ in 0..1 {
            let s1_copy = s.clone();
            let shared1_copy = shared.clone();
            rt1_copy.spawn(async move {
                for _ in 0..500 {
                    let mut v = shared1_copy.lock().await;
                    if *v >= 999 {
                        *v += 1;
                        s1_copy.send(()).unwrap();
                    } else {
                        *v += 1;
                    }
                }
            });
        }

        if let Err(_) = r.recv_timeout(Duration::from_millis(10000)) {
            // println!("!!!!!!recv timeout, len: {:?}", (rt0.len(), rt1.len()));
        }
    });
}
