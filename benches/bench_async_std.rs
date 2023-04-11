#![feature(test)]
extern crate test;

use async_std::sync::Mutex;
use async_std::task;
use std::sync::Arc;
use std::time::Duration;
use test::Bencher;

use crossbeam_channel::bounded;

#[bench]
fn async_std_bench_async_mutex(b: &mut Bencher) {
    b.iter(|| {
        let (s, r) = bounded(1);

        let shared = Arc::new(Mutex::new(0));

        for _ in 0..1 {
            let s0_copy = s.clone();
            let shared0_copy = shared.clone();
            task::spawn(async move {
                for _ in 0..500 {
                    let mut v = shared0_copy.lock_arc().await;
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
            task::spawn(async move {
                for _ in 0..500 {
                    let mut v = shared1_copy.lock_arc().await;
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
