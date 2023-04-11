#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Sender, bounded};

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap()
}

#[bench]
fn tokio_block_on(b: &mut Bencher) {
    let rt = rt();
    b.iter(|| rt.block_on(async {}));
}

#[bench]
fn tokio_local_spawn_many(b: &mut Bencher) {
    let rt = rt();

    const COUNT: usize = 10000;

    b.iter(|| {
        rt.block_on(async move {
            for _ in 0..COUNT {
                let _ = tokio::spawn(async move {});
            }
        });
    });
}
