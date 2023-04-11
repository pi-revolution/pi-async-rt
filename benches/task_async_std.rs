#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Sender, bounded};

use async_std::task;

#[bench]
fn async_std_block_on(b: &mut Bencher) {
    b.iter(|| task::block_on(async {}));
}

#[bench]
fn async_std_local_spawn_many(b: &mut Bencher) {
    const COUNT: usize = 10000;

    b.iter(|| {
        {
            task::block_on(async move {
                for _ in 0..COUNT {
                    let _ = task::spawn(async move {
                    });
                }
            });
        }
    });
}
