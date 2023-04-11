#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{mpsc, Arc};

use async_std::task;
use futures::channel::oneshot;
use crossbeam_channel::{Sender, bounded};

#[derive(Clone)]
struct AtomicCounter(Sender<()>);
impl Drop for AtomicCounter {
    fn drop(&mut self) {
        self.0.send(()); //通知执行完成
    }
}

#[bench]
fn async_std_spawn_empty_many(b: &mut Bencher) {
    let (send, recv) = bounded(1);

    b.iter(move || {
        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            thread::spawn(move || {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

#[bench]
fn async_std_await_empty_many(b: &mut Bencher) {
    let (send, recv) = bounded(1);

    b.iter(move || {
        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            task::spawn(async move {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            task::spawn(async move {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            task::spawn(async move {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            task::spawn(async move {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

#[bench]
fn async_std_spawn_many(b: &mut Bencher) {
    let (send, recv) = bounded(1);

    b.iter(move || {
        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            task::spawn(async move {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });

            task::spawn(async move {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });

            task::spawn(async move {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });

            task::spawn(async move {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    let _ = task::spawn(async move {
                        counter_copy;
                    });
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

#[bench]
fn async_std_yield_many(b: &mut Bencher) {
    const NUM_YIELD: usize = 1_000;
    const TASKS: usize = 200;

    let (tx, rx) = mpsc::sync_channel(TASKS);

    b.iter(move || {
        for _ in 0..TASKS {
            let tx = tx.clone();

            task::spawn(async move {
                for _ in 0..NUM_YIELD {
                    task::yield_now().await;
                }

                tx.send(()).unwrap();
            });
        }

        for _ in 0..TASKS {
            let _ = rx.recv().unwrap();
        }
    });
}

#[bench]
fn async_std_ping_pong(b: &mut Bencher) {
    const NUM_PINGS: usize = 1_000;

    let rem = Arc::new(AtomicUsize::new(0));

    b.iter(|| {
        let (done_tx, done_rx) = bounded(1);
        let rem = rem.clone();
        rem.store(NUM_PINGS, Relaxed);

        task::spawn(async {
            task::spawn(async move {
                for _ in 0..NUM_PINGS {
                    let rem = rem.clone();
                    let done_tx = done_tx.clone();

                    task::spawn(async move {
                        let (tx1, rx1) = oneshot::channel();
                        let (tx2, rx2) = oneshot::channel();

                        task::spawn(async move {
                            rx1.await.unwrap();
                            tx2.send(()).unwrap();
                        });

                        tx1.send(()).unwrap();
                        rx2.await.unwrap();

                        if 1 == rem.fetch_sub(1, Relaxed) {
                            done_tx.send(()).unwrap();
                        }
                    });
                }
            });
        });
        let _ = done_rx.recv().unwrap();
    });
}

#[bench]
fn async_std_chained_spawn(b: &mut Bencher) {
    const ITER: usize = 1_000;

    fn iter(done_tx: mpsc::SyncSender<()>, n: usize) {
        if n == 0 {
            done_tx.send(()).unwrap();
        } else {
            task::spawn(async move {
                iter(done_tx, n - 1);
            });
        }
    }

    let (done_tx, done_rx) = mpsc::sync_channel(1000);

    b.iter(move || {
        let done_tx = done_tx.clone();

        task::block_on(async {
            task::spawn(async move {
                iter(done_tx, ITER);
            });

            done_rx.recv().unwrap();
        });
    });
}

#[bench]
fn async_std_spawn_one_to_one(b: &mut Bencher) {
    let (send, recv) = bounded(1);

    b.iter(move || {
        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            thread::spawn(move || {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    let _ = task::spawn(async move {
                        let _ = task::spawn(async move {
                            counter_copy;
                        });
                    });
                }
            });

            thread::spawn(move || {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    let _ = task::spawn(async move {
                        let _ = task::spawn(async move {
                            counter_copy;
                        });;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    let _ = task::spawn(async move {
                        let _ = task::spawn(async move {
                            counter_copy;
                        });;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    let _ = task::spawn(async move {
                        let _ = task::spawn(async move {
                            counter_copy;
                        });;
                    });
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}
