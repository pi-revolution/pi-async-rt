#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{mpsc, Arc};

use crossbeam_channel::{Sender, bounded};

use tokio::runtime::{self, Runtime};
use tokio::sync::oneshot;

#[derive(Clone)]
struct AtomicCounter(Sender<()>);
impl Drop for AtomicCounter {
    fn drop(&mut self) {
        self.0.send(()); //通知执行完成
    }
}

#[bench]
fn tokio_spawn_empty_many(b: &mut Bencher) {
    let rt = Arc::new(rt());
    let (send, recv) = bounded(1);

    b.iter(move || {
        let rt0 = rt.clone();
        let rt1 = rt.clone();
        let rt2 = rt.clone();
        let rt3 = rt.clone();

        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            thread::spawn(move || {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    let _ = rt0.spawn(async move {
                        counter_copy;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    let _ = rt1.spawn(async move {
                        counter_copy;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    let _ = rt2.spawn(async move {
                        counter_copy;
                    });
                }
            });

            thread::spawn(move || {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    let _ = rt3.spawn(async move {
                        counter_copy;
                    });
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

#[bench]
fn tokio_await_empty_many(b: &mut Bencher) {
    let rt = Arc::new(rt());
    let (send, recv) = bounded(1);

    b.iter(move || {
        let rt0 = rt.clone();
        let rt1 = rt.clone();
        let rt2 = rt.clone();
        let rt3 = rt.clone();

        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            rt0.spawn(async move {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            rt1.spawn(async move {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            rt2.spawn(async move {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            rt3.spawn(async move {
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
fn tokio_spawn_many(b: &mut Bencher) {
    let rt = Arc::new(rt());
    let (send, recv) = bounded(1);

    b.iter(move || {
        let rt0 = rt.clone();
        let rt1 = rt.clone();
        let rt2 = rt.clone();
        let rt3 = rt.clone();

        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            let rt0_copy = rt0.clone();
            rt0.spawn(async move {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    let _ = rt0_copy.spawn(async move {
                        counter_copy;
                    });
                }
            });

            let rt1_copy = rt1.clone();
            rt1.spawn(async move {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    let _ = rt1_copy.spawn(async move {
                        counter_copy;
                    });
                }
            });

            let rt2_copy = rt2.clone();
            rt2.spawn(async move {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    let _ = rt2_copy.spawn(async move {
                        counter_copy;
                    });
                }
            });

            let rt3_copy = rt3.clone();
            rt3.spawn(async move {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    let _ = rt3_copy.spawn(async move {
                        counter_copy;
                    });
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

#[bench]
fn tokio_yield_many(b: &mut Bencher) {
    const NUM_YIELD: usize = 1_000;
    const TASKS: usize = 200;

    let rt = rt();

    let (tx, rx) = mpsc::sync_channel(TASKS);

    b.iter(move || {
        for _ in 0..TASKS {
            let tx = tx.clone();

            rt.spawn(async move {
                for _ in 0..NUM_YIELD {
                    tokio::task::yield_now().await;
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
fn tokio_ping_pong(b: &mut Bencher) {
    const NUM_PINGS: usize = 1_000;

    let rt = rt();

    let rem = Arc::new(AtomicUsize::new(0));

    b.iter(|| {
        let (done_tx, done_rx) = bounded(1);
        let rem = rem.clone();
        rem.store(NUM_PINGS, Relaxed);

        rt.spawn(async move {
            tokio::spawn(async move {
                for _ in 0..NUM_PINGS {
                    let rem = rem.clone();
                    let done_tx = done_tx.clone();

                    tokio::spawn(async move {
                        let (tx1, rx1) = oneshot::channel();
                        let (tx2, rx2) = oneshot::channel();

                        tokio::spawn(async move {
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
fn tokio_chained_spawn(b: &mut Bencher) {
    const ITER: usize = 1_000;

    let rt = rt();

    fn iter(done_tx: mpsc::SyncSender<()>, n: usize) {
        if n == 0 {
            done_tx.send(()).unwrap();
        } else {
            tokio::spawn(async move {
                iter(done_tx, n - 1);
            });
        }
    }

    let (done_tx, done_rx) = mpsc::sync_channel(1000);

    b.iter(move || {
        let done_tx = done_tx.clone();

        rt.block_on(async {
            tokio::spawn(async move {
                iter(done_tx, ITER);
            });

            done_rx.recv().unwrap();
        });
    });
}

#[bench]
fn tokio_spawn_one_to_one(b: &mut Bencher) {
    let rt = Arc::new(rt());
    let (send, recv) = bounded(1);

    b.iter(move || {
        let rt0 = rt.clone();
        let rt1 = rt.clone();
        let rt2 = rt.clone();
        let rt3 = rt.clone();

        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            thread::spawn(move || {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    let _ = rt0.spawn(async move {
                        let _ = tokio::spawn(async move {
                            counter_copy;
                        });
                    });
                }
            });

            thread::spawn(move || {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    let _ = rt1.spawn(async move {
                        let _ = tokio::spawn(async move {
                            counter_copy;
                        });
                    });
                }
            });

            thread::spawn(move || {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    let _ = rt2.spawn(async move {
                        let _ = tokio::spawn(async move {
                            counter_copy;
                        });
                    });
                }
            });

            thread::spawn(move || {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    let _ = rt3.spawn(async move {
                        let _ = tokio::spawn(async move {
                            counter_copy;
                        });
                    });
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

fn rt() -> Runtime {
    runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}
