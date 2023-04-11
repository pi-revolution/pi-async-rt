#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll};
use std::time::Duration;
use futures::channel::oneshot;
use futures::Future;
use crossbeam_channel::{Sender, bounded};
use pi_async::rt::{startup_global_time_loop,
                   AsyncRuntime, AsyncRuntimeExt, TaskId,
                   multi_thread::{StealableTaskPool, MultiTaskRuntime, MultiTaskRuntimeBuilder}};

#[derive(Clone)]
struct AtomicCounter(Sender<()>);
impl Drop for AtomicCounter {
    fn drop(&mut self) {
        self.0.send(()); //通知执行完成
    }
}

fn rt(weights: [u8; 2]) -> MultiTaskRuntime<(), StealableTaskPool<()>> {
    let pool = StealableTaskPool::with(4,
                                       1000000,
                                       weights,
                                       3000);
    MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(4)
        .set_worker_limit(4, 4)
        .build()
}

#[bench]
fn pi_async_spawn_empty_many(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let rt = rt([254, 1]);
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
                    if let Err(e) = rt0.spawn(async move {
                        counter_copy;
                    }) {
                        panic!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });

            thread::spawn(move || {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    if let Err(e) = rt1.spawn(async move {
                        counter_copy;
                    }) {
                        panic!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });

            thread::spawn(move || {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    if let Err(e) = rt2.spawn(async move {
                        counter_copy;
                    }) {
                        panic!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });

            thread::spawn(move || {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    if let Err(e) = rt3.spawn(async move {
                        counter_copy;
                    }) {
                        println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

#[bench]
fn pi_async_await_empty_many(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let rt = rt([254, 1]);
    let (send, recv) = bounded(1);

    b.iter(move || {
        {
            let counter = Arc::new(AtomicCounter(send.clone()));
            let counter0 = counter.clone();
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let counter3 = counter.clone();

            rt.spawn(async move {
                for _ in 0..2500 {
                    let counter_copy = counter0.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            rt.spawn(async move {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            rt.spawn(async move {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    async move {
                        counter_copy;
                    }.await;
                }
            });

            rt.spawn(async move {
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
fn pi_async_spawn_many(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));


    let rt = rt([1, 254]);
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
                    rt0_copy.spawn_local(async move {
                        counter_copy;
                    });
                }
            });

            let rt1_copy = rt1.clone();
            rt1.spawn(async move {
                for _ in 2500..5000 {
                    let counter_copy = counter1.clone();
                    rt1_copy.spawn_local(async move {
                        counter_copy;
                    });
                }
            });

            let rt2_copy = rt2.clone();
            rt2.spawn(async move {
                for _ in 5000..7500 {
                    let counter_copy = counter2.clone();
                    rt2_copy.spawn_local(async move {
                        counter_copy;
                    });
                }
            });

            let rt3_copy = rt3.clone();
            rt3.spawn(async move {
                for _ in 7500..10000 {
                    let counter_copy = counter3.clone();
                    rt3_copy.spawn_local(async move {
                        counter_copy;
                    });
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}

#[bench]
fn pi_async_yield_many(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let rt = rt([1, 254]);
    const NUM_YIELD: usize = 1_000;
    const TASKS: usize = 200;

    b.iter(move || {
        let (send, recv) = bounded(1);

        {
            let counter = Arc::new(AtomicCounter(send));
            for _ in 0..TASKS {
                let rt_copy = rt.clone();
                let counter_copy = counter.clone();
                let _ = rt.spawn(async move {
                    for _ in 0..NUM_YIELD {
                        rt_copy.yield_now().await;
                    }
                    counter_copy;
                });
            }
        }

        let _ = recv.recv().unwrap();
    });
}

#[bench]
fn pi_async_ping_pong(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    const NUM_PINGS: usize = 1_000;
    let rt = rt([1, 254]);
    let rem = Arc::new(AtomicUsize::new(0));

    b.iter(|| {
        let (send, recv) = bounded(1);
        let rem = rem.clone();
        rem.store(NUM_PINGS, Relaxed);

        let rt_copy = rt.clone();
        let _ = rt.spawn(async move {
            let rt_clone = rt_copy.clone();
            let _ = rt_copy.spawn_local(async move {
                for _ in 0..NUM_PINGS {
                    let rem = rem.clone();
                    let send = send.clone();
                    let rt_clone_ = rt_clone.clone();
                    let _ = rt_clone.spawn_local(async move {
                        let (tx1, rx1) = oneshot::channel();
                        let (tx2, rx2) = oneshot::channel();

                        rt_clone_.spawn_local(async move {
                            rx1.await.unwrap();
                            tx2.send(()).unwrap();
                        });

                        tx1.send(()).unwrap();
                        rx2.await.unwrap();

                        if 1 == rem.fetch_sub(1, Relaxed) {
                            send.send(()).unwrap();
                        }
                    });
                }
            });
        });

        let _ = recv.recv().unwrap();
    });
}

#[bench]
fn pi_async_chained_spawn(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    const ITER: usize = 1_000;
    let rt = rt([1, 254]);
    let (send, recv) = bounded(1);

    fn iter(rt: MultiTaskRuntime<(), StealableTaskPool<()>>,
            send: Sender<()>,
            n: usize) {
        if n == 0 {
            send.send(()).unwrap();
        } else {
            let rt_copy = rt.clone();
            let _ = rt.spawn_priority(10, async move {
                iter(rt_copy.clone(), send, n - 1);
            });
        }
    }

    b.iter(move || {
        let rt_copy = rt.clone();
        let send_copy = send.clone();

        rt.spawn(async move {
            let rt_clone = rt_copy.clone();
            let send_clone = send_copy.clone();
            let _ = rt_copy.spawn_priority(10, async move {
                iter(rt_clone, send_clone, ITER);
            });
        });

        let _ = recv.recv().unwrap();
    });
}

#[bench]
fn pi_async_spawn_one_to_one(b: &mut Bencher) {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let rt = rt([1, 1]);
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
                    let rt0_copy = rt0.clone();
                    let counter_copy = counter0.clone();
                    if let Err(e) = rt0.spawn(async move {
                        let _ = rt0_copy.spawn_local(async move {
                            counter_copy;
                        });
                    }) {
                        panic!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });

            thread::spawn(move || {
                for _ in 2500..5000 {
                    let rt1_copy = rt1.clone();
                    let counter_copy = counter1.clone();
                    if let Err(e) = rt1.spawn(async move {
                        let _ = rt1_copy.spawn_local(async move {
                            counter_copy;
                        });
                    }) {
                        panic!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });

            thread::spawn(move || {
                for _ in 5000..7500 {
                    let rt2_copy = rt2.clone();
                    let counter_copy = counter2.clone();
                    if let Err(e) = rt2.spawn(async move {
                        let _ = rt2_copy.spawn_local(async move {
                            counter_copy;
                        });;
                    }) {
                        panic!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });

            thread::spawn(move || {
                for _ in 7500..10000 {
                    let rt3_copy = rt3.clone();
                    let counter_copy = counter3.clone();
                    if let Err(e) = rt3.spawn(async move {
                        let _ = rt3_copy.spawn_local(async move {
                            counter_copy;
                        });
                    }) {
                        println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                    }
                }
            });
        }

        let _ = recv.clone().recv().unwrap();
    });
}
