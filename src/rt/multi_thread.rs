//! # 多线程运行时
//!
//! - [ComputationalTaskPool]\: 计算型的多线程任务池，适合用于Cpu密集型的应用，
//!   不支持运行时伸缩
//! - [StealableTaskPool]\:
//!   可窃取的多线程任务池，适合用于block较多的应用，支持运行时伸缩
//! - [MultiTaskRuntime]\: 异步多线程任务运行时，支持运行时线程伸缩
//! - [MultiTaskRuntimeBuilder]\: 异步多线程任务运行时构建器
//!
//! [ComputationalTaskPool]: struct.ComputationalTaskPool.html
//! [StealableTaskPool]: struct.StealableTaskPool.html
//! [MultiTaskRuntime]: struct.MultiTaskRuntime.html
//! [MultiTaskRuntimeBuilder]: struct.MultiTaskRuntimeBuilder.html
//!
//! # Examples
//!
//! ```
//! use pi_async::prelude::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool};
//! use pi_async::rt::AsyncRuntimeExt;
//!
//! let pool = StealableTaskPool::with(4,100000,[1, 254],3000);
//! let builer = MultiTaskRuntimeBuilder::new(pool)
//!     .set_timer_interval(1)
//!     .init_worker_size(4)
//!     .set_worker_limit(4, 4);
//! let rt = builer.build();
//! let _ = rt.spawn(async move {});
//! ```

use std::any::Any;
use std::cell::UnsafeCell;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::marker::PhantomData;
use std::mem::transmute;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Waker};
use std::thread::{self, Builder};
use std::time::Duration;
use std::vec::IntoIter;

use async_stream::stream;
use crossbeam_channel::{bounded, unbounded, Sender};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_queue::{ArrayQueue, SegQueue};
use crossbeam_utils::atomic::AtomicCell;
use st3::{StealError, fifo::{Worker as FIFOWorker, Stealer as FIFOStealer}, lifo::Worker as LIFOWorker};
use flume::bounded as async_bounded;
use futures::{
    future::{BoxFuture, FutureExt},
    stream::{BoxStream, Stream, StreamExt},
    task::{waker_ref, ArcWake},
    TryFuture,
};
use parking_lot::{Condvar, Mutex, RwLock};
use rand::{Rng, thread_rng};
use num_cpus;
use wrr::IWRRSelector;
use quanta::{Clock, Instant as QInstant};
use pi_time::Instant;
use tracing::Instrument;
use log::{debug, warn};
use tokio::time::interval;
use tracing_subscriber::filter::combinator::Or;

use super::{
    PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME, PI_ASYNC_THREAD_LOCAL_ID, DEFAULT_MAX_HIGH_PRIORITY_BOUNDED, DEFAULT_HIGH_PRIORITY_BOUNDED, DEFAULT_MAX_LOW_PRIORITY_BOUNDED, alloc_rt_uid, local_async_runtime, AsyncMapReduce, AsyncPipelineResult, AsyncRuntime,
    AsyncRuntimeExt, AsyncTask, AsyncTaskPool, AsyncTaskPoolExt, AsyncTaskTimerByNotCancel, AsyncTimingTask,
    AsyncWait, AsyncWaitAny, AsyncWaitAnyCallback, AsyncWaitTimeout, LocalAsyncRuntime, TaskId, TaskHandle, YieldNow
};

/*
* 默认的初始工作者数量
*/
#[cfg(not(target_arch = "wasm32"))]
const DEFAULT_INIT_WORKER_SIZE: usize = 2;
#[cfg(target_arch = "wasm32")]
const DEFAULT_INIT_WORKER_SIZE: usize = 1;

/*
* 默认的工作者线程名称前缀
*/
const DEFAULT_WORKER_THREAD_PREFIX: &str = "Default-Multi-RT";

/*
* 默认的线程栈大小
*/
const DEFAULT_THREAD_STACK_SIZE: usize = 1024 * 1024;

/*
* 默认的工作者线程空闲休眠时长，单位ms
*/
const DEFAULT_WORKER_THREAD_SLEEP_TIME: u64 = 10;

/*
* 默认的运行时空闲休眠时长，单位ms，运行时空闲是指绑定当前运行时的队列为空，且定时器内未到期的任务为空
*/
const DEFAULT_RUNTIME_SLEEP_TIME: u64 = 1000;

/*
* 默认的最大权重
*/
const DEFAULT_MAX_WEIGHT: u8 = 254;

/*
* 默认的最小权重
*/
const DEFAULT_MIN_WEIGHT: u8 = 1;

///
/// 计算型的工作者任务队列
///
struct ComputationalTaskQueue<O: Default + 'static> {
    stack: Worker<Arc<AsyncTask<ComputationalTaskPool<O>, O>>>,     //工作者任务栈
    queue: SegQueue<Arc<AsyncTask<ComputationalTaskPool<O>, O>>>,   //工作者任务队列
    thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>,            //工作者线程的唤醒器
}

impl<O: Default + 'static> ComputationalTaskQueue<O> {
    //构建计算型的工作者任务队列
    pub fn new(thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>) -> Self {
        let stack = Worker::new_lifo();
        let queue = SegQueue::new();

        ComputationalTaskQueue {
            stack,
            queue,
            thread_waker,
        }
    }

    //获取计算型的工作者任务队列的任务数量
    pub fn len(&self) -> usize {
        self.stack.len() + self.queue.len()
    }
}

///
/// 计算型的多线程任务池，适合用于Cpu密集型的应用，不支持运行时伸缩
///
pub struct ComputationalTaskPool<O: Default + 'static> {
    workers: Vec<ComputationalTaskQueue<O>>, //工作者的任务队列列表
    waits: Option<Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>>, //待唤醒的工作者唤醒器队列
    consume_count: Arc<AtomicUsize>,                                       //任务消费计数
    produce_count: Arc<AtomicUsize>,                                       //任务生产计数
}

unsafe impl<O: Default + 'static> Send for ComputationalTaskPool<O> {}
unsafe impl<O: Default + 'static> Sync for ComputationalTaskPool<O> {}

impl<O: Default + 'static> Default for ComputationalTaskPool<O> {
    fn default() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let core_len = num_cpus::get(); //工作者任务池数据等于本机逻辑核数
        #[cfg(target_arch = "wasm32")]
        let core_len = 1; //工作者任务池数据等于1
        ComputationalTaskPool::new(core_len)
    }
}

impl<O: Default + 'static> AsyncTaskPool<O> for ComputationalTaskPool<O> {
    type Pool = ComputationalTaskPool<O>;

    #[inline]
    fn get_thread_id(&self) -> usize {
        match PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| unsafe { *thread_id.get() }) {
            Err(e) => {
                //不应该执行到这个分支
                panic!(
                    "Get thread id failed, thread: {:?}, reason: {:?}",
                    thread::current(),
                    e
                );
            }
            Ok(id) => id,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        if let Some(len) = self
            .produce_count
            .load(Ordering::Relaxed)
            .checked_sub(self.consume_count.load(Ordering::Relaxed))
        {
            len
        } else {
            0
        }
    }

    #[inline]
    fn push(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        let index = self.produce_count.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[index].queue.push(task);
        Ok(())
    }

    #[inline]
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        let id = self.get_thread_id();
        let rt_uid = task.owner();
        if (id >> 32) == rt_uid {
            //当前是运行时所在线程
            let worker = &self.workers[id & 0xffffffff];
            worker.queue.push(task);

            self.produce_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            //当前不是运行时所在线程
            self.push(task)
        }
    }

    #[inline]
    fn push_priority(&self,
                     priority: usize,
                     task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        if priority >= DEFAULT_MAX_HIGH_PRIORITY_BOUNDED {
            //最高优先级
            let id = self.get_thread_id();
            let rt_uid = task.owner();
            if (id >> 32) == rt_uid {
                let worker = &self.workers[id & 0xffffffff];
                worker.stack.push(task);

                self.produce_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
                self.push(task)
            }
        } else if priority >= DEFAULT_HIGH_PRIORITY_BOUNDED {
            //高优先级
            self.push_local(task)
        } else {
            //低优先级
            self.push(task)
        }
    }

    #[inline]
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.push_priority(DEFAULT_HIGH_PRIORITY_BOUNDED, task)
    }

    #[inline]
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>> {
        let id = self.get_thread_id() & 0xffffffff;
        let worker = &self.workers[id];
        let task = worker.stack.pop();
        if task.is_some() {
            //指定工作者的任务栈有任务，则立即返回任务
            self.consume_count.fetch_add(1, Ordering::Relaxed);
            return task;
        }

        let task = worker.queue.pop();
        if task.is_some() {
            self.consume_count.fetch_add(1, Ordering::Relaxed);
        }

        task
    }

    #[inline]
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>> {
        let mut tasks = Vec::with_capacity(self.len());
        while let Some(task) = self.try_pop() {
            tasks.push(task);
        }

        tasks.into_iter()
    }

    #[inline]
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        //多线程任务运行时不支持此方法
        None
    }
}

impl<O: Default + 'static> AsyncTaskPoolExt<O> for ComputationalTaskPool<O> {
    #[inline]
    fn set_waits(&mut self, waits: Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>) {
        self.waits = Some(waits);
    }

    #[inline]
    fn get_waits(&self) -> Option<&Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>> {
        self.waits.as_ref()
    }

    #[inline]
    fn worker_len(&self) -> usize {
        self.workers.len()
    }

    #[inline]
    fn clone_thread_waker(&self) -> Option<Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        let worker = &self.workers[self.get_thread_id() & 0xffffffff];
        Some(worker.thread_waker.clone())
    }
}

impl<O: Default + 'static> ComputationalTaskPool<O> {
    //构建指定数量的工作者的计算型的多线程任务池
    pub fn new(mut size: usize) -> Self {
        if size < DEFAULT_INIT_WORKER_SIZE {
            //工作者数量过少，则设置为默认的工作者数量
            size = DEFAULT_INIT_WORKER_SIZE;
        }

        let mut workers = Vec::with_capacity(size);
        for _ in 0..size {
            let thread_waker = Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new()));
            let worker = ComputationalTaskQueue::new(thread_waker);
            workers.push(worker);
        }
        let consume_count = Arc::new(AtomicUsize::new(0));
        let produce_count = Arc::new(AtomicUsize::new(0));

        ComputationalTaskPool {
            workers,
            waits: None,
            consume_count,
            produce_count,
        }
    }
}

///
/// 可窃取的混合任务队列
///
struct StealableTaskQueue<O: Default + 'static> {
    stack:          UnsafeCell<Option<Arc<AsyncTask<StealableTaskPool<O>, O>>>>,    //工作者任务栈
    internal:       FIFOWorker<Arc<AsyncTask<StealableTaskPool<O>, O>>>,            //工作者本地内部任务队列，可窃取
    external:       Worker<Arc<AsyncTask<StealableTaskPool<O>, O>>>,                //工作者本地外部任务队列，可窃取
    selector:       UnsafeCell<IWRRSelector<2>>,                                    //工作者任务队列选择器
    thread_waker:   Arc<(AtomicBool, Mutex<()>, Condvar)>,                          //工作者线程的唤醒器
}

impl<O: Default + 'static> StealableTaskQueue<O> {
    // 构建可窃取的混合任务队列，允许设置初始的栈和队列的初始容量，并自动设置栈和队列的容量
    // 栈和队列的容量是初始容量的最小二次方，例如初始容量为0，则容量为1
    pub fn new(
        init_queue_capacity: usize,
        thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>,
    ) -> (Self,
          FIFOStealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>,
          Stealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>) {
        let stack = UnsafeCell::new(None);
        let internal = FIFOWorker::new(init_queue_capacity);
        let external = Worker::new_fifo();
        let internal_stealer = internal.stealer();
        let external_stealer = external.stealer();
        let selector = UnsafeCell::new(IWRRSelector::new([2, 1]));

        (
            StealableTaskQueue {
                stack,
                internal,
                external,
                selector,
                thread_waker,
            },
            internal_stealer,
            external_stealer
        )
    }

    // 获取栈容量
    pub const fn stack_capacity(&self) -> usize {
        1
    }

    // 获取本地内部任务队列容量
    pub fn internal_capacity(&self) -> usize {
        self.internal.capacity()
    }

    // 获取剩余的本地内部任务队列容量，不准确
    pub fn remaining_internal_capacity(&self) -> usize {
        self.internal.spare_capacity()
    }

    // 获取栈的长度
    #[inline]
    pub fn stack_len(&self) -> usize {
        unsafe {
            if (&*self.stack.get()).is_some() {
                1
            } else {
                0
            }
        }
    }

    // 获取本地内部任务队列长度
    pub fn internal_len(&self) -> usize {
        self
            .internal_capacity()
            .checked_sub(self.remaining_internal_capacity())
            .unwrap_or(0)
    }

    // 获取本地外部任务队列长度
    pub fn external_len(&self) -> usize {
        self.external.len()
    }
}

///
/// 可窃取的混合任务池
///
pub struct StealableTaskPool<O: Default + 'static> {
    public:                         Injector<Arc<AsyncTask<StealableTaskPool<O>, O>>>,          //公共的任务池
    workers:                        Vec<StealableTaskQueue<O>>,                                 //工作者的任务队列列表
    internal_stealers:              Vec<FIFOStealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>>,  //工作者任务队列的本地内部任务窃取者
    external_stealers:              Vec<Stealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>>,      //工作者任务队列的本地外部任务窃取者
    internal_consume:               AtomicUsize,                                                //内部任务消费计数
    internal_produce:               AtomicUsize,                                                //内部任务生产计数
    internal_traffic_statistics:    AtomicUsize,                                                //内部任务流量统计
    external_consume:               AtomicUsize,                                                //外部任务消费计数
    external_produce:               AtomicUsize,                                                //外部任务生产计数
    external_traffic_statistics:    AtomicUsize,                                                //外部任务流量统计
    weights:                        [u8; 2],                                                    //工作者任务队列的权重
    clock:                          Clock,                                                      //任务池的时钟
    interval:                       usize,                                                      //整理的间隔时长，单位ms
    last_time:                      UnsafeCell<QInstant>,                                       //上一次整理的时间
}

unsafe impl<O: Default + 'static> Send for StealableTaskPool<O> {}
unsafe impl<O: Default + 'static> Sync for StealableTaskPool<O> {}

impl<O: Default + 'static> Default for StealableTaskPool<O> {
    fn default() -> Self {
        StealableTaskPool::new()
    }
}

impl<O: Default + 'static> AsyncTaskPool<O> for StealableTaskPool<O> {
    type Pool = StealableTaskPool<O>;

    #[inline]
    fn get_thread_id(&self) -> usize {
        match PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| unsafe { *thread_id.get() }) {
            Err(e) => {
                //不应该执行到这个分支
                panic!(
                    "Get thread id failed, thread: {:?}, reason: {:?}",
                    thread::current(),
                    e
                );
            }
            Ok(id) => id,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.internal_produce
            .load(Ordering::Relaxed)
            .checked_sub(self.internal_consume.load(Ordering::Relaxed))
            .unwrap_or(0)
            +
            self.external_produce
                .load(Ordering::Relaxed)
                .checked_sub(self.external_consume.load(Ordering::Relaxed))
                .unwrap_or(0)
    }

    #[inline]
    fn push(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.public.push(task);

        self
            .external_produce
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[inline]
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        let id = self.get_thread_id();
        let rt_uid = task.owner();
        if (id >> 32) == rt_uid {
            //当前是运行时所在线程
            let worker = &self.workers[id & 0xffffffff];
            if worker.remaining_internal_capacity() > 0 {
                //本地内部任务队列有空闲容量，则立即将任务加入本地内部任务队列
                let _ = worker.internal.push(task);

                self
                    .internal_produce
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
                //本地内部任务队列没有空闲容量，则立即将任务加入公共任务池
                self.push(task)
            }
        } else {
            //当前不是运行时所在线程
            self.push(task)
        }
    }

    #[inline]
    fn push_priority(&self,
                     priority: usize,
                     task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        if priority >= DEFAULT_MAX_HIGH_PRIORITY_BOUNDED {
            //最高优先级
            let id = self.get_thread_id();
            let rt_uid = task.owner();
            if (id >> 32) == rt_uid {
                //当前是运行时所在线程
                let worker = &self.workers[id & 0xffffffff];
                if worker.stack_len() < 1 {
                    //本地任务栈有空闲容量，则立即将任务加入本地任务栈
                    unsafe {
                        *worker.stack.get() = Some(task);
                    }
                } else if worker.remaining_internal_capacity() > 0 {
                    //本地内部任务队列有空闲容量，则立即将任务加入本地内部任务队列
                    let _ = worker.internal.push(task);
                } else {
                    //本地任务栈和本地内部任务队列都没有空闲容量，则立即将任务加入公共任务池
                    return self.push(task);
                }

                self
                    .internal_produce
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
                //当前不是运行时所在线程
                self.push(task)
            }
        } else if priority >= DEFAULT_HIGH_PRIORITY_BOUNDED {
            //高优先级
            self.push_local(task)
        } else {
            //低优先级
            self.push(task)
        }
    }

    #[inline]
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.push_priority(DEFAULT_MAX_HIGH_PRIORITY_BOUNDED, task)
    }

    #[inline]
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>> {
        let id = self.get_thread_id() & 0xffffffff;
        let worker = &self.workers[id];
        let task = unsafe { (&mut *worker
            .stack
            .get())
            .take()
        };
        if task.is_some() {
            //指定工作者的任务栈有任务，则立即返回任务
            return task;
        }

        //从指定工作者的任务队列中弹出任务
        try_pop_by_weight(self, worker, id)
    }

    #[inline]
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>> {
        let mut tasks = Vec::with_capacity(self.len());
        while let Some(task) = self.try_pop() {
            tasks.push(task);
        }

        tasks.into_iter()
    }

    #[inline]
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        //多线程任务运行时不支持此方法
        None
    }
}

// 获取指定数字的MSB
const fn get_msb(n: usize) -> usize {
    usize::BITS as usize - n.leading_zeros() as usize
}

// 尝试通过统计信息更新权重，根据权重选择从本地外部任务队列或本地内部任务队列中弹出任务
fn try_pop_by_weight<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                           local_worker: &StealableTaskQueue<O>,
                                           local_worker_id: usize)
                                           -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    unsafe {
        let duration = pool
            .clock
            .recent()
            .duration_since(*pool.last_time.get())
            .as_millis() as usize;
        if duration >= pool.interval {
            //开始整理外部任务队列和内部任务队列的任务数量，并更新权重
            let new_external_traffic_statistics = pool
                .external_produce
                .load(Ordering::Relaxed);
            let new_internal_traffic_statistics = pool
                .internal_produce
                .load(Ordering::Relaxed);

            //获取外部任务增量和内部任务增量
            let external_delta = if new_external_traffic_statistics == 0 {
                //上次整理到本次整理之间，外部任务数量为空，则增量为1
                1
            } else {
                //上次整理到本次整理之间，外部任务数量不为空，则计算两次整理之间的外部任务数量的增量
                new_external_traffic_statistics
                    .checked_sub(pool
                        .external_traffic_statistics
                        .load(Ordering::Relaxed))
                    .unwrap_or(1)
            };
            pool
                .external_traffic_statistics
                .store(new_external_traffic_statistics, Ordering::Relaxed); //更新外部任务流量统计
            let internal_delta = if new_internal_traffic_statistics == 0 {
                //上次整理到本次整理之间，内部任务数量为空，则增量为1
                1
            } else {
                //上次整理到本次整理之间，内部任务数量不为空，则计算两次整理之间的内部任务数量的增量
                new_internal_traffic_statistics
                    .checked_sub(pool
                        .internal_traffic_statistics
                        .load(Ordering::Relaxed))
                    .unwrap_or(1)
            };
            pool
                .internal_traffic_statistics
                .store(new_internal_traffic_statistics, Ordering::Relaxed); //更新内部任务流量统计

            //更新外部任务队列和内部任务队列的权重
            let selector = &mut *local_worker.selector.get();
            if external_delta > internal_delta {
                //内部任务增量较小
                let msb = get_msb(internal_delta);
                let internal_weight
                    = (internal_delta >> msb.checked_sub(2).unwrap_or(0)).max(1);
                let external_weight
                    = ((external_delta >> msb).min(DEFAULT_MAX_WEIGHT as usize)).max(1);

                selector.change_weight(0, external_weight as u8);
                selector.change_weight(1, internal_weight as u8);
            } else if external_delta < internal_delta {
                //外部任务增量较小
                let msb = get_msb(external_delta);
                let external_weight
                    = (external_delta >> msb.checked_sub(2).unwrap_or(0)).max(1);
                let internal_weight
                    = ((internal_delta >> msb).min(DEFAULT_MAX_WEIGHT as usize)).max(1);

                selector.change_weight(0, external_weight as u8);
                selector.change_weight(1, internal_weight as u8);
            } else {
                //外部任务和内部任务增量相同
                selector.change_weight(0, 1);
                selector.change_weight(1, 1);
            }

            *pool.last_time.get() = pool.clock.recent(); //更新上一次整理的时间
        }

        //根据权重选择从指定的任务队列弹出任务
        match (&mut *local_worker.selector.get()).select() {
            0 => {
                //弹出外部任务
                let task = try_pop_external(pool, local_worker, local_worker_id);
                if task.is_some() {
                    task
                } else {
                    //当前没有外部任务，则尝试弹出内部任务
                    try_pop_internal(pool, local_worker, local_worker_id)
                }
            },
            _ => {
                //弹出内部任务
                let task = try_pop_internal(pool, local_worker, local_worker_id);
                if task.is_some() {
                    task
                } else {
                    //当前没有内部任务，则尝试弹出外部任务
                    try_pop_external(pool, local_worker, local_worker_id)
                }
            },
        }
    }
}

// 尝试弹出内部任务队列的任务
#[inline]
fn try_pop_internal<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                          local_worker: &StealableTaskQueue<O>,
                                          local_worker_id: usize)
    -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    let task = local_worker
        .internal
        .pop();
    if task.is_some() {
        //如果工作者有内部任务，则立即返回
        pool
            .internal_consume
            .fetch_add(1, Ordering::Relaxed);
        task
    } else {
        //工作者的内部任务队列为空，则随机从其它工作者的内部任务队列中窃取任务
        let mut gen = thread_rng();
        let mut worker_stealers: Vec<&FIFOStealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>> = pool
            .internal_stealers
            .iter()
            .enumerate()
            .filter_map(|(index, other)| {
                if index != local_worker_id {
                    Some(other)
                } else {
                    //忽略本地工作者
                    None
                }
            })
            .collect();

        let remaining_len = local_worker.remaining_internal_capacity();
        loop {
            //随机窃取其它工作者的任务队列
            if worker_stealers.len() == 0 {
                //所有其它工作者的任务队列都为空，则返回空
                break;
            }

            let index = gen.gen_range(0..worker_stealers.len());
            let worker_stealer = worker_stealers.swap_remove(index);

            match worker_stealer.steal_and_pop(&local_worker.internal,
                                               |count| {
                                                   let stealable_len = count / 2;
                                                   if stealable_len <= remaining_len {
                                                       //当前工作者内部任务队列的剩余容量足够，则窃取指定的其它工作者的内部任务队列中一半的任务
                                                       if stealable_len == 0 {
                                                           1
                                                       } else {
                                                           stealable_len
                                                       }
                                                   } else {
                                                       //当前工作者内部任务队列的剩余容量不足够，则从指定的其它工作者的内部任务队列中窃取当前工作者内部任务队列剩余容量的任务
                                                       remaining_len
                                                   }
                                               }) {
                Err(StealError::Empty) => {
                    //指定的其它工作者的内部任务队列中没有可窃取的任务，则继续窃取下一个其它工作者的内部任务队列
                    continue;
                },
                Err(StealError::Busy) => {
                    //需要重试窃取指定的其它工作者的内部任务队列中的任务
                    continue;
                },
                Ok((task, _)) => {
                    //从从已窃取到的其它工作者内部任务中获取到首个任务，并立即返回
                    pool.internal_consume.fetch_add(1, Ordering::Relaxed);
                    return Some(task);
                },
            }
        }

        None
    }
}

// 尝试弹出外部任务队列的任务
#[inline]
fn try_pop_external<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                          local_worker: &StealableTaskQueue<O>,
                                          local_worker_id: usize)
    -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    let task = local_worker
        .external
        .pop();
    if task.is_some() {
        //如果工作者有外部任务，则立即返回
        pool
            .external_consume
            .fetch_add(1, Ordering::Relaxed);
        task
    } else {
        //工作者的外部任务队列为空，则从公共任务池中弹出任务
        let task = try_pop_public(pool, local_worker);
        if task.is_some() {
            //如果公共任务池有外部任务，则立即返回
            pool
                .external_consume
                .fetch_add(1, Ordering::Relaxed);
            task
        } else {
            //公共任务池为空，则随机从其它工作者的外部任务队列中窃取任务
            let mut gen = thread_rng();
            let mut worker_stealers: Vec<&Stealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>> = pool
                .external_stealers
                .iter()
                .enumerate()
                .filter_map(|(index, other)| {
                    if index != local_worker_id {
                        Some(other)
                    } else {
                        //忽略当前工作者
                        None
                    }
                })
                .collect();

            loop {
                //随机窃取其它工作者的任务队列
                if worker_stealers.len() == 0 {
                    //所有其它工作者的外部任务队列都为空，则返回空
                    break;
                }

                let index = gen.gen_range(0..worker_stealers.len());
                let worker_stealer = worker_stealers.swap_remove(index);

                match worker_stealer.steal_batch_and_pop(&local_worker.external) {
                    Steal::Success(task) => {
                        //从从已窃取到的其它工作者外部任务中获取到首个任务，并立即返回
                        pool.external_consume.fetch_add(1, Ordering::Relaxed);
                        return Some(task);
                    },
                    Steal::Retry => {
                        //需要重试窃取指定的其它工作者的外部任务队列中的任务
                        continue;
                    },
                    Steal::Empty => {
                        //指定的其它工作者的外部任务队列中没有可窃取的任务，则继续窃取下一个其它工作者的外部任务队列
                        continue;
                    },
                }
            }

            None
        }
    }
}

// 尝试弹出公共任务池的任务
#[inline]
fn try_pop_public<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                        local_worker: &StealableTaskQueue<O>)
    -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    loop {
        match pool.public.steal_batch_and_pop(&local_worker.external) {
            Steal::Empty => {
                //当前公共任务池没有任务
                return None;
            },
            Steal::Retry => {
                //需要重试窃取公共任务池的任务
                continue;
            },
            Steal::Success(task) => {
                //从已窃取到的公共任务中获取到首个任务，并立即返回
                pool.external_consume.fetch_add(1, Ordering::Relaxed);
                return Some(task);
            },
        }
    }
}

impl<O: Default + 'static> AsyncTaskPoolExt<O> for StealableTaskPool<O> {
    #[inline]
    fn worker_len(&self) -> usize {
        self.workers.len()
    }

    #[inline]
    fn clone_thread_waker(&self) -> Option<Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        if let Some(worker) = self.workers.get(self.get_thread_id() & 0xffffffff) {
            return Some(worker.thread_waker.clone());
        }

        None
    }
}

impl<O: Default + 'static> StealableTaskPool<O> {
    /// 可窃取的快速工作者任务池
    pub fn new() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
            let size = num_cpus::get_physical() * 2; //默认最大工作者任务池数量是当前cpu物理核的2倍
        #[cfg(target_arch = "wasm32")]
            let size = 1; //默认最大工作者任务池数量是1
        StealableTaskPool::with(size,
                                0x8000,
                                [1, 1],
                                3000)
    }

    /// 构建指定工作者任务池数量，工作者内部任务队列容量，工作者任务栈容量，任务队列的权重和整理间隔时长的可窃取的快速工作者任务池
    pub fn with(worker_size: usize,
                internal_queue_capacity: usize,
                weights: [u8; 2],
                interval: usize) -> Self {
        if worker_size == 0 {
            //工作者任务池数量无效，则立即抛出异常
            panic!(
                "Create WorkerTaskPool failed, worker size: {}, reason: invalid worker size",
                worker_size
            );
        }
        if interval == 0 {
            panic!(
                "Create WorkerTaskPool failed, interval: {}, reason: invalid interval",
                worker_size
            );
        }

        let public = Injector::new();
        let mut workers = Vec::with_capacity(worker_size);
        let mut internal_stealers = Vec::with_capacity(worker_size);
        let mut external_stealers = Vec::with_capacity(worker_size);
        for _ in 0..worker_size {
            //初始化指定初始作者任务池数量的工作者任务池和窃取者
            let thread_waker = Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new()));
            let (worker,
                internal_stealer,
                external_stealer) =
                StealableTaskQueue::new(internal_queue_capacity,
                                        thread_waker);
            workers.push(worker);
            internal_stealers.push(internal_stealer);
            external_stealers.push(external_stealer);
        }
        let internal_consume = AtomicUsize::new(0);
        let internal_produce = AtomicUsize::new(0);
        let internal_traffic_statistics = AtomicUsize::new(0);
        let external_consume = AtomicUsize::new(0);
        let external_produce = AtomicUsize::new(0);
        let external_traffic_statistics = AtomicUsize::new(0);
        let clock = Clock::new();
        let last_time = UnsafeCell::new(clock.recent());

        StealableTaskPool {
            public,
            workers,
            internal_stealers,
            external_stealers,
            internal_consume,
            internal_produce,
            internal_traffic_statistics,
            external_consume,
            external_produce,
            external_traffic_statistics,
            weights,
            clock,
            interval,
            last_time,
        }
    }
}

///
/// 异步多线程任务运行时，支持运行时线程伸缩
///
pub struct MultiTaskRuntime<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = StealableTaskPool<O>,
>(
    Arc<(
        usize,                                                  //运行时唯一id
        Arc<P>,                                                 //异步任务池
        Option<
            Vec<(
                Sender<(usize, AsyncTimingTask<P, O>)>,
                Arc<AsyncTaskTimerByNotCancel<P, O>>,
            )>,
        >,                                                      //休眠的异步任务生产者和本地定时器
        AtomicUsize,                                            //定时任务计数器
        Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>, //待唤醒的工作者唤醒器队列
        AtomicUsize,                                            //定时器生产计数
        AtomicUsize,                                            //定时器消费计数
    )>,
);

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for MultiTaskRuntime<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for MultiTaskRuntime<O, P>
{
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Clone
    for MultiTaskRuntime<O, P>
{
    fn clone(&self) -> Self {
        MultiTaskRuntime(self.0.clone())
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> AsyncRuntime<O>
    for MultiTaskRuntime<O, P>
{
    type Pool = P;

    /// 共享运行时内部任务池
    fn shared_pool(&self) -> Arc<Self::Pool> {
        (self.0).1.clone()
    }

    /// 获取当前异步运行时的唯一id
    fn get_id(&self) -> usize {
        (self.0).0
    }

    /// 获取当前异步运行时待处理任务数量
    fn wait_len(&self) -> usize {
        (self.0)
            .5
            .load(Ordering::Relaxed)
            .checked_sub((self.0).6.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 获取当前异步运行时任务数量
    fn len(&self) -> usize {
        (self.0).1.len()
    }

    /// 分配异步任务的唯一id
    fn alloc<R: 'static>(&self) -> TaskId {
        TaskId(UnsafeCell::new((TaskHandle::<R>::default().into_raw() as u128) << 64 | self.get_id() as u128 & 0xffffffffffffffff))
    }

    /// 派发一个指定的异步任务到异步运行时
    fn spawn<F>(&self, future: F) -> Result<TaskId>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output=O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_local_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个指定优先级的异步任务到异步运行时
    fn spawn_priority<F>(&self, priority: usize, future: F) -> Result<TaskId>
        where
            F: Future<Output=O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_priority_by_id(task_id.clone(), priority, future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output=O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_yield_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing<F>(&self, future: F, time: usize) -> Result<TaskId>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_timing_by_id(task_id.clone(), future, time) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时
    fn spawn_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
        let result = {
            (self.0).1.push(Arc::new(AsyncTask::new(
                task_id,
                (self.0).1.clone(),
                DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
                Some(future.boxed()),
            )))
        };

        if let Some(worker_waker) = (self.0).4.pop() {
            //有待唤醒的工作者
            let (is_sleep, lock, condvar) = &*worker_waker;
            let _locked = lock.lock();
            if is_sleep.load(Ordering::Relaxed) {
                //待唤醒的工作者，正在休眠，则立即唤醒此工作者
                is_sleep.store(false, Ordering::SeqCst); //设置为未休眠
                condvar.notify_one();
            }
        }

        result
    }

    fn spawn_local_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
        (self.0).1.push_local(Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            DEFAULT_HIGH_PRIORITY_BOUNDED,
            Some(future.boxed()),
        )))
    }

    /// 派发一个指定任务唯一id和任务优先级的异步任务到异步运行时
    fn spawn_priority_by_id<F>(&self,
                               task_id: TaskId,
                               priority: usize,
                               future: F) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
        let result = {
            (self.0).1.push_priority(priority, Arc::new(AsyncTask::new(
                task_id,
                (self.0).1.clone(),
                priority,
                Some(future.boxed()),
            )))
        };

        if let Some(worker_waker) = (self.0).4.pop() {
            //有待唤醒的工作者
            let (is_sleep, lock, condvar) = &*worker_waker;
            let _locked = lock.lock();
            if is_sleep.load(Ordering::Relaxed) {
                //待唤醒的工作者，正在休眠，则立即唤醒此工作者
                is_sleep.store(false, Ordering::SeqCst); //设置为未休眠
                condvar.notify_one();
            }
        }

        result
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时，并立即让出任务的当前运行
    #[inline]
    fn spawn_yield_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
        self.spawn_priority_by_id(task_id,
                                  DEFAULT_HIGH_PRIORITY_BOUNDED,
                                  future)
    }

    /// 派发一个指定任务唯一id和在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing_by_id<F>(&self,
                             task_id: TaskId,
                             future: F,
                             time: usize) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
        let rt = self.clone();
        self.spawn_by_id(task_id, async move {
            if let Some(timers) = &(rt.0).2 {
                //为定时器设置定时异步任务
                let id = (rt.0).1.get_thread_id() & 0xffffffff;
                let (_, timer) = &timers[id];
                timer.set_timer(
                    AsyncTimingTask::WaitRun(Arc::new(AsyncTask::new(
                        rt.alloc::<F::Output>(),
                        (rt.0).1.clone(),
                        DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                        Some(future.boxed()),
                    ))),
                    time,
                );

                (rt.0).5.fetch_add(1, Ordering::Relaxed);
            }

            Default::default()
        })
    }

    /// 挂起指定唯一id的异步任务
    fn pending<Output: 'static>(&self, task_id: &TaskId, waker: Waker) -> Poll<Output> {
        task_id.set_waker::<Output>(waker);
        Poll::Pending
    }

    /// 唤醒指定唯一id的异步任务
    fn wakeup<Output: 'static>(&self, task_id: &TaskId) {
        task_id.wakeup::<Output>();
    }

    /// 挂起当前异步运行时的当前任务，并在指定的其它运行时上派发一个指定的异步任务，等待其它运行时上的异步任务完成后，唤醒当前运行时的当前任务，并返回其它运行时上的异步任务的值
    fn wait<V: Send + 'static>(&self) -> AsyncWait<V> {
        AsyncWait(self.wait_any(2))
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，其中任意一个任务完成，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成的任务的值将被忽略
    fn wait_any<V: Send + 'static>(&self, capacity: usize) -> AsyncWaitAny<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAny {
            capacity,
            producor,
            consumer,
        }
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，任务返回后需要通过用户指定的检查回调进行检查，其中任意一个任务检查通过，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成或未检查通过的任务的值将被忽略，如果所有任务都未检查通过，则强制唤醒当前运行时的当前任务
    fn wait_any_callback<V: Send + 'static>(&self, capacity: usize) -> AsyncWaitAnyCallback<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAnyCallback {
            capacity,
            producor,
            consumer,
        }
    }

    /// 构建用于派发多个异步任务到指定运行时的映射归并，需要指定映射归并的容量
    fn map_reduce<V: Send + 'static>(&self, capacity: usize) -> AsyncMapReduce<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncMapReduce {
            count: 0,
            capacity,
            producor,
            consumer,
        }
    }

    /// 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    fn timeout(&self, timeout: usize) -> BoxFuture<'static, ()> {
        let rt = self.clone();

        if let Some(timers) = &(self.0).2 {
            //有本地定时器，则异步等待指定时间
            match PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| {
                //将休眠的异步任务投递到当前派发线程的定时器内
                let thread_id = unsafe { *thread_id.get() };
                timers[thread_id].clone()
            }) {
                Err(_) => {
                    panic!("Multi thread runtime timeout failed, reason: local thread id not match")
                }
                Ok((producor, _)) => AsyncWaitTimeout::new(rt, producor, timeout).boxed(),
            }
        } else {
            //没有本地定时器，则同步休眠指定时间
            async move {
                thread::sleep(Duration::from_millis(timeout as u64));
            }
            .boxed()
        }
    }

    /// 立即让出当前任务的执行
    fn yield_now(&self) -> BoxFuture<'static, ()> {
        async move {
            YieldNow(false).await;
        }.boxed()
    }

    /// 生成一个异步管道，输入指定流，输入流的每个值通过过滤器生成输出流的值
    fn pipeline<S, SO, F, FO>(&self, input: S, mut filter: F) -> BoxStream<'static, FO>
    where
        S: Stream<Item = SO> + Send + 'static,
        SO: Send + 'static,
        F: FnMut(SO) -> AsyncPipelineResult<FO> + Send + 'static,
        FO: Send + 'static,
    {
        let output = stream! {
            for await value in input {
                match filter(value) {
                    AsyncPipelineResult::Disconnect => {
                        //立即中止管道
                        break;
                    },
                    AsyncPipelineResult::Filtered(result) => {
                        yield result;
                    },
                }
            }
        };

        output.boxed()
    }

    /// 关闭异步运行时，返回请求关闭是否成功
    fn close(&self) -> bool {
        false
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> AsyncRuntimeExt<O>
    for MultiTaskRuntime<O, P>
{
    fn spawn_with_context<F, C>(&self, task_id: TaskId, future: F, context: C) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
        C: 'static,
    {
        let task = Arc::new(AsyncTask::with_context(
            task_id,
            (self.0).1.clone(),
            DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
            Some(future.boxed()),
            context,
        ));
        let result = (self.0).1.push(task);

        if let Some(worker_waker) = (self.0).4.pop() {
            //有待唤醒的工作者
            let (is_sleep, lock, condvar) = &*worker_waker;
            let _locked = lock.lock();
            if is_sleep.load(Ordering::Relaxed) {
                //待唤醒的工作者，正在休眠，则立即唤醒此工作者
                is_sleep.store(false, Ordering::SeqCst); //设置为未休眠
                condvar.notify_one();
            }
        }

        result
    }

    fn spawn_timing_with_context<F, C>(
        &self,
        task_id: TaskId,
        future: F,
        context: C,
        time: usize,
    ) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
        C: Send + 'static,
    {
        let rt = self.clone();
        self.spawn_by_id(task_id, async move {
            if let Some(timers) = &(rt.0).2 {
                //为定时器设置定时异步任务
                let id = (rt.0).1.get_thread_id() & 0xffffffff;
                let (_, timer) = &timers[id];
                timer.set_timer(
                    AsyncTimingTask::WaitRun(Arc::new(AsyncTask::with_context(
                        rt.alloc::<F::Output>(),
                        (rt.0).1.clone(),
                        DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                        Some(future.boxed()),
                        context,
                    ))),
                    time,
                );

                (rt.0).5.fetch_add(1, Ordering::Relaxed);
            }

            Default::default()
        })
    }

    fn block_on<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        <F as Future>::Output: Default + Send + 'static,
    {
        //从本地线程获取当前异步运行时
        if let Some(local_rt) = local_async_runtime::<F::Output>() {
            //本地线程绑定了异步运行时
            if local_rt.get_id() == self.get_id() {
                //如果是相同运行时，则立即返回错误
                return Err(Error::new(
                    ErrorKind::WouldBlock,
                    format!("Block on failed, reason: would block"),
                ));
            }
        }

        let (sender, receiver) = bounded(1);
        if let Err(e) = self.spawn(async move {
            //在指定运行时中执行，并返回结果
            let r = future.await;
            sender.send(r);

            Default::default()
        }) {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Block on failed, reason: {:?}", e),
            ));
        }

        //同步阻塞等待异步任务返回
        match receiver.recv() {
            Err(e) => Err(Error::new(
                ErrorKind::Other,
                format!("Block on failed, reason: {:?}", e),
            )),
            Ok(result) => Ok(result),
        }
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    MultiTaskRuntime<O, P>
{
    /// 获取当前运行时可新增的工作者数量
    pub fn idler_len(&self) -> usize {
        (self.0).1.idler_len()
    }

    /// 获取当前运行时的工作者数量
    pub fn worker_len(&self) -> usize {
        (self.0).1.worker_len()
    }

    /// 获取当前运行时缓冲区的任务数量，缓冲区的任务暂时没有分配给工作者
    pub fn buffer_len(&self) -> usize {
        (self.0).1.buffer_len()
    }

    /// 获取当前多线程异步运行时的本地异步运行时
    pub fn to_local_runtime(&self) -> LocalAsyncRuntime<O> {
        LocalAsyncRuntime {
            inner: self.as_raw(),
            get_id_func: MultiTaskRuntime::<O, P>::get_id_raw,
            spawn_func: MultiTaskRuntime::<O, P>::spawn_raw,
            spawn_timing_func: MultiTaskRuntime::<O, P>::spawn_timing_raw,
            timeout_func: MultiTaskRuntime::<O, P>::timeout_raw,
        }
    }

    /// 获取当前多线程异步运行时的指针
    #[inline]
    pub(crate) fn as_raw(&self) -> *const () {
        Arc::into_raw(self.0.clone()) as *const ()
    }

    // 获取指定指针的单线程异步运行时
    #[inline]
    pub(crate) fn from_raw(raw: *const ()) -> Self {
        let inner = unsafe {
            Arc::from_raw(
                raw as *const (
                    usize,
                    Arc<P>,
                    Option<
                        Vec<(
                            Sender<(usize, AsyncTimingTask<P, O>)>,
                            Arc<AsyncTaskTimerByNotCancel<P, O>>,
                        )>,
                    >,
                    AtomicUsize,
                    Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>,
                    AtomicUsize,
                    AtomicUsize,
                ),
            )
        };
        MultiTaskRuntime(inner)
    }

    // 获取当前异步运行时的唯一id
    pub(crate) fn get_id_raw(raw: *const ()) -> usize {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let id = rt.get_id();
        Arc::into_raw(rt.0); //避免提前释放
        id
    }

    // 派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_raw<F>(raw: *const (), future: F) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_by_id(rt.alloc::<F::Output>(), future);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 定时派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_timing_raw(
        raw: *const (),
        future: BoxFuture<'static, O>,
        timeout: usize,
    ) -> Result<()> {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_timing_by_id(rt.alloc::<O>(), future, timeout);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    pub(crate) fn timeout_raw(raw: *const (), timeout: usize) -> BoxFuture<'static, ()> {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let boxed = rt.timeout(timeout);
        Arc::into_raw(rt.0); //避免提前释放
        boxed
    }
}

///
/// 异步多线程任务运行时构建器
///
pub struct MultiTaskRuntimeBuilder<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = StealableTaskPool<O>,
> {
    pool: P,                 //异步多线程任务运行时
    prefix: String,          //工作者线程名称前缀
    init: usize,             //初始工作者数量
    min: usize,              //最少工作者数量
    max: usize,              //最大工作者数量
    stack_size: usize,       //工作者线程栈大小
    timeout: u64,            //工作者空闲时最长休眠时间
    interval: Option<usize>, //工作者定时器间隔
    marker: PhantomData<O>,
}

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for MultiTaskRuntimeBuilder<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for MultiTaskRuntimeBuilder<O, P>
{
}

impl<O: Default + 'static> Default for MultiTaskRuntimeBuilder<O> {
    //默认构建可窃取可伸缩的多线程运行时
    fn default() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let core_len = num_cpus::get(); //默认的工作者的数量为本机逻辑核数
        #[cfg(target_arch = "wasm32")]
        let core_len = 1; //默认的工作者的数量为1
        let pool = StealableTaskPool::with(core_len,
                                           65535,
                                           [1, 1],
                                           3000);
        MultiTaskRuntimeBuilder::new(pool)
            .thread_stack_size(2 * 1024 * 1024)
            .set_timer_interval(1)
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    MultiTaskRuntimeBuilder<O, P>
{
    /// 构建指定任务池、线程名前缀、初始线程数量、最少线程数量、最大线程数量、线程栈大小、线程空闲时最长休眠时间和是否使用本地定时器的多线程任务池
    pub fn new(mut pool: P) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let core_len = num_cpus::get(); //获取本机cpu逻辑核数
        #[cfg(target_arch = "wasm32")]
        let core_len = 1; //默认为1

        MultiTaskRuntimeBuilder {
            pool,
            prefix: DEFAULT_WORKER_THREAD_PREFIX.to_string(),
            init: core_len,
            min: core_len,
            max: core_len,
            stack_size: DEFAULT_THREAD_STACK_SIZE,
            timeout: DEFAULT_WORKER_THREAD_SLEEP_TIME,
            interval: None,
            marker: PhantomData,
        }
    }

    /// 设置工作者线程名称前缀
    pub fn thread_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// 设置工作者线程栈大小
    pub fn thread_stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = stack_size;
        self
    }

    /// 设置初始工作者数量
    pub fn init_worker_size(mut self, mut init: usize) -> Self {
        if init == 0 {
            //初始线程数量过小，则设置默认的初始线程数量
            init = DEFAULT_INIT_WORKER_SIZE;
        }

        self.init = init;
        self
    }

    /// 设置最小工作者数量和最大工作者数量
    pub fn set_worker_limit(mut self, mut min: usize, mut max: usize) -> Self {
        if self.init > max {
            //初始线程数量大于最大线程数量，则设置最大线程数量为初始线程数量
            max = self.init;
        }

        if min == 0 || min > max {
            //最少线程数量无效，则设置最少线程数量为最大线程数量
            min = max;
        }

        self.min = min;
        self.max = max;
        self
    }

    /// 设置工作者空闲时最大休眠时长
    pub fn set_timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }

    /// 设置工作者定时器间隔
    pub fn set_timer_interval(mut self, interval: usize) -> Self {
        self.interval = Some(interval);
        self
    }

    /// 构建并启动多线程异步运行时
    pub fn build(mut self) -> MultiTaskRuntime<O, P> {
        //构建多线程任务运行时的本地定时器和定时异步任务生产者
        let interval = self.interval;
        let mut timers = if let Some(_) = interval {
            Some(Vec::with_capacity(self.max))
        } else {
            None
        };
        for _ in 0..self.max {
            //初始化指定的最大线程数量的本地定时器和定时异步任务生产者，定时器不会在关闭工作者时被移除
            if let Some(vec) = &mut timers {
                let timer = AsyncTaskTimerByNotCancel::new();
                let producor = timer.producor.clone();
                let timer = Arc::new(timer);
                vec.push((producor, timer));
            };
        }

        //构建多线程任务运行时
        let rt_uid = alloc_rt_uid();
        let waits = Arc::new(ArrayQueue::new(self.max));
        let mut pool = self.pool;
        pool.set_waits(waits.clone()); //设置待唤醒的工作者唤醒器队列
        let pool = Arc::new(pool);
        let runtime = MultiTaskRuntime(Arc::new((
            rt_uid,
            pool,
            timers,
            AtomicUsize::new(0),
            waits,
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        )));

        //构建初始化线程数量的线程构建器
        let mut builders = Vec::with_capacity(self.init);
        for index in 0..self.init {
            let builder = Builder::new()
                .name(self.prefix.clone() + "-" + index.to_string().as_str())
                .stack_size(self.stack_size);
            builders.push(builder);
        }

        //启动工作者线程
        let min = self.min;
        for index in 0..builders.len() {
            let builder = builders.remove(0);
            let runtime = runtime.clone();
            let timeout = self.timeout;
            let timer = if let Some(timers) = &(runtime.0).2 {
                let (_, timer) = &timers[index];
                Some(timer.clone())
            } else {
                None
            };

            spawn_worker_thread(builder, index, runtime, min, timeout, interval, timer);
        }

        runtime
    }
}

//分派工作者线程，并开始工作
fn spawn_worker_thread<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
>(
    builder: Builder,
    index: usize,
    runtime: MultiTaskRuntime<O, P>,
    min: usize,
    timeout: u64,
    interval: Option<usize>,
    timer: Option<Arc<AsyncTaskTimerByNotCancel<P, O>>>,
) {
    if let Some(timer) = timer {
        //设置了定时器
        let rt_uid = runtime.get_id();
        let _ = builder.spawn(move || {
            //设置线程本地唯一id
            if let Err(e) = PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| unsafe {
                *thread_id.get() = rt_uid << 32 | index & 0xffffffff;
            }) {
                panic!(
                    "Multi thread runtime startup failed, thread id: {:?}, reason: {:?}",
                    index, e
                );
            }

            //绑定运行时到线程
            let runtime_copy = runtime.clone();
            match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME.try_with(move |rt| {
                let raw = Arc::into_raw(Arc::new(runtime_copy.to_local_runtime()))
                    as *mut LocalAsyncRuntime<O> as *mut ();
                rt.store(raw, Ordering::Relaxed);
            }) {
                Err(e) => {
                    panic!("Bind multi runtime to local thread failed, reason: {:?}", e);
                }
                Ok(_) => (),
            }

            //执行有定时器的工作循环
            timer_work_loop(
                runtime,
                index,
                min,
                timeout,
                interval.unwrap() as u64,
                timer,
            );
        });
    } else {
        //未设置定时器
        let rt_uid = runtime.get_id();
        let _ = builder.spawn(move || {
            //设置线程本地唯一id
            if let Err(e) = PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| unsafe {
                *thread_id.get() = rt_uid << 32 | index & 0xffffffff;
            }) {
                panic!(
                    "Multi thread runtime startup failed, thread id: {:?}, reason: {:?}",
                    index, e
                );
            }

            //绑定运行时到线程
            let runtime_copy = runtime.clone();
            match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME.try_with(move |rt| {
                let raw = Arc::into_raw(Arc::new(runtime_copy.to_local_runtime()))
                    as *mut LocalAsyncRuntime<O> as *mut ();
                rt.store(raw, Ordering::Relaxed);
            }) {
                Err(e) => {
                    panic!("Bind multi runtime to local thread failed, reason: {:?}", e);
                }
                Ok(_) => (),
            }

            //执行无定时器的工作循环
            work_loop(runtime, index, min, timeout);
        });
    }
}

//线程工作循环
fn timer_work_loop<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    runtime: MultiTaskRuntime<O, P>,
    index: usize,
    min: usize,
    sleep_timeout: u64,
    timer_interval: u64,
    timer: Arc<AsyncTaskTimerByNotCancel<P, O>>,
) {
    //初始化当前线程的线程id和线程活动状态
    let pool = (runtime.0).1.clone();
    let worker_waker = pool.clone_thread_waker().unwrap();

    let mut sleep_count = 0; //连续休眠计数器
    let clock = Clock::new();
    loop {
        //设置新的定时异步任务，并唤醒已到期的定时异步任务
        let mut timer_run_millis = clock.recent(); //重置定时器运行时长
        let mut pop_len = 0;
        (runtime.0)
            .5
            .fetch_add(timer.consume(),
                       Ordering::Relaxed);
        loop {
            let current_time = timer.is_require_pop();
            if let Some(current_time) = current_time {
                //当前有到期的定时异步任务，则开始处理到期的所有定时异步任务
                loop {
                    let timed_out = timer.pop(current_time);
                    if let Some(timing_task) = timed_out {
                        match timing_task {
                            AsyncTimingTask::Pended(expired) => {
                                //唤醒休眠的异步任务，不需要立即在本工作者中执行，因为休眠的异步任务无法取消
                                runtime.wakeup::<O>(&expired);
                            }
                            AsyncTimingTask::WaitRun(expired) => {
                                //执行到期的定时异步任务，需要立即在本工作者中执行，因为定时异步任务可以取消
                                (runtime.0)
                                    .1
                                    .push_priority(DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                                                   expired);
                                if let Some(task) = pool.try_pop() {
                                    sleep_count = 0; //重置连续休眠次数
                                    run_task(&runtime, task);
                                }
                            }
                        }
                        pop_len += 1;

                        if let Some(task) = pool.try_pop() {
                            //执行当前工作者任务池中的异步任务，避免定时异步任务占用当前工作者的所有工作时间
                            sleep_count = 0; //重置连续休眠次数
                            run_task(&runtime, task);
                        }
                    } else {
                        //当前所有的到期任务已处理完，则退出本次定时异步任务处理
                        break;
                    }
                }
            } else {
                //当前没有到期的定时异步任务，则退出本次定时异步任务处理
                break;
            }
        }
        (runtime.0)
            .6
            .fetch_add(pop_len,
                       Ordering::Relaxed);

        //继续执行当前工作者任务池中的异步任务
        match pool.try_pop() {
            None => {
                if runtime.len() > 0 {
                    //确认当前还有任务需要处理，可能还没分配到当前工作者，则当前工作者继续工作
                    continue;
                }

                //无任务，则准备休眠
                {
                    let (is_sleep, lock, condvar) = &*worker_waker;
                    let mut locked = lock.lock();

                    //设置当前为休眠状态
                    is_sleep.store(true, Ordering::SeqCst);

                    //获取休眠的实际时长
                    let diff_time = clock
                        .recent()
                        .duration_since(timer_run_millis)
                        .as_millis() as u64; //获取定时器运行时长
                    let real_timeout = if timer.len() == 0 {
                        //当前定时器没有未到期的任务，则休眠指定时长
                        sleep_timeout
                    } else {
                        //当前定时器还有未到期的任务，则计算需要休眠的时长
                        if diff_time >= timer_interval {
                            //定时器内部时间与当前时间差距过大，则忽略休眠，并继续工作
                            continue;
                        } else {
                            //定时器内部时间与当前时间差距不大，则休眠差值时间
                            timer_interval - diff_time
                        }
                    };

                    //记录待唤醒的工作者唤醒器，用于有新任务时唤醒对应的工作者
                    (runtime.0).4.push(worker_waker.clone());

                    //让当前工作者休眠，等待有任务时被唤醒或超时后自动唤醒
                    if condvar
                        .wait_for(&mut locked, Duration::from_millis(real_timeout))
                        .timed_out()
                    {
                        //条件超时唤醒，则设置状态为未休眠
                        is_sleep.store(false, Ordering::SeqCst);
                        //记录连续休眠次数，因为任务导致的唤醒不会计数
                        sleep_count += 1;
                    }
                }
            }
            Some(task) => {
                //有任务，则执行
                sleep_count = 0; //重置连续休眠次数
                run_task(&runtime, task);
            }
        }
    }

    //关闭当前工作者的任务池
    (runtime.0).1.close_worker();
    warn!(
        "Worker of runtime closed, runtime: {}, worker: {}, thread: {:?}",
        runtime.get_id(),
        index,
        thread::current()
    );
}

//线程工作循环
fn work_loop<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    runtime: MultiTaskRuntime<O, P>,
    index: usize,
    min: usize,
    sleep_timeout: u64,
) {
    //初始化当前线程的线程id和线程活动状态
    let pool = (runtime.0).1.clone();
    let worker_waker = pool.clone_thread_waker().unwrap();

    let mut sleep_count = 0; //连续休眠计数器
    loop {
        match pool.try_pop() {
            None => {
                //无任务，则准备休眠
                if runtime.len() > 0 {
                    //确认当前还有任务需要处理，可能还没分配到当前工作者，则当前工作者继续工作
                    continue;
                }

                {
                    let (is_sleep, lock, condvar) = &*worker_waker;
                    let mut locked = lock.lock();

                    //设置当前为休眠状态
                    is_sleep.store(true, Ordering::SeqCst);

                    //记录待唤醒的工作者唤醒器，用于有新任务时唤醒对应的工作者
                    (runtime.0).4.push(worker_waker.clone());

                    //让当前工作者休眠，等待有任务时被唤醒或超时后自动唤醒
                    if condvar
                        .wait_for(&mut locked, Duration::from_millis(sleep_timeout))
                        .timed_out()
                    {
                        //条件超时唤醒，则设置状态为未休眠
                        is_sleep.store(false, Ordering::SeqCst);
                        //记录连续休眠次数，因为任务导致的唤醒不会计数
                        sleep_count += 1;
                    }
                }
            }
            Some(task) => {
                //有任务，则执行
                sleep_count = 0; //重置连续休眠次数
                run_task(&runtime, task);
            }
        }
    }

    //关闭当前工作者的任务池
    (runtime.0).1.close_worker();
    warn!(
        "Worker of runtime closed, runtime: {}, worker: {}, thread: {:?}",
        runtime.get_id(),
        index,
        thread::current()
    );
}

//执行异步任务
#[inline]
fn run_task<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    runtime: &MultiTaskRuntime<O, P>,
    task: Arc<AsyncTask<P, O>>,
) {
    let waker = waker_ref(&task);
    let mut context = Context::from_waker(&*waker);
    if let Some(mut future) = task.get_inner() {
        if let Poll::Pending = future.as_mut().poll(&mut context) {
            //当前未准备好，则恢复异步任务，以保证异步服务后续访问异步任务和异步任务不被提前释放
            task.set_inner(Some(future));
        }
    } else {
        //当前异步任务在唤醒时还未被重置内部任务，则继续加入当前异步运行时队列，并等待下次被执行
        (runtime.0).1.push(task);
    }
}
