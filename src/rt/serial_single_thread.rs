//! # 单线程运行时
//!
//! # 特征:
//!
//! - [SingleTaskPool]\: 单线程任务池
//! - [SingleTaskRunner]\: 单线程异步任务执行器
//! - [SingleTaskRuntime]\: 异步单线程任务运行时
//!
//! [SingleTaskPool]: struct.SingleTaskPool.html
//! [SingleTaskRunner]: struct.SingleTaskRunner.html
//! [SingleTaskRuntime]: struct.SingleTaskRuntime.html
//!

use std::thread;
use std::any::Any;
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::mem::transmute;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Waker};
use std::vec::IntoIter;
use std::collections::vec_deque::VecDeque;
use std::process::Output;

use async_stream::stream;
use crossbeam_channel::{bounded, unbounded, Sender};
use crossbeam_queue::SegQueue;
use flume::bounded as async_bounded;
use futures::{
    future::{FutureExt, LocalBoxFuture},
    stream::{LocalBoxStream, Stream, StreamExt},
    task::{waker_ref, ArcWake},
};
use parking_lot::{Condvar, Mutex};
use quanta::Clock;

use wrr::IWRRSelector;

use crate::{
    lock::spin,
    rt::{
        PI_ASYNC_THREAD_LOCAL_ID, DEFAULT_MAX_HIGH_PRIORITY_BOUNDED, DEFAULT_HIGH_PRIORITY_BOUNDED, DEFAULT_MAX_LOW_PRIORITY_BOUNDED, alloc_rt_uid,
        serial::{
            bind_local_thread, local_async_runtime, AsyncMapReduce, AsyncRuntime, AsyncRuntimeExt,
            AsyncTask, AsyncTaskPool, AsyncTaskPoolExt, AsyncTaskTimer, AsyncTimingTask, AsyncWait,
            AsyncWaitAny, AsyncWaitAnyCallback, AsyncWaitResult, AsyncWaitTimeout,
            LocalAsyncRuntime,
        },
        AsyncPipelineResult, TaskId, TaskHandle, YieldNow
    },
};

///
/// 单线程任务池
///
pub struct SingleTaskPool<O: Default + 'static> {
    id:             usize,                                                      //绑定的线程唯一id
    public:         SegQueue<Arc<AsyncTask<SingleTaskPool<O>, O>>>,             //外部任务队列
    internal:       UnsafeCell<VecDeque<Arc<AsyncTask<SingleTaskPool<O>, O>>>>, //内部任务队列
    stack:          UnsafeCell<Vec<Arc<AsyncTask<SingleTaskPool<O>, O>>>>,      //本地任务栈
    selector:       UnsafeCell<IWRRSelector<2>>,                                //任务池选择器
    consume_count:  AtomicUsize,                                                //任务消费计数
    produce_count:  AtomicUsize,                                                //任务生产计数
    thread_waker:   Option<Arc<(AtomicBool, Mutex<()>, Condvar)>>,              //绑定线程的唤醒器
}

unsafe impl<O: Default + 'static> Send for SingleTaskPool<O> {}
unsafe impl<O: Default + 'static> Sync for SingleTaskPool<O> {}

impl<O: Default + 'static> Default for SingleTaskPool<O> {
    fn default() -> Self {
        SingleTaskPool::new([1, 1])
    }
}

impl<O: Default + 'static> AsyncTaskPool<O> for SingleTaskPool<O> {
    type Pool = SingleTaskPool<O>;

    #[inline]
    fn get_thread_id(&self) -> usize {
        let rt_uid = self.id;
        match PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| {
            let current = unsafe { *thread_id.get() };
            if current == usize::MAX {
                //当前还未初始化当前运行时的线程id，则初始化
                unsafe {
                    *thread_id.get() = rt_uid << 32;
                    *thread_id.get()
                }
            } else {
                current
            }
        }) {
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
        self.public.push(task);
        self.produce_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[inline]
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        let id = self.get_thread_id();
        let rt_uid = task.owner();
        if (id >> 32) == rt_uid {
            //当前是运行时所在线程
            unsafe {{
                (&mut *self.internal.get()).push_back(task);
            }}
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
                //当前是运行时所在线程
                unsafe {
                    let stack = (&mut *self.stack.get());
                    if stack
                        .capacity()
                        .checked_sub(stack.len())
                        .unwrap_or(0) >= 0 {
                        //本地任务栈有空闲容量，则立即将任务加入本地任务栈
                        (&mut *self.stack.get()).push(task);
                    } else {
                        //本地内部任务队列有空闲容量，则立即将任务加入本地内部任务队列
                        (&mut *self.internal.get()).push_back(task);
                    }
                }

                self.produce_count.fetch_add(1, Ordering::Relaxed);
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
        self.push_priority(DEFAULT_HIGH_PRIORITY_BOUNDED, task)
    }

    #[inline]
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>> {
        let task = unsafe { (&mut *self
            .stack
            .get())
            .pop()
        };
        if task.is_some() {
            //指定工作者的任务栈有任务，则立即返回任务
            self.consume_count.fetch_add(1, Ordering::Relaxed);
            return task;
        }

        //从指定工作者的任务队列中弹出任务
        let task = try_pop_by_weight(self);
        if task.is_some() {
            self
                .consume_count
                .fetch_add(1, Ordering::Relaxed);
        }
        task
    }

    #[inline]
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>> {
        let mut all = Vec::with_capacity(self.len());

        let internal = unsafe { (&mut *self.internal.get()) };
        for _ in 0..internal.len() {
            if let Some(task) = internal.pop_front() {
                all.push(task);
            }
        }

        let public_len = self.public.len();
        for _ in 0..public_len {
            if let Some(task) = self.public.pop() {
                all.push(task);
            }
        }

        all.into_iter()
    }

    #[inline]
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        self.thread_waker.as_ref()
    }
}

// 尝试通过统计信息更新权重，根据权重选择从本地外部任务队列或本地内部任务队列中弹出任务
fn try_pop_by_weight<O: Default + 'static>(pool: &SingleTaskPool<O>)
    -> Option<Arc<AsyncTask<SingleTaskPool<O>, O>>> {
    unsafe {
        //根据权重选择从指定的任务队列弹出任务
        match (&mut *pool.selector.get()).select() {
            0 => {
                //弹出外部任务
                let task = try_pop_external(pool);
                if task.is_some() {
                    task
                } else {
                    //当前没有外部任务，则尝试弹出内部任务
                    try_pop_internal(pool)
                }
            },
            _ => {
                //弹出内部任务
                let task = try_pop_internal(pool);
                if task.is_some() {
                    task
                } else {
                    //当前没有内部任务，则尝试弹出外部任务
                    try_pop_external(pool)
                }
            },
        }
    }
}

// 尝试弹出内部任务队列的任务
#[inline]
fn try_pop_internal<O: Default + 'static>(pool: &SingleTaskPool<O>)
    -> Option<Arc<AsyncTask<SingleTaskPool<O>, O>>> {
    unsafe { (&mut *pool.internal.get()).pop_front() }
}

// 尝试弹出外部任务队列的任务
#[inline]
fn try_pop_external<O: Default + 'static>(pool: &SingleTaskPool<O>)
    -> Option<Arc<AsyncTask<SingleTaskPool<O>, O>>> {
    pool.public.pop()
}

impl<O: Default + 'static> AsyncTaskPoolExt<O> for SingleTaskPool<O> {
    fn set_thread_waker(&mut self, thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>) {
        self.thread_waker = Some(thread_waker);
    }
}

impl<O: Default + 'static> SingleTaskPool<O> {
    /// 构建指定权重的单线程任务池
    pub fn new(weights: [u8; 2]) -> Self {
        let rt_uid = alloc_rt_uid();
        let public = SegQueue::new();
        let internal = UnsafeCell::new(VecDeque::new());
        let stack = UnsafeCell::new(Vec::with_capacity(1));
        let selector = UnsafeCell::new(IWRRSelector::new(weights));
        let consume_count = AtomicUsize::new(0);
        let produce_count = AtomicUsize::new(0);

        SingleTaskPool {
            id: (rt_uid << 8) & 0xffff | 1,
            public,
            internal,
            stack,
            selector,
            consume_count,
            produce_count,
            thread_waker: Some(Arc::new((
                AtomicBool::new(false),
                Mutex::new(()),
                Condvar::new(),
            ))),
        }
    }
}

///
/// 异步单线程任务运行时
///
pub struct SingleTaskRuntime<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = SingleTaskPool<O>,
>(
    Arc<(
        usize,                                  //运行时唯一id
        Arc<P>,                                 //异步任务池
        Sender<(usize, AsyncTimingTask<P, O>)>, //休眠的异步任务生产者
        AsyncTaskTimer<P, O>,                   //本地定时器
        AtomicUsize,                            //定时器任务生产计数
        AtomicUsize,                            //定时器任务消费计数
    )>,
);

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for SingleTaskRuntime<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for SingleTaskRuntime<O, P>
{
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Clone
    for SingleTaskRuntime<O, P>
{
    fn clone(&self) -> Self {
        SingleTaskRuntime(self.0.clone())
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> AsyncRuntime<O>
    for SingleTaskRuntime<O, P>
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
            .4
            .load(Ordering::Relaxed)
            .checked_sub((self.0).5.load(Ordering::Relaxed))
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
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_local_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个指定优先级的异步任务到异步运行时
    fn spawn_priority<F>(&self, priority: usize, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_priority_by_id(task_id.clone(), priority, future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_yield_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing<F>(&self, future: F, time: usize) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_timing_by_id(task_id.clone(), future, time) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时
    fn spawn_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        if let Err(e) = (self.0).1.push(Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
            Some(future.boxed_local()),
        ))) {
            return Err(Error::new(ErrorKind::Other, e));
        }

        Ok(())
    }

    /// 派发一个指定任务唯一id的异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        (self.0).1.push_local(Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            DEFAULT_HIGH_PRIORITY_BOUNDED,
            Some(future.boxed_local()))))
    }

    /// 派发一个指定任务唯一id和任务优先级的异步任务到异步运行时
    fn spawn_priority_by_id<F>(&self,
                               task_id: TaskId,
                               priority: usize,
                               future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        (self.0).1.push_priority(priority, Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            priority,
            Some(future.boxed_local()))))
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时，并立即让出任务的当前运行
    #[inline]
    fn spawn_yield_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
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
            F: Future<Output = O> + 'static {
        let rt = self.clone();
        self.spawn_by_id(task_id, async move {
            (rt.0).3.set_timer(
                AsyncTimingTask::WaitRun(Arc::new(AsyncTask::new(
                    rt.alloc::<F::Output>(),
                    (rt.0).1.clone(),
                    DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                    Some(future.boxed_local()),
                ))),
                time,
            );

            (rt.0).4.fetch_add(1, Ordering::Relaxed);
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
    fn wait<V: 'static>(&self) -> AsyncWait<V> {
        AsyncWait::new(self.wait_any(2))
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，其中任意一个任务完成，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成的任务的值将被忽略
    fn wait_any<V: 'static>(&self, capacity: usize) -> AsyncWaitAny<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAny::new(capacity, producor, consumer)
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，任务返回后需要通过用户指定的检查回调进行检查，其中任意一个任务检查通过，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成或未检查通过的任务的值将被忽略，如果所有任务都未检查通过，则强制唤醒当前运行时的当前任务
    fn wait_any_callback<V: 'static>(&self, capacity: usize) -> AsyncWaitAnyCallback<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAnyCallback::new(capacity, producor, consumer)
    }

    /// 构建用于派发多个异步任务到指定运行时的映射归并，需要指定映射归并的容量
    fn map_reduce<V: 'static>(&self, capacity: usize) -> AsyncMapReduce<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncMapReduce::new(0, capacity, producor, consumer)
    }

    /// 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    fn timeout(&self, timeout: usize) -> LocalBoxFuture<'static, ()> {
        let rt = self.clone();
        let producor = (self.0).2.clone();

        AsyncWaitTimeout::new(rt, producor, timeout).boxed_local()
    }

    /// 立即让出当前任务的执行
    fn yield_now(&self) -> LocalBoxFuture<'static, ()> {
        async move {
            YieldNow(false).await;
        }.boxed_local()
    }

    /// 生成一个异步管道，输入指定流，输入流的每个值通过过滤器生成输出流的值
    fn pipeline<S, SO, F, FO>(&self, input: S, mut filter: F) -> LocalBoxStream<'static, FO>
    where
        S: Stream<Item = SO> + 'static,
        SO: 'static,
        F: FnMut(SO) -> AsyncPipelineResult<FO> + 'static,
        FO: 'static,
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

        output.boxed_local()
    }

    /// 关闭异步运行时，返回请求关闭是否成功
    fn close(&self) -> bool {
        false
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> AsyncRuntimeExt<O>
    for SingleTaskRuntime<O, P>
{
    fn spawn_with_context<F, C>(&self, task_id: TaskId, future: F, context: C) -> Result<()>
    where
        F: Future<Output = O> + 'static,
        C: 'static,
    {
        if let Err(e) = (self.0).1.push(Arc::new(AsyncTask::with_context(
            task_id,
            (self.0).1.clone(),
            DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
            Some(future.boxed_local()),
            context,
        ))) {
            return Err(Error::new(ErrorKind::Other, e));
        }

        Ok(())
    }

    fn spawn_timing_with_context<F, C>(
        &self,
        task_id: TaskId,
        future: F,
        context: C,
        time: usize,
    ) -> Result<()>
    where
        F: Future<Output = O> + 'static,
        C: 'static,
    {
        let rt = self.clone();
        self.spawn_by_id(task_id, async move {
            (rt.0).3.set_timer(
                AsyncTimingTask::WaitRun(Arc::new(AsyncTask::with_context(
                    rt.alloc::<F::Output>(),
                    (rt.0).1.clone(),
                    DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                    Some(future.boxed_local()),
                    context,
                ))),
                time,
            );

            (rt.0).4.fetch_add(1, Ordering::Relaxed);
            Default::default()
        })
    }

    fn block_on<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + 'static,
        <F as Future>::Output: Default + 'static {
        let runner = SingleTaskRunner {
            is_running: AtomicBool::new(true),
            runtime: self.clone(),
            clock: Clock::new(),
        };
        let mut result: Option<<F as Future>::Output> = None;
        let result_raw = (&mut result) as *mut Option<<F as Future>::Output> as usize;

        self.spawn(async move {
            //在指定运行时中执行，并返回结果
            let r = future.await;
            unsafe {
                *(result_raw as *mut Option<<F as Future>::Output>) = Some(r);
            }

            Default::default()
        });

        loop {
            //执行异步任务
            while runner.run()? > 0 {}

            //尝试获取异步任务的执行结果
            if let Some(result) = result.take() {
                //异步任务已完成，则立即返回执行结果
                return Ok(result);
            }
        }
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    SingleTaskRuntime<O, P>
{
    /// 获取当前单线程异步运行时的本地异步运行时
    pub fn to_local_runtime(&self) -> LocalAsyncRuntime<O> {
        LocalAsyncRuntime::new(
            self.as_raw(),
            SingleTaskRuntime::<O, P>::get_id_raw,
            SingleTaskRuntime::<O, P>::spawn_raw,
            SingleTaskRuntime::<O, P>::spawn_timing_raw,
            SingleTaskRuntime::<O, P>::timeout_raw,
        )
    }

    // 获取当前单线程异步运行时的指针
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
                    Sender<(usize, AsyncTimingTask<P, O>)>,
                    AsyncTaskTimer<P, O>,
                    AtomicUsize,
                    AtomicUsize,
                ),
            )
        };
        SingleTaskRuntime(inner)
    }

    // 获取当前异步运行时的唯一id
    pub(crate) fn get_id_raw(raw: *const ()) -> usize {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let id = rt.get_id();
        Arc::into_raw(rt.0); //避免提前释放
        id
    }

    // 派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_raw(raw: *const (), future: LocalBoxFuture<'static, O>) -> Result<()> {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_by_id(rt.alloc::<O>(), future);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 定时派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_timing_raw(
        raw: *const (),
        future: LocalBoxFuture<'static, O>,
        timeout: usize,
    ) -> Result<()> {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_timing_by_id(rt.alloc::<O>(), future, timeout);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    pub(crate) fn timeout_raw(raw: *const (), timeout: usize) -> LocalBoxFuture<'static, ()> {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let boxed = rt.timeout(timeout);
        Arc::into_raw(rt.0); //避免提前释放
        boxed
    }
}

///
/// 单线程异步任务执行器
///
pub struct SingleTaskRunner<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = SingleTaskPool<O>,
> {
    is_running: AtomicBool,                 //是否开始运行
    runtime:    SingleTaskRuntime<O, P>,    //异步单线程任务运行时
    clock:      Clock,                      //执行器的时钟
}

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for SingleTaskRunner<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for SingleTaskRunner<O, P>
{
}

impl<O: Default + 'static> Default for SingleTaskRunner<O> {
    fn default() -> Self {
        SingleTaskRunner::new(SingleTaskPool::default())
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    SingleTaskRunner<O, P>
{
    /// 用指定的任务池构建单线程异步运行时
    pub fn new(pool: P) -> Self {
        let rt_uid = pool.get_thread_id() >> 32;
        let pool = Arc::new(pool);

        //构建本地定时器和定时异步任务生产者
        let timer = AsyncTaskTimer::new();
        let producor = timer.get_producor().clone();
        let timer_producor_count = AtomicUsize::new(0);
        let timer_consume_count = AtomicUsize::new(0);

        //构建单线程任务运行时
        let runtime = SingleTaskRuntime(Arc::new((rt_uid,
                                                  pool,
                                                  producor,
                                                  timer,
                                                  timer_producor_count,
                                                  timer_consume_count)));

        SingleTaskRunner {
            is_running: AtomicBool::new(false),
            runtime,
            clock: Clock::new(),
        }
    }

    /// 获取单线程异步任务执行器的线程唤醒器
    pub fn get_thread_waker(&self) -> Option<Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        (self.runtime.0).1.get_thread_waker().cloned()
    }

    /// 启动单线程异步任务执行器
    pub fn startup(&self) -> Option<SingleTaskRuntime<O, P>> {
        if cfg!(target_arch = "aarch64") {
            match self
                .is_running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(false) => {
                    //未启动，则启动，并返回单线程异步运行时
                    Some(self.runtime.clone())
                }
                _ => {
                    //已启动，则忽略
                    None
                }
            }
        } else {
            match self.is_running.compare_exchange_weak(
                false,
                true,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(false) => {
                    //未启动，则启动，并返回单线程异步运行时
                    Some(self.runtime.clone())
                }
                _ => {
                    //已启动，则忽略
                    None
                }
            }
        }
    }

    /// 运行一次单线程异步任务执行器，返回当前任务池中任务的数量
    pub fn run_once(&self) -> Result<usize> {
        if !self.is_running.load(Ordering::Relaxed) {
            //未启动，则返回错误原因
            return Err(Error::new(
                ErrorKind::Other,
                "Single thread runtime not running",
            ));
        }

        //设置新的定时任务，并唤醒已过期的定时任务
        let mut pop_len = 0;
        (self.runtime.0)
            .4
            .fetch_add((self.runtime.0).3.consume(),
                       Ordering::Relaxed);
        loop {
            let current_time = (self.runtime.0).3.is_require_pop();
            if let Some(current_time) = current_time {
                //当前有到期的定时异步任务，则只处理到期的一个定时异步任务
                let timed_out = (self.runtime.0).3.pop(current_time);
                if let Some((_handle, timing_task)) = timed_out {
                    match timing_task {
                        AsyncTimingTask::Pended(expired) => {
                            //唤醒休眠的异步任务，并立即执行
                            self.runtime.wakeup::<O>(&expired);
                            if let Some(task) = (self.runtime.0).1.try_pop() {
                                run_task(task);
                            }
                        }
                        AsyncTimingTask::WaitRun(expired) => {
                            //立即执行到期的定时异步任务，并立即执行
                            (self.runtime.0).1.push_priority(DEFAULT_MAX_HIGH_PRIORITY_BOUNDED, expired);
                            if let Some(task) = (self.runtime.0).1.try_pop() {
                                run_task(task);
                            }
                        }
                    }
                    pop_len += 1;
                }
            } else {
                //当前没有到期的定时异步任务，则退出本次定时异步任务处理
                break;
            }
        }
        (self.runtime.0)
            .5
            .fetch_add(pop_len,
                       Ordering::Relaxed);

        //继续执行当前任务池中的一个异步任务
        match (self.runtime.0).1.try_pop() {
            None => {
                //当前没有异步任务，则立即返回
                return Ok(0);
            }
            Some(task) => {
                run_task(task);
            }
        }

        Ok((self.runtime.0).1.len())
    }

    /// 运行单线程异步任务执行器，并执行任务池中的所有任务
    pub fn run(&self) -> Result<usize> {
        if !self.is_running.load(Ordering::Relaxed) {
            //未启动，则返回错误原因
            return Err(Error::new(
                ErrorKind::Other,
                "Single thread runtime not running",
            ));
        }

        loop {
            //设置新的定时任务，并唤醒已过期的定时任务
            let mut pop_len = 0;
            let mut start_run_millis = self.clock.recent(); //重置开运行时长
            (self.runtime.0)
                .4
                .fetch_add((self.runtime.0).3.consume(),
                           Ordering::Relaxed);
            loop {
                let current_time = (self.runtime.0).3.is_require_pop();
                if let Some(current_time) = current_time {
                    //当前有到期的定时异步任务，则只处理到期的一个定时异步任务
                    let timed_out = (self.runtime.0).3.pop(current_time);
                    if let Some((handle, timing_task)) = timed_out {
                        match timing_task {
                            AsyncTimingTask::Pended(expired) => {
                                //唤醒休眠的异步任务，并立即执行
                                self.runtime.wakeup::<O>(&expired);
                                if let Some(task) = (self.runtime.0).1.try_pop() {
                                    run_task(task);
                                }
                            }
                            AsyncTimingTask::WaitRun(expired) => {
                                //立即执行到期的定时异步任务，并立即执行
                                (self.runtime.0).1.push_priority(handle, expired);
                                if let Some(task) = (self.runtime.0).1.try_pop() {
                                    run_task(task);
                                }
                            }
                        }
                        pop_len += 1;
                    }
                } else {
                    //当前没有到期的定时异步任务，则退出本次定时异步任务处理
                    break;
                }
            }
            (self.runtime.0)
                .5
                .fetch_add(pop_len,
                           Ordering::Relaxed);

            //继续执行当前任务池中的一个异步任务
            while self
                .clock
                .recent()
                .duration_since(start_run_millis)
                .as_millis() < 1 {
                match (self.runtime.0).1.try_pop() {
                    None => {
                        //当前没有异步任务，则立即返回
                        return Ok((self.runtime.0).1.len());
                    }
                    Some(task) => {
                        run_task(task);
                    }
                }
            }
        }
    }

    /// 转换为本地异步单线程任务运行时
    pub fn into_local(self) -> SingleTaskRuntime<O, P> {
        self.runtime
    }
}

//执行异步任务
#[inline]
fn run_task<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    task: Arc<AsyncTask<P, O>>,
) {
    let waker = waker_ref(&task);
    let mut context = Context::from_waker(&*waker);
    if let Some(mut future) = task.get_inner() {
        if let Poll::Pending = future.as_mut().poll(&mut context) {
            //当前未准备好，则恢复异步任务，以保证异步服务后续访问异步任务和异步任务不被提前释放
            task.set_inner(Some(future));
        }
    }
}
