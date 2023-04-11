use std::future::Future;
use std::time::Duration;
use std::task::{Poll, Waker};
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use parking_lot::{Mutex, Condvar};
use futures::{future::{FutureExt, LocalBoxFuture},
              stream::{Stream, LocalBoxStream}};
use crossbeam_channel::Sender;

use crate::rt::{TaskId, AsyncPipelineResult,
                serial::{AsyncTask,
                         AsyncTimingTask,
                         AsyncTaskTimer,
                         AsyncRuntime,
                         AsyncRuntimeExt,
                         AsyncTaskPool,
                         AsyncTaskPoolExt,
                         AsyncWait,
                         AsyncWaitAny,
                         AsyncWaitAnyCallback,
                         AsyncMapReduce,
                         LocalAsyncRuntime,
                         spawn_worker_thread, wakeup_worker_thread},
                YieldNow,
                serial_single_thread::{SingleTaskPool, SingleTaskRunner, SingleTaskRuntime}};

///
/// 工作者异步运行时
///
pub struct WorkerRuntime<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = SingleTaskPool<O>,
>(Arc<(
    Arc<AtomicBool>,                        //工作者状态
    Arc<(AtomicBool, Mutex<()>, Condvar)>,  //工作者线程唤醒器
    SingleTaskRuntime<O, P>                 //单线程运行时
)>);

unsafe impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
> Send for WorkerRuntime<O, P> {}
unsafe impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
> Sync for WorkerRuntime<O, P> {}

impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
> Clone for WorkerRuntime<O, P> {
    fn clone(&self) -> Self {
        WorkerRuntime(self.0.clone())
    }
}

impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> AsyncRuntime<O> for WorkerRuntime<O, P> {
    type Pool = P;

    /// 共享运行时内部任务池
    #[inline]
    fn shared_pool(&self) -> Arc<Self::Pool> {
        (self.0).2.shared_pool()
    }

    /// 获取当前异步运行时的唯一id
    #[inline]
    fn get_id(&self) -> usize {
        (self.0).2.get_id()
    }

    /// 获取当前异步运行时待处理任务数量
    #[inline]
    fn wait_len(&self) -> usize {
        (self.0).2.wait_len()
    }

    /// 获取当前异步运行时任务数量
    #[inline]
    fn len(&self) -> usize {
        (self.0).2.len()
    }

    /// 分配异步任务的唯一id
    #[inline]
    fn alloc<R: 'static>(&self) -> TaskId {
        (self.0).2.alloc::<R>()
    }

    /// 派发一个指定的异步任务到异步运行时
    fn spawn<F>(&self, future: F) -> Result<TaskId>
        where F: Future<Output = O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn async task failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn(future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn local async task failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_local(future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个指定优先级的异步任务到异步运行时
    fn spawn_priority<F>(&self, priority: usize, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn priority async task failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_priority(priority, future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn yield priority async task failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_yield(future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing<F>(&self, future: F, time: usize) -> Result<TaskId>
        where F: Future<Output = O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn timing async task failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_timing(future, time);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时
    fn spawn_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn async task by id failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_by_id(task_id, future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个指定任务唯一id的异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn local async task by id failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_local_by_id(task_id, future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个指定任务唯一id和任务优先级的异步任务到异步运行时
    fn spawn_priority_by_id<F>(&self,
                               task_id: TaskId,
                               priority: usize,
                               future: F) -> Result<()>
        where
            F: Future<Output=O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn priority async task by id failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_priority_by_id(task_id, priority, future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn yield async task by id failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_yield_by_id(task_id, future);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 派发一个指定任务唯一id和在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing_by_id<F>(&self,
                             task_id: TaskId,
                             future: F,
                             time: usize) -> Result<()>
        where
            F: Future<Output=O> + 'static {
        if !(self.0).0.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Other, "Spawn timing async task by id failed, reason: worker already closed"));
        }

        let result = (self.0).2.spawn_timing_by_id(task_id, future, time);
        wakeup_worker_thread(&(self.0).1, &(self.0).2);
        result
    }

    /// 挂起指定唯一id的异步任务
    #[inline]
    fn pending<Output: 'static>(&self, task_id: &TaskId, waker: Waker) -> Poll<Output> {
        (self.0).2.pending::<Output>(task_id, waker)
    }

    /// 唤醒指定唯一id的异步任务
    #[inline]
    fn wakeup<Output: 'static>(&self, task_id: &TaskId) {
        (self.0).2.wakeup::<Output>(task_id);
    }

    /// 挂起当前异步运行时的当前任务，并在指定的其它运行时上派发一个指定的异步任务，等待其它运行时上的异步任务完成后，唤醒当前运行时的当前任务，并返回其它运行时上的异步任务的值
    #[inline]
    fn wait<V: 'static>(&self) -> AsyncWait<V> {
        (self.0).2.wait()
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，其中任意一个任务完成，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成的任务的值将被忽略
    #[inline]
    fn wait_any<V: 'static>(&self, capacity: usize) -> AsyncWaitAny<V> {
        (self.0).2.wait_any(capacity)
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，任务返回后需要通过用户指定的检查回调进行检查，其中任意一个任务检查通过，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成或未检查通过的任务的值将被忽略，如果所有任务都未检查通过，则强制唤醒当前运行时的当前任务
    #[inline]
    fn wait_any_callback<V: 'static>(&self, capacity: usize) -> AsyncWaitAnyCallback<V> {
        (self.0).2.wait_any_callback(capacity)
    }

    /// 构建用于派发多个异步任务到指定运行时的映射归并，需要指定映射归并的容量
    #[inline]
    fn map_reduce<V: 'static>(&self, capacity: usize) -> AsyncMapReduce<V> {
        (self.0).2.map_reduce(capacity)
    }

    /// 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    #[inline]
    fn timeout(&self, timeout: usize) -> LocalBoxFuture<'static, ()> {
        (self.0).2.timeout(timeout)
    }

    /// 立即让出当前任务的执行
    #[inline]
    fn yield_now(&self) -> LocalBoxFuture<'static, ()> {
        (self.0).2.yield_now()
    }

    /// 生成一个异步管道，输入指定流，输入流的每个值通过过滤器生成输出流的值
    #[inline]
    fn pipeline<S, SO, F, FO>(&self, input: S, mut filter: F) -> LocalBoxStream<'static, FO>
        where S: Stream<Item = SO> + 'static,
              SO: 'static,
              F: FnMut(SO) -> AsyncPipelineResult<FO> + 'static,
              FO: 'static {
        (self.0).2.pipeline(input, filter)
    }

    /// 关闭异步运行时，返回请求关闭是否成功
    fn close(&self) -> bool {
        if cfg!(target_arch = "aarch64") {
            if let Ok(true) = (self.0).0.compare_exchange(true,
                                                          false,
                                                          Ordering::SeqCst,
                                                          Ordering::SeqCst) {
                //设置工作者状态成功，检查运行时所在线程是否需要唤醒
                wakeup_worker_thread(&(self.0).1, &(self.0).2);
                true
            } else {
                false
            }
        } else {
            if let Ok(true) = (self.0).0.compare_exchange_weak(true,
                                                               false,
                                                               Ordering::SeqCst,
                                                               Ordering::SeqCst) {
                //设置工作者状态成功，检查运行时所在线程是否需要唤醒
                wakeup_worker_thread(&(self.0).1, &(self.0).2);
                true
            } else {
                false
            }
        }
    }
}

impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> AsyncRuntimeExt<O> for WorkerRuntime<O, P> {
    #[inline]
    fn spawn_with_context<F, C>(&self,
                                task_id: TaskId,
                                future: F,
                                context: C) -> Result<()>
        where F: Future<Output = O> + 'static,
              C: 'static {
        (self.0).2.spawn_with_context(task_id, future, context)
    }

    #[inline]
    fn spawn_timing_with_context<F, C>(&self,
                                       task_id: TaskId,
                                       future: F,
                                       context: C,
                                       time: usize) -> Result<()>
        where F: Future<Output = O> + 'static,
              C: 'static {
        (self.0).2.spawn_timing_with_context(task_id, future, context, time)
    }

    #[inline]
    fn block_on<F>(&self, future: F) -> Result<F::Output>
        where F: Future + 'static,
              <F as Future>::Output: Default + 'static {
        (self.0).2.block_on::<F>(future)
    }
}

impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> WorkerRuntime<O, P> {
    /// 获取工作者异步运行时的工作者状态
    pub fn get_worker_status(&self) -> &Arc<AtomicBool> {
        &(self.0).0
    }

    /// 获取工作者异步运行时的工作者线程状态
    pub fn get_worker_waker(&self) -> &Arc<(AtomicBool, Mutex<()>, Condvar)> {
        &(self.0).1
    }

    /// 获取工作者异步运行时的单线程异步运行时
    pub fn get_worker_runtime(&self) -> &SingleTaskRuntime<O, P> {
        &(self.0).2
    }

    /// 获取当前工作者异步运行时的本地异步运行时
    pub fn to_local_runtime(&self) -> LocalAsyncRuntime<O> {
        LocalAsyncRuntime::new(
            self.as_raw(),
            WorkerRuntime::<O, P>::get_id_raw,
            WorkerRuntime::<O, P>::spawn_raw,
            WorkerRuntime::<O, P>::spawn_timing_raw,
            WorkerRuntime::<O, P>::timeout_raw
        )
    }

    // 获取当前工作者异步运行时的指针
    #[inline]
    pub(crate) fn as_raw(&self) -> *const () {
        Arc::into_raw(self.0.clone()) as *const ()
    }

    // 获取指定指针的工作者异步运行时
    #[inline]
    pub(crate) fn from_raw(raw: *const ()) -> Self {
        let inner = unsafe {
            Arc::from_raw(raw as *const (
                Arc<AtomicBool>,
                Arc<(AtomicBool, Mutex<()>, Condvar)>,
                SingleTaskRuntime<O, P>),
            )
        };
        WorkerRuntime(inner)
    }

    // 获取当前异步运行时的唯一id
    pub(crate) fn get_id_raw(raw: *const ()) -> usize {
        let rt = WorkerRuntime::<O, P>::from_raw(raw);
        let id = rt.get_id();
        Arc::into_raw(rt.0); //避免提前释放
        id
    }

    // 派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_raw(raw: *const (),
                            future: LocalBoxFuture<'static, O>) -> Result<()> {
        let rt = WorkerRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_by_id(rt.alloc::<O>(), future);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 定时派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_timing_raw(raw: *const (),
                                   future: LocalBoxFuture<'static, O>,
                                   timeout: usize) -> Result<()> {
        let rt = WorkerRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_timing_by_id(rt.alloc::<O>(), future, timeout);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    pub(crate) fn timeout_raw(raw: *const (),
                              timeout: usize) -> LocalBoxFuture<'static, ()> {
        let rt = WorkerRuntime::<O, P>::from_raw(raw);
        let boxed = rt.timeout(timeout);
        Arc::into_raw(rt.0); //避免提前释放
        boxed
    }
}

///
/// 工作者任务执行器
///
pub struct WorkerTaskRunner<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P> = SingleTaskPool<O>,
>(Arc<(
    Arc<AtomicBool>,                        //工作者状态
    Arc<(AtomicBool, Mutex<()>, Condvar)>,  //工作者线程唤醒器
    SingleTaskRunner<O, P>,                 //单线程异步任务执行器
    WorkerRuntime<O, P>,                    //工作者运行时
)>);

unsafe impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> Send for WorkerTaskRunner<O, P> {}
unsafe impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> Sync for WorkerTaskRunner<O, P> {}

impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> Clone for WorkerTaskRunner<O, P> {
    fn clone(&self) -> Self {
        WorkerTaskRunner(self.0.clone())
    }
}

impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> From<(Arc<AtomicBool>, Arc<(AtomicBool, Mutex<()>, Condvar)>, SingleTaskRuntime<O, P>)> for WorkerRuntime<O, P> {
    //将外部的工作者状态，工作者线程唤醒器和指定任务池的单线程异步运行时转换成工作者异步运行时
    fn from(from: (Arc<AtomicBool>,
                   Arc<(AtomicBool, Mutex<()>, Condvar)>,
                   SingleTaskRuntime<O, P>,)) -> Self {
        WorkerRuntime(Arc::new(from))
    }
}

impl<O: Default + 'static> Default for WorkerTaskRunner<O> {
    fn default() -> Self {
        WorkerTaskRunner::new(SingleTaskPool::default(),
                              Arc::new(AtomicBool::new(true)),
                              Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new())))
    }
}

impl<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
> WorkerTaskRunner<O, P> {
    /// 用指定的任务池构建工作者任务执行器
    pub fn new(pool: P,
               worker_status: Arc<AtomicBool>,
               worker_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>) -> Self {
        let runner = SingleTaskRunner::new(pool);
        let rt = runner.startup().unwrap();
        let inner = (worker_status.clone(), worker_waker.clone(), rt);
        let runtime = WorkerRuntime(Arc::new(inner));

        let inner = (worker_status,
                     worker_waker,
                     runner,
                     runtime);

        WorkerTaskRunner(Arc::new(inner))
    }

    /// 获取当前工作者异步任务执行器的工作者运行时
    pub fn get_runtime(&self) -> WorkerRuntime<O, P> {
        (self.0).3.clone()
    }

    /// 运行一次工作者异步任务执行器，返回当前任务池中任务的数量
    #[inline]
    pub fn run_once(&self) -> Result<usize> {
        (self.0).2.run_once()
    }

    /// 运行单线程异步任务执行器，并执行任务池中的所有任务
    #[inline]
    pub fn run(&self) -> Result<usize> {
        (self.0).2.run()
    }

    /// 启动工作者异步任务执行器
    pub fn startup<LF, GQL>(self,
                            thread_name: &str,
                            thread_stack_size: usize,
                            sleep_timeout: u64,
                            loop_interval: Option<u64>,
                            loop_func: LF,
                            get_queue_len: GQL) -> WorkerRuntime<O, P>
        where P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
              LF: Fn() -> (bool, Duration) + Send + 'static,
              GQL: Fn() -> usize + Send + 'static{
        let rt_copy = (self.0).3.clone();
        let thread_handler = (self.0).0.clone();
        let thread_waker = (self.0).1.clone();
        spawn_worker_thread(
            thread_name,
            thread_stack_size,
            thread_handler,
            thread_waker,
            sleep_timeout,
            loop_interval,
            loop_func,
            move || {
                rt_copy.wait_len() + get_queue_len()
            },
        );

        (self.0).3.clone()
    }
}