//! # 基于Future(MVP)，用于为外部提供基础的通用异步运行时和工具
//!
//! ## 主要特征
//! - 可定制的[任务池]，
//! - 外部使用[任务ID]可以很方便的[唤醒]和[挂起]
//! - 抽象接口，可以自由实现自己的[运行时]，
//! - [单线程运行时推动]可以由自己推动运行。
//!
//! [任务池]: rt/trait.AsyncTaskPool.html
//! [任务ID]: rt/struct.TaskId.html
//! [运行时]: rt/trait.AsyncRuntime.html
//! [单线程运行时推动]: rt/single_thread/struct.SingleTaskRunner.html#method.run
//! [唤醒]: rt/trait.AsyncRuntime.html#tymethod.wakeup
//! [挂起]: rt/trait.AsyncRuntime.html#tymethod.pending
//!
//! # Examples
//!
//! 本地异步运行时:
//! ```
//! use pi_async::rt::{AsyncRuntime, AsyncRuntimeExt, serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}};
//! let rt = LocalTaskRunner::<()>::new().into_local();
//! let _ = rt.block_on(async move {});
//! ```
//!
//! 多线程异步运行时使用:
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
//!
//! # Features
//!
//! - ```serial```: 用于开启单线程顺序任务

#![allow(warnings)]
#![feature(panic_info_message)]
#![feature(allocator_api)]
#![feature(alloc_error_hook)]
#![feature(thread_id_value)]
#![feature(negative_impls)]

pub mod lock;
pub mod prelude;
pub mod rt;

mod tests;
