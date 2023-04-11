基于Future(MVP)，用于为外部提供基础的通用异步运行时和工具

# 主要特征

- 任务池: 可定制的任务池
- 任务ID: 外部使用任务ID可以很方便的唤醒和挂起
- 抽象接口: 可以自由实现自己的运行时
- 运行时推动：单线程运行时可以用自己的方式推动运行

# Examples

本地异步运行时:
```
 use pi_async::rt::{AsyncRuntime, AsyncRuntimeExt, serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}};
 let rt = LocalTaskRunner::<()>::new().into_local();
 let _ = rt.block_on(async move {});
```

多线程异步运行时使用:
```
 use pi_async::prelude::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool};
 use pi_async::rt::AsyncRuntimeExt;

 let pool = StealableTaskPool::with(4,100000,[1, 254],3000);
 let builer = MultiTaskRuntimeBuilder::new(pool)
     .set_timer_interval(1)
     .init_worker_size(4)
     .set_worker_limit(4, 4);
 let rt = builer.build();
 let _ = rt.spawn(async move {});
```

# 基准测试

## 云服务平台
- 16核(vCPU) 2.5 GHz主频、3.2 GHz睿频的Intel ® Xeon ® Platinum 8269CY（Cascade Lake）
- 内存:64G
- CentOS 7.3 64位


|项目|pi_async|async_std|tokio|备注|
|---|---|---|---|---|
|bench_async_mutex|3,266 ns/iter (+/- 136)|149,332 ns/iter (+/- 7,212)|6,374,238 ns/iter (+/- 861,432)||
|contention|338,786 ns/iter (+/- 68,222)|901,779 ns/iter (+/- 28,380)|2,157,495 ns/iter (+/- 38,100)||
|create|257 ns/iter (+/- 2)|61 ns/iter (+/- 0)|63 ns/iter (+/- 0)||
|no_contention|215,515 ns/iter (+/- 1,121)|225,034 ns/iter (+/- 740)|550,285 ns/iter (+/- 2,313)||
|await_empty_many|605,232 ns/iter (+/- 125,354)|394,823 ns/iter (+/- 19,107)|393,625 ns/iter (+/- 4,459)||
|chained_spawn|504,570 ns/iter (+/- 24,166)|1,090,176 ns/iter (+/- 27,817)|251,943 ns/iter (+/- 1,412)||
|ping_pong|1,176,361 ns/iter (+/- 197,786)|3,859,845 ns/iter (+/- 73,410)|1,193,711 ns/iter (+/- 20,376)||
|spawn_empty_many|4,187,949 ns/iter (+/- 587,053)|18,887,015 ns/iter (+/- 347,589)|9,941,412 ns/iter (+/- 659,722)||
|spawn_many|3,436,761 ns/iter (+/- 279,137)|19,001,495 ns/iter (+/- 380,355)|7,615,952 ns/iter (+/- 210,639)||
|spawn_one_to_one|6,205,756 ns/iter (+/- 826,745)|36,189,628 ns/iter (+/- 357,690)|16,620,075 ns/iter (+/- 589,085)||
|yield_many|23,757,528 ns/iter (+/- 4,110,213)|52,304,694 ns/iter (+/- 519,928)|17,746,497 ns/iter (+/- 550,878)||
|block_on|83 ns/iter (+/- 0)|2,593 ns/iter (+/- 48)|178 ns/iter (+/- 1)||
|local_run|666,627 ns/iter (+/- 5,476)||||
|local_send_many|5,885,537 ns/iter (+/- 98,251)||||
|local_spawn_many|1,260,102 ns/iter (+/- 5,423)|20,201,034 ns/iter (+/- 692,642)|1,553,246 ns/iter (+/- 49,815)||

# 贡献指南

# License

This project is licensed under the [MIT license].

[MIT license]: LICENSE

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in pi_async by you, shall be licensed as MIT, without any additional
terms or conditions.