use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

/// 这个代码块用来实现 AtomicCounterstruct。
/// 它使用 AtomicUsize 成员将累计的数量存储在每一次的调用中，
/// 并且以 Arc<atmoicBool> 类型的成员存储累计值的正确性。
/// new() 方法被用来初始化这个结构体，
/// 其参数为允许的最大正确累计数。
/// 而 Drop 方法用来检查是每一次调用后得到的正确累计数是否等于初始化时的累计值，
/// 如果不相等便抛出一个assertion 错误。
pub(crate) struct AtomicCounter(AtomicUsize, Instant, Arc<AtomicBool>, usize);
impl Drop for AtomicCounter {
    fn drop(&mut self) {
        // 设置 `self.2` 为 `true`，表示任务已结束
        self.2.store(true, Ordering::Relaxed);
        // 检查 `self.0` 是否匹配正确计数 `self.3`
        assert_eq!(self.0.load(Ordering::Relaxed), self.3);
    }
}

impl AtomicCounter {
    pub(crate) fn new(correct_count: usize) -> Self {
        let now = Instant::now();
        AtomicCounter(AtomicUsize::new(0), now, Arc::new(AtomicBool::new(false)), correct_count)
    }

    pub(crate) fn fetch_add(&self, val: usize) -> usize {
        self.0.fetch_add(val, Ordering::Relaxed)
    }

    pub(crate) fn get_state(&self) -> Arc<AtomicBool> {
        self.2.clone()
    }
}
