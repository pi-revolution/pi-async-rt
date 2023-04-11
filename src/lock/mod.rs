//! # 提供了异步运行时需要的队列和锁
//!

use std::hint::spin_loop;

/*
* 根据指定值进行自旋，返回下次自旋的值
*/
#[inline]
pub(crate) fn spin(mut len: u32) -> u32 {
    if len < 1 {
        len = 1;
    } else if len > 10 {
        len = 10;
    }

    for _ in 0..(1 << len) {
        spin_loop()
    }

    len + 1
}
