use futures::Future;
use pi_async::{prelude::SingleTaskRunner, rt::AsyncRuntimeExt};
use std::{thread::sleep, time::Duration};

/// Runs the provided future, blocking the current thread until the
/// future completes.
///
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    <F as Future>::Output: Default + Send + 'static,
{
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    rt.block_on(future).unwrap()
}

#[test]
fn async_block() {
    assert_eq!(4, block_on(async { 4 }));
}

async fn five() -> u8 {
    5
}

#[test]
fn async_fn() {
    assert_eq!(5, block_on(five()));
}

#[test]
fn test_sleep() {
    let deadline = Duration::from_millis(100);

    block_on(async move {
        sleep(deadline);
    });
}
