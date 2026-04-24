//! Shared async runtime for object-store backed readers.

use std::sync::LazyLock;

use tokio::runtime::{Builder, Runtime};

pub(crate) static RT: LazyLock<Runtime> = LazyLock::new(|| {
    match Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("failed to build shared Tokio runtime: {error}"),
    }
});
