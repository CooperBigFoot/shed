//! Optional object-store counters for remote source benchmarking.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};

const BENCH_NET_ENV: &str = "PYSHED_BENCH_NET";

/// Cloneable snapshot of counters for one object path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathCounters {
    /// Number of object-store requests attributed to this path.
    pub requests: u64,
    /// Number of response bytes read for this path.
    pub bytes_in: u64,
    /// Number of request body bytes written for this path.
    pub bytes_out: u64,
}

/// Cloneable snapshot of aggregate object-store counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpStatsSnapshot {
    /// Total object-store requests counted by the wrapper.
    pub total_requests: u64,
    /// Total response bytes read by consumers.
    pub total_bytes_in: u64,
    /// Total request body bytes written by consumers.
    pub total_bytes_out: u64,
    /// Per-path counters keyed by object path string.
    pub per_path: BTreeMap<String, PathCounters>,
}

/// Shared handle used to snapshot object-store counters.
#[derive(Debug, Clone, Default)]
pub struct HttpStatsHandle {
    inner: Arc<HttpStatsInner>,
}

impl HttpStatsHandle {
    fn new() -> Self {
        Self::default()
    }

    /// Return a cloneable snapshot of the current counters.
    pub fn snapshot(&self) -> HttpStatsSnapshot {
        let per_path = self
            .inner
            .per_path
            .iter()
            .map(|entry| {
                let counters = entry.value();
                (
                    entry.key().clone(),
                    PathCounters {
                        requests: counters.requests.load(Ordering::Relaxed),
                        bytes_in: counters.bytes_in.load(Ordering::Relaxed),
                        bytes_out: counters.bytes_out.load(Ordering::Relaxed),
                    },
                )
            })
            .collect();

        HttpStatsSnapshot {
            total_requests: self.inner.total_requests.load(Ordering::Relaxed),
            total_bytes_in: self.inner.total_bytes_in.load(Ordering::Relaxed),
            total_bytes_out: self.inner.total_bytes_out.load(Ordering::Relaxed),
            per_path,
        }
    }

    fn record_request(&self, path: &str) {
        self.inner.total_requests.fetch_add(1, Ordering::Relaxed);
        let counters = self.inner.per_path.entry(path.to_string()).or_default();
        counters.requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_bytes_in(&self, path: &str, bytes: u64) {
        self.inner
            .total_bytes_in
            .fetch_add(bytes, Ordering::Relaxed);
        let counters = self.inner.per_path.entry(path.to_string()).or_default();
        counters.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_bytes_out(&self, path: &str, bytes: u64) {
        self.inner
            .total_bytes_out
            .fetch_add(bytes, Ordering::Relaxed);
        let counters = self.inner.per_path.entry(path.to_string()).or_default();
        counters.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct HttpStatsInner {
    total_requests: AtomicU64,
    total_bytes_in: AtomicU64,
    total_bytes_out: AtomicU64,
    per_path: DashMap<String, AtomicPathCounters>,
}

#[derive(Debug, Default)]
struct AtomicPathCounters {
    requests: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

/// Wrap an object store when `PYSHED_BENCH_NET=1`.
pub fn wrap_if_enabled(
    store: Arc<dyn ObjectStore>,
) -> (Arc<dyn ObjectStore>, Option<HttpStatsHandle>) {
    if std::env::var(BENCH_NET_ENV).as_deref() != Ok("1") {
        return (store, None);
    }

    let stats = HttpStatsHandle::new();
    let counting = CountingStore {
        inner: store,
        stats: stats.clone(),
    };
    (Arc::new(counting), Some(stats))
}

/// Object-store wrapper that counts requests and transferred bytes.
#[derive(Debug, Clone)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    stats: HttpStatsHandle,
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

impl CountingStore {
    fn path_key(path: &Path) -> String {
        path.as_ref().to_string()
    }

    fn prefix_key(prefix: Option<&Path>) -> String {
        prefix
            .map(Self::path_key)
            .unwrap_or_else(|| "<root>".to_string())
    }

    fn wrap_get_result(&self, path: &str, result: GetResult) -> GetResult {
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let stats = self.stats.clone();
        let path = path.to_string();
        let stream = result
            .into_stream()
            .inspect_ok(move |chunk| {
                stats.record_bytes_in(&path, chunk.len() as u64);
            })
            .boxed();

        GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes,
        }
    }
}

impl ObjectStore for CountingStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Pin<Box<dyn Future<Output = Result<PutResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let path = Self::path_key(location);
            self.stats.record_request(&path);
            self.stats
                .record_bytes_out(&path, payload.content_length() as u64);
            self.inner.put_opts(location, payload, opts).await
        })
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        opts: PutMultipartOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn MultipartUpload>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let path = Self::path_key(location);
            self.stats.record_request(&path);
            self.inner.put_multipart_opts(location, opts).await
        })
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        options: GetOptions,
    ) -> Pin<Box<dyn Future<Output = Result<GetResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let path = Self::path_key(location);
            self.stats.record_request(&path);
            let result = self.inner.get_opts(location, options).await?;
            Ok(self.wrap_get_result(&path, result))
        })
    }

    fn get_ranges<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        ranges: &'life2 [Range<u64>],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Bytes>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let path = Self::path_key(location);
            self.stats.record_request(&path);
            let bytes = self.inner.get_ranges(location, ranges).await?;
            let len = bytes.iter().map(|chunk| chunk.len() as u64).sum();
            self.stats.record_bytes_in(&path, len);
            Ok(bytes)
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let stats = self.stats.clone();
        let locations = locations
            .inspect_ok(move |path| {
                stats.record_request(&Self::path_key(path));
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.stats.record_request(&Self::prefix_key(prefix));
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.stats.record_request(&Self::prefix_key(prefix));
        self.inner.list_with_offset(prefix, offset)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 Path>,
    ) -> Pin<Box<dyn Future<Output = Result<ListResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.stats.record_request(&Self::prefix_key(prefix));
            self.inner.list_with_delimiter(prefix).await
        })
    }

    fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
        options: CopyOptions,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.stats.record_request(&Self::path_key(from));
            self.inner.copy_opts(from, to, options).await
        })
    }

    fn rename_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
        options: RenameOptions,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.stats.record_request(&Self::path_key(from));
            self.inner.rename_opts(from, to, options).await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex, MutexGuard};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::wrap_if_enabled;
    use crate::runtime::RT;

    static BENCH_NET_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct BenchNetEnv {
        _guard: MutexGuard<'static, ()>,
        previous: Option<OsString>,
    }

    impl BenchNetEnv {
        fn set(value: Option<&str>) -> Self {
            let guard = BENCH_NET_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let previous = std::env::var_os("PYSHED_BENCH_NET");
            // SAFETY: these tests serialize PYSHED_BENCH_NET mutations with
            // BENCH_NET_ENV_LOCK and restore the prior value before unlocking.
            unsafe {
                match value {
                    Some(value) => std::env::set_var("PYSHED_BENCH_NET", value),
                    None => std::env::remove_var("PYSHED_BENCH_NET"),
                }
            }
            Self {
                _guard: guard,
                previous,
            }
        }
    }

    impl Drop for BenchNetEnv {
        fn drop(&mut self) {
            // SAFETY: BENCH_NET_ENV_LOCK is still held while the prior
            // environment value is restored.
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var("PYSHED_BENCH_NET", value),
                    None => std::env::remove_var("PYSHED_BENCH_NET"),
                }
            }
        }
    }

    #[test]
    fn disabled_env_does_not_create_stats_handle() {
        let _env = BenchNetEnv::set(None);
        let store = Arc::new(InMemory::new());

        let (_store, stats) = wrap_if_enabled(store);

        assert_eq!(stats.map(|stats| stats.snapshot()), None);
    }

    #[test]
    fn enabled_env_counts_put_get_and_head() {
        let _env = BenchNetEnv::set(Some("1"));
        let store = Arc::new(InMemory::new());
        let (store, stats) = wrap_if_enabled(store);
        let stats = stats.expect("stats should be enabled");
        let path = Path::from("bench/object.txt");

        RT.block_on(async {
            store
                .put(&path, PutPayload::from("hello"))
                .await
                .expect("put should succeed");
            let bytes = store
                .get(&path)
                .await
                .expect("get should succeed")
                .bytes()
                .await
                .expect("body should read");
            assert_eq!(bytes.as_ref(), b"hello");
            store.head(&path).await.expect("head should succeed");
        });

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.total_requests, 3);
        assert_eq!(snapshot.total_bytes_in, 5);
        assert_eq!(snapshot.total_bytes_out, 5);
        assert_eq!(
            snapshot.per_path.get("bench/object.txt"),
            Some(&super::PathCounters {
                requests: 3,
                bytes_in: 5,
                bytes_out: 5,
            })
        );
    }
}
