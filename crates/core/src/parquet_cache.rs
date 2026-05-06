//! Bounded in-memory column-chunk cache for remote Parquet reads.
//!
//! # Architecture
//!
//! [`ParquetRowGroupCache`] is a weighted LRU cache backed by `moka::sync::Cache`.
//! Each entry is keyed by a [`ChunkKey`] that identifies the remote artifact and
//! the byte range requested. Weights are the number of bytes in the cached
//! [`bytes::Bytes`] value.
//!
//! [`ParquetFooterCache`] is a thread-safe metadata map keyed directly by
//! [`ArtifactIdent`]. It stores `Arc<ParquetMetaData>` values so repeated
//! stream-builder construction can share footer metadata without copying it.
//!
//! [`CachingReader`] wraps a [`ParquetObjectReader`] and implements
//! [`AsyncFileReader`], intercepting `get_bytes` calls to serve row-group cache
//! hits and `get_metadata` calls to serve footer cache hits without a network
//! round-trip.
//!
//! # Known Limitations
//!
//! - **In-memory only.** No persistent on-disk backing, no cross-process sharing.
//!
//! - **Moka weigher is u32.** Chunks exceeding `u32::MAX` bytes (~4 GiB) are
//!   silently uncached (the insert is a no-op from the eviction perspective). This
//!   is not a practical concern for Parquet column chunks.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use moka::sync::Cache;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;

/// Default in-memory budget for remote Parquet row-group caching.
pub const DEFAULT_PARQUET_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Identifies which remote artifact a cached chunk belongs to.
///
/// Captured once at session-open time from the manifest and HEAD response.
/// Baking file size, ETag, and last-modified time into the key means stale R2
/// overwrites are detected at the next `Engine(...)` construction rather than
/// silently returning wrong bytes.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ArtifactIdent {
    /// HFX fabric name (from manifest).
    pub fabric_name: String,
    /// Adapter version string (from manifest).
    pub adapter_version: String,
    /// Artifact filename — `"catchments.parquet"` or `"snap.parquet"`.
    pub artifact: &'static str,
    /// File size in bytes from the HEAD response.
    pub file_size: u64,
    /// ETag from the HEAD response, if the object store returns one.
    pub etag: Option<String>,
    /// Last-modified timestamp from the HEAD response.
    pub last_modified: DateTime<Utc>,
}

/// Cache key for one column-chunk byte range.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ChunkKey {
    /// Identifies the remote artifact this chunk belongs to.
    pub ident: ArtifactIdent,
    /// Start offset of the byte range within the file.
    pub chunk_offset: u64,
    /// Number of bytes in the range.
    pub chunk_length: u64,
}

/// Bounded in-memory weighted-LRU cache of Parquet column-chunk byte ranges.
///
/// Create via [`ParquetRowGroupCache::new`] and share across readers via
/// `Arc`. `moka` handles interior locking.
pub struct ParquetRowGroupCache {
    inner: Cache<ChunkKey, Bytes>,
    capacity_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for ParquetRowGroupCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRowGroupCache")
            .field("capacity_bytes", &self.capacity_bytes)
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

impl ParquetRowGroupCache {
    /// Create a new cache bounded to `max_bytes` total byte weight.
    pub fn new(max_bytes: u64) -> Arc<Self> {
        let inner = Cache::builder()
            .max_capacity(max_bytes)
            // moka weigher must return u32; chunks > u32::MAX are inserted with
            // weight 0 (effectively unpinned/uncached). See module doc.
            .weigher(|_k: &ChunkKey, v: &Bytes| v.len().min(u32::MAX as usize) as u32)
            .eviction_listener(|key, value, cause| {
                tracing::debug!(
                    artifact = key.ident.artifact,
                    offset = key.chunk_offset,
                    length = key.chunk_length,
                    evicted_bytes = value.len(),
                    cause = ?cause,
                    "parquet_cache evict"
                );
            })
            .build();

        Arc::new(Self {
            inner,
            capacity_bytes: max_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    /// Return the cached bytes for `key`, incrementing the hit counter.
    ///
    /// Returns `None` on a miss (miss counter incremented by the caller).
    pub fn get(&self, key: &ChunkKey) -> Option<Bytes> {
        let result = self.inner.get(key);
        if result.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Insert `bytes` under `key`, weighted by `bytes.len()`.
    pub fn insert(&self, key: ChunkKey, bytes: Bytes) {
        self.inner.insert(key, bytes);
    }

    /// Return the configured capacity in bytes.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Return the cumulative hit count.
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Return the cumulative miss count.
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

impl Drop for ParquetRowGroupCache {
    fn drop(&mut self) {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        tracing::info!(
            cache_size_bytes = self.inner.weighted_size(),
            capacity_bytes = self.capacity_bytes,
            hits = hits,
            misses = misses,
            hit_rate = hit_rate,
            "parquet_cache stats"
        );
    }
}

/// Thread-safe in-memory cache of Parquet footer metadata.
///
/// This cache is intentionally separate from [`ParquetRowGroupCache`]: footers
/// are keyed by artifact identity only and always stored as shared
/// `Arc<ParquetMetaData>` values.
pub struct ParquetFooterCache {
    inner: DashMap<ArtifactIdent, Arc<ParquetMetaData>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for ParquetFooterCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetFooterCache")
            .field("entries", &self.inner.len())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

impl ParquetFooterCache {
    /// Create a new footer metadata cache.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: DashMap::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    /// Return cached metadata for `ident`.
    pub fn get(&self, ident: &ArtifactIdent) -> Option<Arc<ParquetMetaData>> {
        let metadata = self.inner.get(ident).map(|entry| Arc::clone(entry.value()));
        if metadata.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        metadata
    }

    /// Insert metadata for `ident`.
    pub fn insert(&self, ident: ArtifactIdent, metadata: Arc<ParquetMetaData>) {
        self.inner.insert(ident, metadata);
    }

    /// Return the number of cached footer entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return true when no footer entries are cached.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the cumulative hit count.
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Return the cumulative miss count.
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

impl Drop for ParquetFooterCache {
    fn drop(&mut self) {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        tracing::info!(
            entries = self.inner.len(),
            hits = hits,
            misses = misses,
            hit_rate = hit_rate,
            "parquet_footer_cache stats"
        );
    }
}

/// [`AsyncFileReader`] that wraps a [`ParquetObjectReader`] and intercepts
/// row-group byte reads and footer metadata reads against shared caches.
///
/// On a cache hit the value is served directly without a network round-trip.
/// On a miss the inner reader fetches the value, which is then inserted into
/// the corresponding cache before being returned.
pub struct CachingReader {
    inner: ParquetObjectReader,
    row_group_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    ident: ArtifactIdent,
}

impl CachingReader {
    /// Create a caching reader wrapping `inner` for artifact `ident`.
    pub fn new(
        inner: ParquetObjectReader,
        cache: Arc<ParquetRowGroupCache>,
        ident: ArtifactIdent,
    ) -> Self {
        Self {
            inner,
            row_group_cache: Some(cache),
            footer_cache: None,
            ident,
        }
    }

    /// Create a caching reader with optional row-group and footer caches.
    pub fn new_with_caches(
        inner: ParquetObjectReader,
        row_group_cache: Option<Arc<ParquetRowGroupCache>>,
        footer_cache: Option<Arc<ParquetFooterCache>>,
        ident: ArtifactIdent,
    ) -> Self {
        Self {
            inner,
            row_group_cache,
            footer_cache,
            ident,
        }
    }
}

impl AsyncFileReader for CachingReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let Some(cache) = self.row_group_cache.clone() else {
            return self.inner.get_bytes(range);
        };

        let key = ChunkKey {
            ident: self.ident.clone(),
            chunk_offset: range.start,
            chunk_length: range.end - range.start,
        };

        if let Some(bytes) = cache.get(&key) {
            tracing::debug!(
                artifact = key.ident.artifact,
                offset = key.chunk_offset,
                length = key.chunk_length,
                "parquet_cache hit"
            );
            return std::future::ready(Ok(bytes)).boxed();
        }

        // Cache miss — record it and fetch from the inner reader.
        cache.misses.fetch_add(1, Ordering::Relaxed);
        let inner_fut = self.inner.get_bytes(range);
        async move {
            let bytes = inner_fut.await?;
            tracing::debug!(
                artifact = key.ident.artifact,
                offset = key.chunk_offset,
                length = key.chunk_length,
                fetched_bytes = bytes.len(),
                "parquet_cache miss"
            );
            cache.insert(key, bytes.clone());
            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let Some(cache) = self.row_group_cache.clone() else {
            return self.inner.get_byte_ranges(ranges);
        };

        let mut ordered = vec![None; ranges.len()];
        let mut missing_indexes = Vec::new();
        let mut missing_keys = Vec::new();
        let mut missing_ranges = Vec::new();

        for (index, range) in ranges.into_iter().enumerate() {
            let key = ChunkKey {
                ident: self.ident.clone(),
                chunk_offset: range.start,
                chunk_length: range.end - range.start,
            };

            if let Some(bytes) = cache.get(&key) {
                tracing::debug!(
                    artifact = key.ident.artifact,
                    offset = key.chunk_offset,
                    length = key.chunk_length,
                    "parquet_cache hit"
                );
                ordered[index] = Some(bytes);
            } else {
                missing_indexes.push(index);
                missing_keys.push(key);
                missing_ranges.push(range);
            }
        }

        let missing_count = missing_ranges.len();
        cache
            .misses
            .fetch_add(missing_count as u64, Ordering::Relaxed);

        if missing_count == 0 {
            return std::future::ready(ordered.into_iter().collect::<Option<Vec<_>>>().ok_or_else(
                || {
                    ParquetError::General(
                        "cache hit assembly omitted a requested byte range".to_owned(),
                    )
                },
            ))
            .boxed();
        }

        let inner_fut = self.inner.get_byte_ranges(missing_ranges);
        async move {
            let fetched = inner_fut.await?;
            if fetched.len() != missing_count {
                return Err(ParquetError::General(format!(
                    "inner reader returned {} byte ranges for {} requested ranges",
                    fetched.len(),
                    missing_count
                )));
            }

            for ((index, key), bytes) in missing_indexes.into_iter().zip(missing_keys).zip(fetched)
            {
                tracing::debug!(
                    artifact = key.ident.artifact,
                    offset = key.chunk_offset,
                    length = key.chunk_length,
                    fetched_bytes = bytes.len(),
                    "parquet_cache miss"
                );
                cache.insert(key, bytes.clone());
                ordered[index] = Some(bytes);
            }

            ordered
                .into_iter()
                .map(|bytes| {
                    bytes.ok_or_else(|| {
                        ParquetError::General(
                            "inner reader returned fewer byte ranges than requested".to_owned(),
                        )
                    })
                })
                .collect()
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let Some(footer_cache) = self.footer_cache.clone() else {
            return self.inner.get_metadata(options);
        };

        if let Some(metadata) = footer_cache.get(&self.ident) {
            tracing::debug!(artifact = self.ident.artifact, "parquet_footer_cache hit");
            return std::future::ready(Ok(metadata)).boxed();
        }

        let ident = self.ident.clone();
        let inner_fut = self.inner.get_metadata(options);
        async move {
            let metadata = inner_fut.await?;
            tracing::debug!(artifact = ident.artifact, "parquet_footer_cache miss");
            footer_cache.insert(ident, Arc::clone(&metadata));
            Ok(metadata)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::{self, Write};
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    use super::*;

    #[derive(Clone)]
    struct RecordingSubscriber {
        events: Arc<Mutex<Vec<String>>>,
    }

    struct EventVisitor {
        line: String,
    }

    impl Visit for EventVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            if !self.line.is_empty() {
                self.line.push(' ');
            }
            let _ = write!(&mut self.line, "{}={value:?}", field.name());
        }
    }

    impl Subscriber for RecordingSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut visitor = EventVisitor {
                line: String::new(),
            };
            event.record(&mut visitor);
            self.events.lock().unwrap().push(visitor.line);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    fn make_ident() -> ArtifactIdent {
        ArtifactIdent {
            fabric_name: "testfabric".to_owned(),
            adapter_version: "v1".to_owned(),
            artifact: "catchments.parquet",
            file_size: 1_000_000,
            etag: Some("abc123".to_owned()),
            last_modified: DateTime::<Utc>::UNIX_EPOCH,
        }
    }

    #[test]
    fn hit_miss_counters() {
        let cache = ParquetRowGroupCache::new(1024 * 1024);
        let ident = make_ident();

        let key1 = ChunkKey {
            ident: ident.clone(),
            chunk_offset: 0,
            chunk_length: 100,
        };
        let key2 = ChunkKey {
            ident: ident.clone(),
            chunk_offset: 200,
            chunk_length: 50,
        };

        // Neither key is in the cache yet.
        assert!(cache.get(&key1).is_none());
        assert_eq!(cache.miss_count(), 0); // get() does not bump misses — caller does
        assert_eq!(cache.hit_count(), 0);

        // Insert key1.
        cache.insert(key1.clone(), Bytes::from_static(b"hello world"));

        // Now key1 is a hit.
        let fetched = cache.get(&key1);
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap(), Bytes::from_static(b"hello world"));
        assert_eq!(cache.hit_count(), 1);

        // key2 still a miss.
        assert!(cache.get(&key2).is_none());
        assert_eq!(cache.hit_count(), 1);
    }

    #[test]
    fn caching_reader_get_byte_ranges_mixes_hits_and_misses_in_order() {
        // ParquetObjectReader does not expose its request/coalescing count, so
        // this regression test cannot directly assert the single bulk miss
        // fetch. It verifies the observable contract of the override: cached
        // ranges are hits, only cold input ranges increment misses, cold bytes
        // are inserted, and output order matches the caller's range order.
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;
        use object_store::{ObjectStore, ObjectStoreExt};
        use parquet::arrow::async_reader::ParquetObjectReader;

        use crate::runtime::RT;

        // Populate an in-memory store with a small known payload.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("test/file.bin");
        let payload: Vec<u8> = (0u8..=63).collect();
        RT.block_on(async {
            store
                .put(&path, PutPayload::from(payload.clone()))
                .await
                .unwrap();
        });

        let inner = ParquetObjectReader::new(store, path).with_file_size(payload.len() as u64);
        let cache = ParquetRowGroupCache::new(1024 * 1024);
        let ident = make_ident();
        cache.insert(
            ChunkKey {
                ident: ident.clone(),
                chunk_offset: 8,
                chunk_length: 8,
            },
            Bytes::copy_from_slice(&payload[8..16]),
        );
        let mut reader = CachingReader::new(inner, cache.clone(), ident.clone());

        let ranges = vec![32u64..40, 8u64..16, 0u64..4];
        let bytes_vec = RT
            .block_on(async { reader.get_byte_ranges(ranges.clone()).await })
            .expect("get_byte_ranges should succeed");
        assert_eq!(bytes_vec.len(), 3);
        assert_eq!(bytes_vec[0].as_ref(), &payload[32..40]);
        assert_eq!(bytes_vec[1].as_ref(), &payload[8..16]);
        assert_eq!(bytes_vec[2].as_ref(), &payload[0..4]);

        assert_eq!(cache.hit_count(), 1, "pre-warmed range should be a hit");
        assert_eq!(cache.miss_count(), 2, "only cold ranges should miss");

        for range in &ranges {
            let key = ChunkKey {
                ident: ident.clone(),
                chunk_offset: range.start,
                chunk_length: range.end - range.start,
            };
            assert!(
                cache.inner.get(&key).is_some(),
                "range {range:?} missing from cache"
            );
        }

        // Second call with the same ranges: now all warm. Hits should bump.
        let bytes_vec_warm = RT
            .block_on(async { reader.get_byte_ranges(ranges.clone()).await })
            .expect("warm get_byte_ranges should succeed");
        assert_eq!(bytes_vec_warm.len(), 3);
        assert_eq!(
            cache.hit_count(),
            4,
            "expected 3 hits on the warm pass; got {}",
            cache.hit_count()
        );
        assert_eq!(
            cache.miss_count(),
            2,
            "miss count must not change on the warm pass"
        );
    }

    #[test]
    fn footer_cache_get_metadata_hits_on_second_reader() {
        use std::io::Cursor;

        use arrow::array::{Int64Builder, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use object_store::PutPayload;
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;
        use object_store::{ObjectStore, ObjectStoreExt};
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::async_reader::ParquetObjectReader;

        use crate::runtime::RT;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut ids = Int64Builder::new();
        ids.append_value(1);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids.finish())]).unwrap();
        let cursor = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
        writer.write(&batch).unwrap();
        let payload = writer.into_inner().unwrap().into_inner();
        let payload_len = payload.len() as u64;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("test/footer.parquet");
        RT.block_on(async {
            store
                .put(&path, PutPayload::from(payload))
                .await
                .expect("fixture parquet should be written");
        });

        let footer_cache = ParquetFooterCache::new();
        let ident = make_ident();
        let inner =
            ParquetObjectReader::new(store.clone(), path.clone()).with_file_size(payload_len);
        let mut first_reader =
            CachingReader::new_with_caches(inner, None, Some(footer_cache.clone()), ident.clone());
        let first_metadata = RT
            .block_on(async { first_reader.get_metadata(None).await })
            .expect("first metadata read should succeed");

        assert_eq!(footer_cache.miss_count(), 1);
        assert_eq!(footer_cache.hit_count(), 0);
        assert_eq!(footer_cache.len(), 1);

        let inner = ParquetObjectReader::new(store, path).with_file_size(payload_len);
        let mut second_reader =
            CachingReader::new_with_caches(inner, None, Some(footer_cache.clone()), ident.clone());
        let second_metadata = RT
            .block_on(async { second_reader.get_metadata(None).await })
            .expect("second metadata read should succeed");

        assert!(Arc::ptr_eq(&first_metadata, &second_metadata));
        assert_eq!(footer_cache.miss_count(), 1);
        assert_eq!(footer_cache.hit_count(), 1);

        let mut etag_bump = ident.clone();
        etag_bump.etag = Some("def456".to_owned());
        assert!(footer_cache.get(&etag_bump).is_none());

        let mut changed_ident = ident;
        changed_ident.adapter_version = "v2".to_owned();
        assert!(footer_cache.get(&changed_ident).is_none());

        assert_eq!(footer_cache.hit_count(), 1);
        assert_eq!(footer_cache.miss_count(), 3);
    }

    #[test]
    fn lru_eviction_under_tight_budget_logs_debug_event() {
        // Budget: 10 bytes. Insert two 8-byte chunks.  Second insert should
        // force eviction because both entries cannot fit together.
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatch = tracing::Dispatch::new(RecordingSubscriber {
            events: events.clone(),
        });
        let ident = make_ident();

        let key_a = ChunkKey {
            ident: ident.clone(),
            chunk_offset: 0,
            chunk_length: 8,
        };
        let key_b = ChunkKey {
            ident: ident.clone(),
            chunk_offset: 100,
            chunk_length: 8,
        };

        tracing::dispatcher::with_default(&dispatch, || {
            let cache = ParquetRowGroupCache::new(10);
            cache.insert(key_a.clone(), Bytes::from(vec![0u8; 8]));
            // Read key_a to exercise the access-order path before pressure.
            let _ = cache.get(&key_a);

            // Insert key_b — combined weight (16) exceeds capacity (10), so
            // moka must evict one entry to stay within budget.
            cache.insert(key_b.clone(), Bytes::from(vec![1u8; 8]));

            // Moka eviction is async/lazy, so we sync before asserting.
            cache.inner.run_pending_tasks();

            assert!(
                cache.inner.weighted_size() <= 10,
                "cache remained over budget after pending tasks"
            );
            let a_present = cache.inner.get(&key_a).is_some();
            let b_present = cache.inner.get(&key_b).is_some();
            assert!(
                !(a_present && b_present),
                "both keys present after tight eviction — expected at most one"
            );
        });

        let events = events.lock().unwrap();
        let evict_events = events
            .iter()
            .filter(|line| line.contains("parquet_cache evict"))
            .collect::<Vec<_>>();
        assert_eq!(
            evict_events.len(),
            1,
            "expected one eviction log event; captured: {events:?}"
        );
        let event = evict_events[0];
        assert!(event.contains("artifact=\"catchments.parquet\""));
        assert!(event.contains("length=8"));
        assert!(event.contains("evicted_bytes=8"));
    }
}
