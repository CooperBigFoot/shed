//! Regression tests for remote catchment Parquet open behavior.

use std::fmt;
use std::future::Future;
use std::io::Cursor;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{BinaryBuilder, Float32Builder, Int64Builder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use futures_util::stream::BoxStream;
use hfx_core::{AtomId, BoundingBox};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::parquet_cache::ParquetFooterCache;
use crate::reader::catchment_store::CatchmentStore;
use crate::runtime::RT;

#[derive(Debug, Default)]
struct StoreCounters {
    head_calls: AtomicUsize,
    get_range_calls: AtomicUsize,
    get_ranges_calls: AtomicUsize,
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<StoreCounters>,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counters: Arc::new(StoreCounters::default()),
        }
    }

    fn head_calls(&self) -> usize {
        self.counters.head_calls.load(Ordering::SeqCst)
    }

    fn range_read_calls(&self) -> usize {
        self.counters.get_range_calls.load(Ordering::SeqCst)
            + self.counters.get_ranges_calls.load(Ordering::SeqCst)
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

impl ObjectStore for CountingStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Pin<Box<dyn Future<Output = Result<PutResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.put_opts(location, payload, opts).await })
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        opts: PutMultipartOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn MultipartUpload>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.put_multipart_opts(location, opts).await })
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        options: GetOptions,
    ) -> Pin<Box<dyn Future<Output = Result<GetResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        if options.head {
            self.counters.head_calls.fetch_add(1, Ordering::SeqCst);
        }
        if options.range.is_some() {
            self.counters.get_range_calls.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move { self.inner.get_opts(location, options).await })
    }

    fn get_ranges<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        ranges: &'life2 [Range<u64>],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Bytes>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.counters
            .get_ranges_calls
            .fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { self.inner.get_ranges(location, ranges).await })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<ObjectPath>>,
    ) -> BoxStream<'static, Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 ObjectPath>,
    ) -> Pin<Box<dyn Future<Output = Result<ListResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.list_with_delimiter(prefix).await })
    }

    fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjectPath,
        to: &'life2 ObjectPath,
        options: CopyOptions,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.copy_opts(from, to, options).await })
    }
}

#[test]
fn open_remote_indexes_ids_with_one_streaming_scan() {
    let path = ObjectPath::from("catchments.parquet");
    let base_store = Arc::new(InMemory::new());
    let payload = write_catchments_fixture();
    RT.block_on(async {
        base_store
            .put(&path, PutPayload::from(payload))
            .await
            .expect("fixture parquet should be written");
    });

    let counting_store = Arc::new(CountingStore::new(base_store));
    let object_store = Arc::clone(&counting_store) as Arc<dyn ObjectStore>;
    let store =
        CatchmentStore::open_remote(object_store, path, "memory://catchments.parquet".into())
            .expect("remote catchments should open");

    let num_row_groups = 4;
    assert_eq!(counting_store.head_calls(), 1);
    assert!(
        counting_store.range_read_calls() < 2 * num_row_groups,
        "open should not rebuild Parquet metadata once per row group; saw {} range reads",
        counting_store.range_read_calls()
    );

    let query_ids: Vec<AtomId> = (1..=8).map(|id| AtomId::new(id).unwrap()).collect();
    let mut result_ids: Vec<_> = store
        .query_geometries_by_ids(&query_ids)
        .expect("geometry query should still work")
        .into_iter()
        .map(|row| row.into_parts().0)
        .collect();
    result_ids.sort_by_key(|id| id.get());

    assert_eq!(result_ids, query_ids);
}

#[test]
fn footer_cache_reused_for_second_open_and_query() {
    let path = ObjectPath::from("catchments.parquet");
    let base_store = Arc::new(InMemory::new());
    let payload = write_catchments_fixture();
    RT.block_on(async {
        base_store
            .put(&path, PutPayload::from(payload))
            .await
            .expect("fixture parquet should be written");
    });

    let counting_store = Arc::new(CountingStore::new(base_store));
    let object_store = Arc::clone(&counting_store) as Arc<dyn ObjectStore>;
    let footer_cache = ParquetFooterCache::new();

    let first_store = CatchmentStore::open_remote_with_caches(
        object_store.clone(),
        path.clone(),
        "memory://catchments.parquet".into(),
        "testfabric".to_owned(),
        "test-v1".to_owned(),
        None,
        Some(footer_cache.clone()),
        None,
    )
    .expect("first remote catchment store should open");

    assert_eq!(footer_cache.miss_count(), 1);
    assert_eq!(
        footer_cache.hit_count(),
        1,
        "ID indexing should reuse the footer fetched during open"
    );

    let bbox = BoundingBox::new(0.0, 0.0, 1.0, 1.0).unwrap();
    let first_query_misses = footer_cache.miss_count();
    first_store
        .query_by_bbox(&bbox)
        .expect("first query should succeed");
    assert_eq!(
        footer_cache.miss_count(),
        first_query_misses,
        "query builder should not refetch footer after open"
    );
    assert_eq!(
        footer_cache.hit_count(),
        2,
        "first query should hit the shared footer cache"
    );

    let second_store = CatchmentStore::open_remote_with_caches(
        object_store,
        path,
        "memory://catchments.parquet".into(),
        "testfabric".to_owned(),
        "test-v1".to_owned(),
        None,
        Some(footer_cache.clone()),
        None,
    )
    .expect("second remote catchment store should open");
    assert_eq!(
        footer_cache.miss_count(),
        1,
        "second open should reuse the first footer cache entry"
    );
    assert_eq!(
        footer_cache.hit_count(),
        4,
        "second open and ID indexing should both hit the footer cache"
    );

    second_store
        .query_by_bbox(&bbox)
        .expect("second query should succeed");
    assert_eq!(footer_cache.miss_count(), 1);
    assert_eq!(footer_cache.hit_count(), 5);
}

fn catchments_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("area_km2", DataType::Float32, false),
        Field::new("up_area_km2", DataType::Float32, true),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
        Field::new("geometry", DataType::Binary, false),
    ]))
}

fn write_catchments_fixture() -> Vec<u8> {
    let schema = catchments_schema();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(cursor, Arc::clone(&schema), Some(props)).unwrap();

    let mut ids = Int64Builder::new();
    let mut areas = Float32Builder::new();
    let mut up_areas = Float32Builder::new();
    let mut minxs = Float32Builder::new();
    let mut minys = Float32Builder::new();
    let mut maxxs = Float32Builder::new();
    let mut maxys = Float32Builder::new();
    let mut geoms = BinaryBuilder::new();

    for id in 1..=8 {
        let minx = (id - 1) as f32;
        let maxx = minx + 0.5;
        ids.append_value(id);
        areas.append_value(1.0);
        up_areas.append_null();
        minxs.append_value(minx);
        minys.append_value(0.0);
        maxxs.append_value(maxx);
        maxys.append_value(0.5);
        geoms.append_value(minimal_wkb_polygon(minx as f64, 0.0, maxx as f64, 0.5));
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids.finish()),
            Arc::new(areas.finish()),
            Arc::new(up_areas.finish()),
            Arc::new(minxs.finish()),
            Arc::new(minys.finish()),
            Arc::new(maxxs.finish()),
            Arc::new(maxys.finish()),
            Arc::new(geoms.finish()),
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.into_inner().unwrap().into_inner()
}

fn minimal_wkb_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1u8);
    wkb.extend_from_slice(&3u32.to_le_bytes());
    wkb.extend_from_slice(&1u32.to_le_bytes());
    wkb.extend_from_slice(&5u32.to_le_bytes());
    for (x, y) in [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ] {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}
