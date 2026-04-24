//! Object-store integration coverage for remote HFX sessions.

use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{BinaryBuilder, Float32Builder, Int64Array, Int64Builder, ListBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use hfx_core::BoundingBox;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use shed_core::Engine;
use shed_core::algo::GeoCoord;
use shed_core::engine::{DelineationOptions, RefinementOutcome};
use shed_core::session::DatasetSession;
use tempfile::TempDir;
use url::Url;

static CACHE_ENV_LOCK: Mutex<()> = Mutex::new(());

struct CacheEnv {
    _guard: MutexGuard<'static, ()>,
    previous: Option<std::ffi::OsString>,
}

impl CacheEnv {
    fn set(path: &Path) -> Self {
        let guard = CACHE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var_os("HFX_CACHE_DIR");
        // SAFETY: tests in this binary serialize HFX_CACHE_DIR changes with
        // CACHE_ENV_LOCK and restore the previous value before unlocking.
        unsafe {
            std::env::set_var("HFX_CACHE_DIR", path);
        }
        Self {
            _guard: guard,
            previous,
        }
    }
}

impl Drop for CacheEnv {
    fn drop(&mut self) {
        // SAFETY: CACHE_ENV_LOCK is still held while the prior environment
        // value is restored.
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var("HFX_CACHE_DIR", value),
                None => std::env::remove_var("HFX_CACHE_DIR"),
            }
        }
    }
}

#[test]
fn phase_3a7_open_remote_inmemory_reads_manifest_graph_and_catchments() {
    let cache_dir = TempDir::new().unwrap();
    let _cache_env = CacheEnv::set(cache_dir.path());
    let root = ObjectPath::from("phase-3a7/full-open");
    let url = Url::parse("s3://shed-test/phase-3a7/full-open").unwrap();
    let store = Arc::new(InMemory::new());
    put_remote_fixture(&store, &root, RemoteFixture::Full);

    let session = DatasetSession::open_remote_with_store(store, &root, &url)
        .expect("remote session should open from InMemory object store");

    assert_eq!(session.manifest().atom_count().get(), 3);
    assert_eq!(session.graph().len(), 3);
    assert_eq!(session.catchments().total_rows(), 3);

    let bbox = BoundingBox::new(1.55, 0.05, 1.85, 0.35).unwrap();
    let rows = session
        .catchments()
        .query_by_bbox(&bbox)
        .expect("remote catchments should be queryable");
    assert_eq!(rows.len(), 1);

    assert!(
        cache_dir
            .path()
            .join("testfabric")
            .join("test-v1")
            .join("manifest.json")
            .is_file()
    );
    assert!(
        cache_dir
            .path()
            .join("testfabric")
            .join("test-v1")
            .join("graph.arrow")
            .is_file()
    );
}

#[test]
fn phase_3a7_delineate_remote_inmemory_end_to_end_and_reuses_manifest_graph_cache() {
    let cache_dir = TempDir::new().unwrap();
    let _cache_env = CacheEnv::set(cache_dir.path());
    let root = ObjectPath::from("phase-3a7/delineate-cache");
    let url = Url::parse("s3://shed-test/phase-3a7/delineate-cache").unwrap();
    let first_store = Arc::new(InMemory::new());
    put_remote_fixture(&first_store, &root, RemoteFixture::Full);

    let first_session = DatasetSession::open_remote_with_store(first_store, &root, &url)
        .expect("first remote session should fetch and cache manifest and graph");
    assert_remote_delineation_succeeds(first_session);

    let catchments_only_store = Arc::new(InMemory::new());
    put_remote_fixture(&catchments_only_store, &root, RemoteFixture::CatchmentsOnly);

    let cached_session = DatasetSession::open_remote_with_store(catchments_only_store, &root, &url)
        .expect("cached manifest and graph should combine with remote catchments");
    assert_remote_delineation_succeeds(cached_session);
}

fn assert_remote_delineation_succeeds(session: DatasetSession) {
    let engine = Engine::builder(session).build();
    let result = engine
        .delineate(GeoCoord::new(1.70, 0.20), &DelineationOptions::default())
        .expect("remote Engine::delineate should succeed end-to-end");

    assert_eq!(result.terminal_atom_id().get(), 3);
    assert_eq!(result.upstream_atom_ids().len(), 3);
    assert!(result.area_km2().as_f64() > 0.0);
    assert!(
        !result.geometry().0.is_empty(),
        "watershed geometry should be assembled"
    );
    assert_eq!(
        result.refinement(),
        &RefinementOutcome::NoRastersAvailable,
        "synthetic remote fixture intentionally has no rasters"
    );
}

enum RemoteFixture {
    Full,
    CatchmentsOnly,
}

fn put_remote_fixture(store: &Arc<InMemory>, root: &ObjectPath, fixture: RemoteFixture) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        if matches!(fixture, RemoteFixture::Full) {
            put_object(
                store,
                root,
                "manifest.json",
                PutPayload::from(manifest_bytes()),
            )
            .await;
            put_object(store, root, "graph.arrow", PutPayload::from(graph_bytes())).await;
        }
        put_object(
            store,
            root,
            "catchments.parquet",
            PutPayload::from(catchments_bytes()),
        )
        .await;
    });
}

async fn put_object(store: &Arc<InMemory>, root: &ObjectPath, name: &str, payload: PutPayload) {
    store
        .put(&root.clone().join(name), payload)
        .await
        .unwrap_or_else(|err| panic!("failed to put remote fixture artifact {name}: {err}"));
}

fn manifest_bytes() -> String {
    serde_json::json!({
        "format_version": "0.1",
        "fabric_name": "testfabric",
        "crs": "EPSG:4326",
        "topology": "tree",
        "terminal_sink_id": 0,
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "atom_count": 3,
        "created_at": "2026-01-01T00:00:00Z",
        "adapter_version": "test-v1"
    })
    .to_string()
}

fn graph_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "upstream_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        ),
    ]));

    let id_arr = Int64Array::from(vec![1_i64, 2, 3]);
    let mut list_builder = ListBuilder::new(Int64Builder::new());
    list_builder.append(true);
    list_builder.values().append_value(1);
    list_builder.append(true);
    list_builder.values().append_value(2);
    list_builder.append(true);
    let upstream_arr = list_builder.finish();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_arr), Arc::new(upstream_arr)],
    )
    .unwrap();

    let cursor = Cursor::new(Vec::new());
    let mut writer = FileWriter::try_new(cursor, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    writer.into_inner().unwrap().into_inner()
}

fn catchments_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("area_km2", DataType::Float32, false),
        Field::new("up_area_km2", DataType::Float32, true),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
        Field::new("geometry", DataType::Binary, false),
    ]));

    let mut id_b = Int64Builder::new();
    let mut area_b = Float32Builder::new();
    let mut up_area_b = Float32Builder::new();
    let mut minx_b = Float32Builder::new();
    let mut miny_b = Float32Builder::new();
    let mut maxx_b = Float32Builder::new();
    let mut maxy_b = Float32Builder::new();
    let mut geom_b = BinaryBuilder::new();

    for atom_id in 1..=3_i64 {
        let minx = atom_id as f32 * 0.5;
        let maxx = minx + 0.4;

        id_b.append_value(atom_id);
        area_b.append_value(10.0);
        up_area_b.append_null();
        minx_b.append_value(minx);
        miny_b.append_value(0.0);
        maxx_b.append_value(maxx);
        maxy_b.append_value(0.4);
        geom_b.append_value(&minimal_wkb_polygon(minx as f64, 0.0, maxx as f64, 0.4));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_b.finish()),
            Arc::new(area_b.finish()),
            Arc::new(up_area_b.finish()),
            Arc::new(minx_b.finish()),
            Arc::new(miny_b.finish()),
            Arc::new(maxx_b.finish()),
            Arc::new(maxy_b.finish()),
            Arc::new(geom_b.finish()),
        ],
    )
    .unwrap();
    let cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
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
