//! Integration tests for [`DatasetSession`].
//!
//! Exercises session opening, error paths, graph traversal, and spatial
//! queries against synthetic HFX datasets built inline.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Int64Array, Int64Builder, ListBuilder,
    RecordBatch,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tempfile::TempDir;

use hfx_core::{AtomId, BoundingBox, Topology};
use shed_core::SessionError;
use shed_core::session::DatasetSession;

// ---------------------------------------------------------------------------
// WKB helpers
// ---------------------------------------------------------------------------

fn minimal_wkb_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<u8> {
    let mut w = Vec::new();
    w.push(1u8); // little-endian
    w.extend_from_slice(&3u32.to_le_bytes()); // polygon type
    w.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
    w.extend_from_slice(&5u32.to_le_bytes()); // 5 points (closed)
    for (x, y) in [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ] {
        w.extend_from_slice(&x.to_le_bytes());
        w.extend_from_slice(&y.to_le_bytes());
    }
    w
}

fn minimal_wkb_linestring(x1: f64, y1: f64, x2: f64, y2: f64) -> Vec<u8> {
    let mut w = Vec::new();
    w.push(1u8); // little-endian
    w.extend_from_slice(&2u32.to_le_bytes()); // linestring type
    w.extend_from_slice(&2u32.to_le_bytes()); // 2 points
    for (x, y) in [(x1, y1), (x2, y2)] {
        w.extend_from_slice(&x.to_le_bytes());
        w.extend_from_slice(&y.to_le_bytes());
    }
    w
}

// ---------------------------------------------------------------------------
// Artifact writer helpers
// ---------------------------------------------------------------------------

fn write_manifest(root: &Path, atom_count: usize, snap: bool, rasters: bool, topology: &str) {
    let mut m = serde_json::json!({
        "format_version": "0.1",
        "fabric_name": "testfabric",
        "crs": "EPSG:4326",
        "topology": topology,
        "terminal_sink_id": 0,
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "atom_count": atom_count,
        "created_at": "2026-01-01T00:00:00Z",
        "adapter_version": "test-v1"
    });
    if snap {
        m["has_snap"] = serde_json::json!(true);
    }
    if rasters {
        m["has_rasters"] = serde_json::json!(true);
        m["flow_dir_encoding"] = serde_json::json!("esri");
    }
    std::fs::write(root.join("manifest.json"), m.to_string()).unwrap();
}

/// Write a linear-chain graph: atom 1 is headwater, atom i has upstream=[i-1].
fn write_graph(root: &Path, atom_count: usize) {
    let ids: Vec<i64> = (1..=(atom_count as i64)).collect();
    let upstream: Vec<Vec<i64>> = (1..=(atom_count as i64))
        .map(|i| if i == 1 { vec![] } else { vec![i - 1] })
        .collect();
    write_graph_raw(root, &ids, &upstream);
}

/// Write a graph with explicit atom IDs and upstream-ID lists.
///
/// Unlike [`write_graph`], which always generates a linear chain `1..=N`,
/// this helper lets callers specify arbitrary IDs so mismatches between graph
/// and catchments can be constructed.
fn write_graph_custom(root: &Path, ids: &[i64], upstream_ids: &[Vec<i64>]) {
    write_graph_raw(root, ids, upstream_ids);
}

/// Write a DAG graph with the given id and upstream vectors.
fn write_graph_raw(root: &Path, ids: &[i64], upstream_ids: &[Vec<i64>]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "upstream_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        ),
    ]));

    let id_arr = Int64Array::from(ids.to_vec());
    let mut list_builder = ListBuilder::new(Int64Builder::new());
    for ups in upstream_ids {
        for &u in ups {
            list_builder.values().append_value(u);
        }
        list_builder.append(true);
    }
    let upstream_arr = list_builder.finish();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_arr), Arc::new(upstream_arr)],
    )
    .unwrap();

    let file = std::fs::File::create(root.join("graph.arrow")).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

fn write_catchments(root: &Path, atom_count: usize, row_group_size: usize) {
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

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let file = std::fs::File::create(root.join("catchments.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut id_b = Int64Builder::new();
    let mut area_b = Float32Builder::new();
    let mut up_area_b = Float32Builder::new();
    let mut minx_b = Float32Builder::new();
    let mut miny_b = Float32Builder::new();
    let mut maxx_b = Float32Builder::new();
    let mut maxy_b = Float32Builder::new();
    let mut geom_b = BinaryBuilder::new();

    for i in 1..=(atom_count as i64) {
        let idx = i as f32;
        let minx = idx * 0.5;
        let miny = 0.0f32;
        let maxx = idx * 0.5 + 0.4;
        let maxy = 0.4f32;

        id_b.append_value(i);
        area_b.append_value(10.0f32);
        up_area_b.append_null();
        minx_b.append_value(minx);
        miny_b.append_value(miny);
        maxx_b.append_value(maxx);
        maxy_b.append_value(maxy);
        geom_b.append_value(minimal_wkb_polygon(
            minx as f64,
            miny as f64,
            maxx as f64,
            maxy as f64,
        ));
    }

    let batch = RecordBatch::try_new(
        schema,
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

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_snap(root: &Path, atom_count: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("catchment_id", DataType::Int64, false),
        Field::new("weight", DataType::Float32, false),
        Field::new("is_mainstem", DataType::Boolean, false),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
        Field::new("geometry", DataType::Binary, false),
    ]));

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(8192))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let file = std::fs::File::create(root.join("snap.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut id_b = Int64Builder::new();
    let mut catchment_id_b = Int64Builder::new();
    let mut weight_b = Float32Builder::new();
    let mut is_mainstem_b = BooleanBuilder::new();
    let mut minx_b = Float32Builder::new();
    let mut miny_b = Float32Builder::new();
    let mut maxx_b = Float32Builder::new();
    let mut maxy_b = Float32Builder::new();
    let mut geom_b = BinaryBuilder::new();

    for i in 1..=(atom_count as i64) {
        let idx = i as f32;
        let minx = idx * 0.5;
        let miny = 0.0f32;
        let maxx = idx * 0.5 + 0.4;
        let maxy = 0.4f32;
        let cx = ((minx + maxx) / 2.0) as f64;
        let cy = ((miny + maxy) / 2.0) as f64;

        id_b.append_value(i);
        catchment_id_b.append_value(i);
        weight_b.append_value(100.0f32);
        is_mainstem_b.append_value(true);
        minx_b.append_value(minx);
        miny_b.append_value(miny);
        maxx_b.append_value(maxx);
        maxy_b.append_value(maxy);
        geom_b.append_value(minimal_wkb_linestring(cx - 0.1, cy, cx + 0.1, cy));
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(catchment_id_b.finish()),
            Arc::new(weight_b.finish()),
            Arc::new(is_mainstem_b.finish()),
            Arc::new(minx_b.finish()),
            Arc::new(miny_b.finish()),
            Arc::new(maxx_b.finish()),
            Arc::new(maxy_b.finish()),
            Arc::new(geom_b.finish()),
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Write snap targets where every bbox is degenerate (minx == maxx, miny == maxy).
///
/// Each snap target is a point at the centre of the corresponding catchment
/// bbox. The HFX spec permits degenerate snap bboxes; the session must open
/// without error and return results when queried with a covering bbox.
fn write_snap_with_degenerate_bbox(root: &Path, atom_count: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("catchment_id", DataType::Int64, false),
        Field::new("weight", DataType::Float32, false),
        Field::new("is_mainstem", DataType::Boolean, false),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
        Field::new("geometry", DataType::Binary, false),
    ]));

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(8192))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let file = std::fs::File::create(root.join("snap.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut id_b = Int64Builder::new();
    let mut catchment_id_b = Int64Builder::new();
    let mut weight_b = Float32Builder::new();
    let mut is_mainstem_b = BooleanBuilder::new();
    let mut minx_b = Float32Builder::new();
    let mut miny_b = Float32Builder::new();
    let mut maxx_b = Float32Builder::new();
    let mut maxy_b = Float32Builder::new();
    let mut geom_b = BinaryBuilder::new();

    for i in 1..=(atom_count as i64) {
        let idx = i as f32;
        // Degenerate point bbox: minx == maxx, miny == maxy at catchment centre.
        let px = idx * 0.5 + 0.2;
        let py = 0.2f32;

        id_b.append_value(i);
        catchment_id_b.append_value(i);
        weight_b.append_value(100.0f32);
        is_mainstem_b.append_value(true);
        minx_b.append_value(px);
        miny_b.append_value(py);
        maxx_b.append_value(px); // intentionally equal to minx
        maxy_b.append_value(py); // intentionally equal to miny
        geom_b.append_value(minimal_wkb_linestring(
            px as f64 - 0.01,
            py as f64,
            px as f64 + 0.01,
            py as f64,
        ));
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(catchment_id_b.finish()),
            Arc::new(weight_b.finish()),
            Arc::new(is_mainstem_b.finish()),
            Arc::new(minx_b.finish()),
            Arc::new(miny_b.finish()),
            Arc::new(maxx_b.finish()),
            Arc::new(maxy_b.finish()),
            Arc::new(geom_b.finish()),
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Write snap targets with explicit catchment IDs.
///
/// The row geometry and bbox placement follow the same layout as [`write_snap`],
/// but `catchment_id` values are supplied by the caller so integrity failures
/// can be constructed.
fn write_snap_with_custom_catchment_ids(root: &Path, catchment_ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("catchment_id", DataType::Int64, false),
        Field::new("weight", DataType::Float32, false),
        Field::new("is_mainstem", DataType::Boolean, false),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
        Field::new("geometry", DataType::Binary, false),
    ]));

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(8192))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let file = std::fs::File::create(root.join("snap.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut id_b = Int64Builder::new();
    let mut catchment_id_b = Int64Builder::new();
    let mut weight_b = Float32Builder::new();
    let mut is_mainstem_b = BooleanBuilder::new();
    let mut minx_b = Float32Builder::new();
    let mut miny_b = Float32Builder::new();
    let mut maxx_b = Float32Builder::new();
    let mut maxy_b = Float32Builder::new();
    let mut geom_b = BinaryBuilder::new();

    for (idx, &catchment_id) in catchment_ids.iter().enumerate() {
        let row_id = (idx + 1) as i64;
        let bbox_idx = row_id as f32;
        let minx = bbox_idx * 0.5;
        let miny = 0.0f32;
        let maxx = bbox_idx * 0.5 + 0.4;
        let maxy = 0.4f32;
        let cx = ((minx + maxx) / 2.0) as f64;
        let cy = ((miny + maxy) / 2.0) as f64;

        id_b.append_value(row_id);
        catchment_id_b.append_value(catchment_id);
        weight_b.append_value(100.0f32);
        is_mainstem_b.append_value(true);
        minx_b.append_value(minx);
        miny_b.append_value(miny);
        maxx_b.append_value(maxx);
        maxy_b.append_value(maxy);
        geom_b.append_value(minimal_wkb_linestring(cx - 0.1, cy, cx + 0.1, cy));
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(catchment_id_b.finish()),
            Arc::new(weight_b.finish()),
            Arc::new(is_mainstem_b.finish()),
            Arc::new(minx_b.finish()),
            Arc::new(miny_b.finish()),
            Arc::new(maxx_b.finish()),
            Arc::new(maxy_b.finish()),
            Arc::new(geom_b.finish()),
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Build a complete synthetic HFX dataset directory. Returns (TempDir, root path).
/// The TempDir must stay alive for the duration of the test.
fn build_dataset(
    atom_count: usize,
    snap: bool,
    rasters: bool,
    topology: &str,
) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    write_manifest(&root, atom_count, snap, rasters, topology);
    write_graph(&root, atom_count);
    write_catchments(&root, atom_count, 8192);
    if snap {
        write_snap(&root, atom_count);
    }
    if rasters {
        std::fs::write(root.join("flow_dir.tif"), b"stub").unwrap();
        std::fs::write(root.join("flow_acc.tif"), b"stub").unwrap();
    }
    (dir, root)
}

/// Write a DAG graph for 4 atoms where atoms 3 and 4 both have atom 2 upstream
/// (bifurcation), and atom 2 has atom 1 upstream.
///
/// Topology:
///   atom 1: headwater (upstream=[])
///   atom 2: upstream=[1]
///   atom 3: upstream=[2]
///   atom 4: upstream=[2]  ← bifurcation: both 3 and 4 share upstream 2
fn write_dag_graph(root: &Path) {
    let ids: Vec<i64> = vec![1, 2, 3, 4];
    let upstream: Vec<Vec<i64>> = vec![
        vec![],  // atom 1: headwater
        vec![1], // atom 2: upstream of 1
        vec![2], // atom 3: upstream of 2
        vec![2], // atom 4: upstream of 2 (bifurcation)
    ];
    write_graph_raw(root, &ids, &upstream);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_open_valid_minimal_dataset() {
    let (_dir, root) = build_dataset(3, false, false, "tree");
    let session = DatasetSession::open_path(&root).expect("minimal dataset should open");

    assert_eq!(session.manifest().atom_count().get(), 3);
    assert_eq!(session.manifest().fabric_name(), "testfabric");
    assert_eq!(session.topology(), Topology::Tree);
    assert_eq!(session.graph().len(), 3);
    assert_eq!(session.catchments().total_rows(), 3);
    assert!(session.snap().is_none());
    assert!(session.raster_paths().is_none());
    assert_eq!(session.root(), root.as_path());
}

#[test]
fn test_open_valid_full_dataset() {
    let (_dir, root) = build_dataset(5, true, true, "tree");
    let session = DatasetSession::open_path(&root).expect("full dataset should open");

    assert_eq!(session.manifest().atom_count().get(), 5);
    assert!(session.snap().is_some());
    assert_eq!(session.snap().unwrap().total_rows(), 5);

    let rp = session.raster_paths().expect("raster_paths should be Some");
    assert_eq!(rp.flow_dir(), root.join("flow_dir.tif").as_path());
    assert_eq!(rp.flow_acc(), root.join("flow_acc.tif").as_path());
    assert_eq!(
        rp.flow_dir_uri(),
        root.join("flow_dir.tif").display().to_string()
    );
    assert_eq!(
        rp.flow_acc_uri(),
        root.join("flow_acc.tif").display().to_string()
    );
}

#[test]
fn test_open_missing_root() {
    let result = DatasetSession::open("/nonexistent/shed/test/path/xyz123");
    assert!(
        matches!(result, Err(SessionError::RootNotFound { .. })),
        "expected RootNotFound, got: {result:?}"
    );
}

#[test]
fn test_open_unsupported_remote_source() {
    let err = DatasetSession::open("gs://shed-test/example/root").unwrap_err();

    assert!(matches!(err, SessionError::UnsupportedDatasetSource { .. }));
}

#[test]
fn test_open_missing_manifest() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    // Write graph and catchments but not manifest.json
    write_graph(root, 2);
    write_catchments(root, 2, 8192);

    let result = DatasetSession::open_path(root);
    assert!(
        matches!(
            result,
            Err(SessionError::RequiredArtifactMissing {
                artifact: "manifest.json",
                ..
            })
        ),
        "expected RequiredArtifactMissing for manifest.json, got: {result:?}"
    );
}

#[test]
fn test_open_missing_graph() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    // Write manifest and catchments but not graph.arrow
    write_manifest(root, 2, false, false, "tree");
    write_catchments(root, 2, 8192);

    let result = DatasetSession::open_path(root);
    assert!(
        matches!(
            result,
            Err(SessionError::RequiredArtifactMissing {
                artifact: "graph.arrow",
                ..
            })
        ),
        "expected RequiredArtifactMissing for graph.arrow, got: {result:?}"
    );
}

#[test]
fn test_open_missing_catchments() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    // Write manifest and graph but not catchments.parquet
    write_manifest(root, 2, false, false, "tree");
    write_graph(root, 2);

    let result = DatasetSession::open_path(root);
    assert!(
        matches!(
            result,
            Err(SessionError::RequiredArtifactMissing {
                artifact: "catchments.parquet",
                ..
            })
        ),
        "expected RequiredArtifactMissing for catchments.parquet, got: {result:?}"
    );
}

#[test]
fn test_open_snap_declared_but_missing() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    // Manifest declares snap, but no snap.parquet is written
    write_manifest(root, 3, true, false, "tree");
    write_graph(root, 3);
    write_catchments(root, 3, 8192);
    // snap.parquet intentionally absent

    let result = DatasetSession::open_path(root);
    assert!(
        matches!(
            result,
            Err(SessionError::OptionalArtifactMissing {
                artifact: "snap.parquet",
                ..
            })
        ),
        "expected OptionalArtifactMissing for snap.parquet, got: {result:?}"
    );
}

#[test]
fn test_open_atom_count_mismatch() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    // Manifest says 100 atoms but we only write 3
    write_manifest(root, 100, false, false, "tree");
    write_graph(root, 3);
    write_catchments(root, 3, 8192);

    let result = DatasetSession::open_path(root);
    assert!(
        matches!(
            result,
            Err(SessionError::AtomCountMismatch {
                manifest_count: 100,
                actual_count: 3
            })
        ),
        "expected AtomCountMismatch(100, 3), got: {result:?}"
    );
}

#[test]
fn test_graph_traversal_from_session() {
    // 5-atom linear chain: 1 <- 2 <- 3 <- 4 <- 5
    // Starting at atom 5, walk upstream to the headwater.
    let (_dir, root) = build_dataset(5, false, false, "tree");
    let session = DatasetSession::open_path(&root).unwrap();
    let graph = session.graph();

    let mut current_id = AtomId::new(5).unwrap();
    let mut visited: Vec<i64> = vec![current_id.get()];

    loop {
        let row = graph.get(current_id).expect("atom should exist in graph");
        if row.is_headwater() {
            break;
        }
        let ups = row.upstream_ids();
        assert_eq!(
            ups.len(),
            1,
            "linear chain: each non-headwater has exactly 1 upstream"
        );
        current_id = ups[0];
        visited.push(current_id.get());
    }

    // Should have walked 5 -> 4 -> 3 -> 2 -> 1
    assert_eq!(visited, vec![5, 4, 3, 2, 1]);

    // Verify atom 1 is a headwater
    let headwater = graph.get(AtomId::new(1).unwrap()).unwrap();
    assert!(headwater.is_headwater());

    // Verify atom 5's direct upstream is atom 4
    let row5 = graph.get(AtomId::new(5).unwrap()).unwrap();
    assert_eq!(row5.upstream_ids(), &[AtomId::new(4).unwrap()]);
}

#[test]
fn test_catchment_bbox_query() {
    // 5 atoms with bboxes at:
    //   atom i: [i*0.5, 0.0, i*0.5+0.4, 0.4]
    //   atom 1: [0.5, 0.0, 0.9, 0.4]
    //   atom 2: [1.0, 0.0, 1.4, 0.4]
    //   atom 3: [1.5, 0.0, 1.9, 0.4]
    //   atom 4: [2.0, 0.0, 2.4, 0.4]
    //   atom 5: [2.5, 0.0, 2.9, 0.4]
    let (_dir, root) = build_dataset(5, false, false, "tree");
    let session = DatasetSession::open_path(&root).unwrap();

    // Query bbox strictly inside atom 3's region, not touching atom 2 (ends at 1.4)
    // or atom 4 (starts at 2.0). BoundingBox uses f32 values.
    let query = BoundingBox::new(1.5, 0.0, 1.9, 0.4).unwrap();
    let results = session.catchments().query_by_bbox(&query).unwrap();

    assert_eq!(
        results.len(),
        1,
        "expected exactly 1 catchment, got: {:?}",
        results.len()
    );
    assert_eq!(results[0].id(), AtomId::new(3).unwrap(), "expected atom 3");
}

#[test]
fn test_snap_bbox_query() {
    // 5 atoms + snap. Snap targets share the same bboxes as catchments:
    //   atom 2 snap bbox: [1.0, 0.0, 1.4, 0.4]
    //   atom 3 snap bbox: [1.5, 0.0, 1.9, 0.4]
    let (_dir, root) = build_dataset(5, true, false, "tree");
    let session = DatasetSession::open_path(&root).unwrap();

    let snap = session.snap().expect("snap should be present");

    // Query bbox strictly inside atom 2's region: [1.0, 0.0, 1.4, 0.4]
    // Does not touch atom 1 (ends at 0.9) or atom 3 (starts at 1.5).
    let query = BoundingBox::new(1.0, 0.0, 1.4, 0.4).unwrap();
    let results = snap.query_by_bbox(&query).unwrap();

    assert_eq!(
        results.len(),
        1,
        "expected exactly 1 snap target, got: {:?}",
        results.len()
    );
    assert_eq!(
        results[0].catchment_id(),
        AtomId::new(2).unwrap(),
        "expected snap target for atom 2"
    );
}

#[test]
fn test_dag_topology() {
    // 4-atom DAG:
    //   atom 1: headwater
    //   atom 2: upstream=[1]
    //   atom 3: upstream=[2]
    //   atom 4: upstream=[2]  ← both 3 and 4 share upstream atom 2
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    write_manifest(&root, 4, false, false, "dag");
    write_dag_graph(&root);
    write_catchments(&root, 4, 8192);

    let session = DatasetSession::open_path(&root).expect("DAG dataset should open");

    assert_eq!(session.topology(), Topology::Dag, "topology should be DAG");

    let graph = session.graph();
    assert_eq!(graph.len(), 4);

    // Atom 3 has upstream=[2]
    let row3 = graph.get(AtomId::new(3).unwrap()).expect("atom 3 missing");
    assert_eq!(row3.upstream_ids(), &[AtomId::new(2).unwrap()]);
    assert!(!row3.is_headwater());

    // Atom 4 also has upstream=[2] (bifurcation)
    let row4 = graph.get(AtomId::new(4).unwrap()).expect("atom 4 missing");
    assert_eq!(row4.upstream_ids(), &[AtomId::new(2).unwrap()]);
    assert!(!row4.is_headwater());

    // Walk from atom 3 to headwater: 3 -> 2 -> 1
    let mut current = AtomId::new(3).unwrap();
    let mut path = vec![current.get()];
    loop {
        let row = graph.get(current).unwrap();
        if row.is_headwater() {
            break;
        }
        current = row.upstream_ids()[0];
        path.push(current.get());
    }
    assert_eq!(path, vec![3, 2, 1]);

    // Walk from atom 4 to headwater: 4 -> 2 -> 1
    let mut current = AtomId::new(4).unwrap();
    let mut path = vec![current.get()];
    loop {
        let row = graph.get(current).unwrap();
        if row.is_headwater() {
            break;
        }
        current = row.upstream_ids()[0];
        path.push(current.get());
    }
    assert_eq!(path, vec![4, 2, 1]);

    // Atom 1 is a headwater
    let row1 = graph.get(AtomId::new(1).unwrap()).unwrap();
    assert!(row1.is_headwater());
}

#[test]
fn test_graph_catchment_id_mismatch() {
    // Graph contains atom 4; catchments only have atoms 1, 2, 3.
    // The atom_count check passes (manifest=3, catchments=3, graph len=3),
    // but the referential integrity check must fire because graph atom 4 has
    // no matching catchment row.
    let dir = TempDir::new().unwrap();
    let root = dir.path();

    write_manifest(root, 3, false, false, "tree");
    // Catchments: atoms 1, 2, 3
    write_catchments(root, 3, 8192);
    // Graph: atoms 1, 2, 4 — atom 4 is absent from catchments, atom 3 is absent from graph
    write_graph_custom(root, &[1, 2, 4], &[vec![], vec![1], vec![2]]);

    let err = DatasetSession::open_path(root).unwrap_err();
    assert!(
        matches!(err, SessionError::IntegrityViolation { .. }),
        "expected IntegrityViolation, got: {err}"
    );
}

#[test]
fn test_graph_upstream_id_missing_from_catchments() {
    // Graph atoms [1, 2, 3] are all present in catchments, but atom 3's
    // upstream list references atom 99 which does not exist in catchments.
    let dir = TempDir::new().unwrap();
    let root = dir.path();

    write_manifest(root, 3, false, false, "tree");
    write_catchments(root, 3, 8192);
    // Atom 3 references upstream atom 99 — not in catchments
    write_graph_custom(root, &[1, 2, 3], &[vec![], vec![1], vec![99]]);

    let err = DatasetSession::open_path(root).unwrap_err();
    assert!(
        matches!(err, SessionError::IntegrityViolation { .. }),
        "expected IntegrityViolation, got: {err}"
    );
    // The error message should name the missing upstream atom
    let msg = err.to_string();
    assert!(msg.contains("99"), "error should mention atom 99: {msg}");
}

#[test]
fn test_degenerate_snap_bbox_opens_and_queries() {
    // Snap targets whose bboxes are degenerate (minx == maxx, miny == maxy).
    // The session must open without error, and a covering bbox query must
    // return results.
    let dir = TempDir::new().unwrap();
    let root = dir.path();

    write_manifest(root, 3, true, false, "tree");
    write_graph(root, 3);
    write_catchments(root, 3, 8192);
    write_snap_with_degenerate_bbox(root, 3);

    let session = DatasetSession::open_path(root)
        .expect("session with degenerate snap bboxes should open successfully");

    let snap = session.snap().expect("snap store should be present");

    // A large bbox that covers all three point locations:
    //   atom 1 point: (0.7, 0.2), atom 2: (1.2, 0.2), atom 3: (1.7, 0.2)
    let bbox = BoundingBox::new(0.0, 0.0, 5.0, 1.0).unwrap();
    let results = snap.query_by_bbox(&bbox).unwrap();
    assert!(
        !results.is_empty(),
        "should find snap targets with degenerate bboxes within the covering query"
    );
}

#[test]
fn test_snap_catchment_id_missing_from_catchments() {
    // Snap row 2 points at catchment 99, which is absent from catchments.parquet.
    // Session open must reject the dataset instead of deferring failure to later
    // outlet-resolution logic.
    let dir = TempDir::new().unwrap();
    let root = dir.path();

    write_manifest(root, 3, true, false, "tree");
    write_graph(root, 3);
    write_catchments(root, 3, 8192);
    write_snap_with_custom_catchment_ids(root, &[1, 99, 3]);

    let err = DatasetSession::open_path(root).unwrap_err();
    assert!(
        matches!(err, SessionError::IntegrityViolation { .. }),
        "expected IntegrityViolation, got: {err}"
    );

    let msg = err.to_string();
    assert!(
        msg.contains("snap") && msg.contains("catchment") && msg.contains("99"),
        "error should mention the missing snap catchment reference: {msg}"
    );
}
