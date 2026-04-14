//! Integration tests for [`resolve_outlet`].
//!
//! Each test builds a synthetic on-disk HFX dataset, opens a
//! [`DatasetSession`], and calls [`resolve_outlet`] — verifying the correct
//! atom ID and resolution method are returned.

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

use shed_core::algo::coord::GeoCoord;
use shed_core::resolver::{
    OutletResolutionError, ResolutionMethod, ResolverConfig, SearchRadiusMetres,
};
use shed_core::session::DatasetSession;
use shed_core::resolve_outlet;

// ---------------------------------------------------------------------------
// WKB helpers
// ---------------------------------------------------------------------------

fn wkb_point(x: f64, y: f64) -> Vec<u8> {
    let mut w = Vec::new();
    w.push(1u8); // little-endian
    w.extend_from_slice(&1u32.to_le_bytes()); // wkbPoint = 1
    w.extend_from_slice(&x.to_le_bytes());
    w.extend_from_slice(&y.to_le_bytes());
    w
}

fn wkb_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<u8> {
    let mut w = Vec::new();
    w.push(1u8); // little-endian
    w.extend_from_slice(&3u32.to_le_bytes()); // polygon type
    w.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
    w.extend_from_slice(&5u32.to_le_bytes()); // 5 points (closed)
    for (x, y) in [(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)] {
        w.extend_from_slice(&x.to_le_bytes());
        w.extend_from_slice(&y.to_le_bytes());
    }
    w
}

fn wkb_linestring(x1: f64, y1: f64, x2: f64, y2: f64) -> Vec<u8> {
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

fn write_manifest(root: &Path, atom_count: usize, has_snap: bool) {
    let mut m = serde_json::json!({
        "format_version": "0.1",
        "fabric_name": "testfabric",
        "crs": "EPSG:4326",
        "topology": "tree",
        "terminal_sink_id": 0,
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "atom_count": atom_count,
        "created_at": "2026-01-01T00:00:00Z",
        "adapter_version": "test-v1"
    });
    if has_snap {
        m["has_snap"] = serde_json::json!(true);
    }
    std::fs::write(root.join("manifest.json"), m.to_string()).unwrap();
}

/// Write a linear-chain graph for the given atom IDs.
///
/// The first ID is the headwater; each subsequent ID has the previous as its
/// sole upstream atom.
fn write_graph(root: &Path, ids: &[i64]) {
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
    for (idx, _) in ids.iter().enumerate() {
        if idx == 0 {
            // headwater — no upstreams
            list_builder.append(true);
        } else {
            list_builder.values().append_value(ids[idx - 1]);
            list_builder.append(true);
        }
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

/// Catchment specification: (id, area_km2, up_area_km2, minx, miny, maxx, maxy).
type CatchmentSpec = (i64, f32, Option<f32>, f64, f64, f64, f64);

fn write_catchments(root: &Path, specs: &[CatchmentSpec]) {
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
        .set_max_row_group_size(8192)
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

    for &(id, area, up_area, minx, miny, maxx, maxy) in specs {
        id_b.append_value(id);
        area_b.append_value(area);
        match up_area {
            Some(v) => up_area_b.append_value(v),
            None => up_area_b.append_null(),
        }
        minx_b.append_value(minx as f32);
        miny_b.append_value(miny as f32);
        maxx_b.append_value(maxx as f32);
        maxy_b.append_value(maxy as f32);
        geom_b.append_value(&wkb_polygon(minx, miny, maxx, maxy));
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

/// Snap target geometry variant.
enum SnapGeom {
    /// A WKB Point at (lon, lat).
    Point(f64, f64),
    /// A WKB LineString from (x1, y1) to (x2, y2).
    Line(f64, f64, f64, f64),
}

/// Snap target specification: (snap_id, catchment_id, weight, is_mainstem, geometry).
type SnapSpec<'a> = (i64, i64, f32, bool, &'a SnapGeom);

fn write_snap(root: &Path, specs: &[SnapSpec<'_>]) {
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
        .set_max_row_group_size(8192)
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

    for &(snap_id, catchment_id, weight, is_mainstem, geom) in specs {
        id_b.append_value(snap_id);
        catchment_id_b.append_value(catchment_id);
        weight_b.append_value(weight);
        is_mainstem_b.append_value(is_mainstem);

        match geom {
            SnapGeom::Point(x, y) => {
                let eps: f32 = 1e-6;
                minx_b.append_value(*x as f32 - eps);
                miny_b.append_value(*y as f32 - eps);
                maxx_b.append_value(*x as f32 + eps);
                maxy_b.append_value(*y as f32 + eps);
                geom_b.append_value(&wkb_point(*x, *y));
            }
            SnapGeom::Line(x1, y1, x2, y2) => {
                minx_b.append_value(x1.min(*x2) as f32);
                miny_b.append_value(y1.min(*y2) as f32);
                maxx_b.append_value(x1.max(*x2) as f32);
                maxy_b.append_value(y1.max(*y2) as f32);
                geom_b.append_value(&wkb_linestring(*x1, *y1, *x2, *y2));
            }
        }
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

// ---------------------------------------------------------------------------
// Convenience dataset builder
// ---------------------------------------------------------------------------

/// Three-catchment layout used by most snap tests.
///
/// Atom 1: [0.5, 0.0, 0.9, 0.4]  centre (0.7, 0.2)
/// Atom 2: [1.0, 0.0, 1.4, 0.4]  centre (1.2, 0.2)
/// Atom 3: [1.5, 0.0, 1.9, 0.4]  centre (1.7, 0.2)
fn three_catchment_specs() -> Vec<CatchmentSpec> {
    vec![
        (1, 10.0, None, 0.5, 0.0, 0.9, 0.4),
        (2, 10.0, None, 1.0, 0.0, 1.4, 0.4),
        (3, 10.0, None, 1.5, 0.0, 1.9, 0.4),
    ]
}

/// Build a complete 3-catchment dataset with Point snap targets at each
/// catchment centre (0.7, 0.2), (1.2, 0.2), (1.7, 0.2).
fn build_3c_snap_dataset() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let catchments = three_catchment_specs();
    let ids: Vec<i64> = catchments.iter().map(|c| c.0).collect();

    write_manifest(&root, catchments.len(), true);
    write_graph(&root, &ids);
    write_catchments(&root, &catchments);

    let geom1 = SnapGeom::Point(0.7, 0.2);
    let geom2 = SnapGeom::Point(1.2, 0.2);
    let geom3 = SnapGeom::Point(1.7, 0.2);
    let snaps: &[SnapSpec<'_>] = &[
        (1, 1, 100.0, true, &geom1),
        (2, 2, 100.0, true, &geom2),
        (3, 3, 100.0, true, &geom3),
    ];
    write_snap(&root, snaps);

    (dir, root)
}

/// Build a complete 3-catchment dataset without a snap file (PiP path).
fn build_3c_pip_dataset() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let catchments = three_catchment_specs();
    let ids: Vec<i64> = catchments.iter().map(|c| c.0).collect();

    write_manifest(&root, catchments.len(), false);
    write_graph(&root, &ids);
    write_catchments(&root, &catchments);

    (dir, root)
}

// ---------------------------------------------------------------------------
// Test 1: snap_happy_path
// ---------------------------------------------------------------------------

#[test]
fn snap_happy_path() {
    let (_dir, root) = build_3c_snap_dataset();
    let session = DatasetSession::open(&root).unwrap();

    // Outlet exactly on snap target 2
    let outlet = GeoCoord::new(1.2, 0.2);
    let config =
        ResolverConfig::new().with_search_radius(SearchRadiusMetres::new(5_000.0));

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 2);
    assert!(
        matches!(result.method, ResolutionMethod::Snap { .. }),
        "expected Snap method, got {:?}",
        result.method
    );
}

// ---------------------------------------------------------------------------
// Test 2: snap_nearest_wins
// ---------------------------------------------------------------------------

#[test]
fn snap_nearest_wins() {
    let (_dir, root) = build_3c_snap_dataset();
    let session = DatasetSession::open(&root).unwrap();

    // Outlet at (1.19, 0.2) — nearest to target 2 at (1.2, 0.2)
    // Target 1 is at (0.7, 0.2) — much further away
    let outlet = GeoCoord::new(1.19, 0.2);
    let config =
        ResolverConfig::new().with_search_radius(SearchRadiusMetres::new(100_000.0));

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 2, "nearest target should be atom 2");
}

// ---------------------------------------------------------------------------
// Test 3: snap_weight_tie_break
// ---------------------------------------------------------------------------

#[test]
fn snap_weight_tie_break() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let catchments: &[CatchmentSpec] = &[
        (1, 10.0, None, 0.0, 0.0, 0.5, 0.4),
        (2, 10.0, None, 0.5, 0.0, 1.0, 0.4),
    ];
    let ids: Vec<i64> = catchments.iter().map(|c| c.0).collect();

    write_manifest(&root, catchments.len(), true);
    write_graph(&root, &ids);
    write_catchments(&root, catchments);

    // Two snap targets mirror-symmetric around lon=0.25:
    //   target 1 at (0.249, 0.2), weight=50  (lower weight)
    //   target 2 at (0.251, 0.2), weight=100 (higher weight)
    // Outlet at (0.25, 0.2) → equidistant from both.
    let g1 = SnapGeom::Point(0.249, 0.2);
    let g2 = SnapGeom::Point(0.251, 0.2);
    let snaps: &[SnapSpec<'_>] = &[
        (1, 1, 50.0, true, &g1),
        (2, 2, 100.0, true, &g2),
    ];
    write_snap(&root, snaps);

    let session = DatasetSession::open(&root).unwrap();
    let outlet = GeoCoord::new(0.25, 0.2);
    let config = ResolverConfig::new().with_search_radius(SearchRadiusMetres::new(5_000.0));

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 2, "higher weight should win tie-break");
}

// ---------------------------------------------------------------------------
// Test 4: snap_mainstem_tie_break
// ---------------------------------------------------------------------------

#[test]
fn snap_mainstem_tie_break() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let catchments: &[CatchmentSpec] = &[
        (1, 10.0, None, 0.0, 0.0, 0.5, 0.4),
        (2, 10.0, None, 0.5, 0.0, 1.0, 0.4),
    ];
    let ids: Vec<i64> = catchments.iter().map(|c| c.0).collect();

    write_manifest(&root, catchments.len(), true);
    write_graph(&root, &ids);
    write_catchments(&root, catchments);

    // Same weight (100.0) but target 1 is tributary, target 2 is mainstem.
    // Outlet equidistant from both.
    let g1 = SnapGeom::Point(0.249, 0.2);
    let g2 = SnapGeom::Point(0.251, 0.2);
    let snaps: &[SnapSpec<'_>] = &[
        (1, 1, 100.0, false, &g1), // tributary
        (2, 2, 100.0, true, &g2),  // mainstem
    ];
    write_snap(&root, snaps);

    let session = DatasetSession::open(&root).unwrap();
    let outlet = GeoCoord::new(0.25, 0.2);
    let config = ResolverConfig::new().with_search_radius(SearchRadiusMetres::new(5_000.0));

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 2, "mainstem should win tie-break over tributary");
}

// ---------------------------------------------------------------------------
// Test 5: snap_no_candidates
// ---------------------------------------------------------------------------

#[test]
fn snap_no_candidates() {
    let (_dir, root) = build_3c_snap_dataset();
    let session = DatasetSession::open(&root).unwrap();

    // Outlet far from all targets, tiny search radius
    let outlet = GeoCoord::new(50.0, 50.0);
    let config = ResolverConfig::new().with_search_radius(SearchRadiusMetres::new(100.0));

    let err = resolve_outlet(&session, outlet, &config).unwrap_err();

    assert!(
        matches!(err, OutletResolutionError::NoSnapCandidates { .. }),
        "expected NoSnapCandidates, got {:?}",
        err
    );
}

// ---------------------------------------------------------------------------
// Test 6: pip_happy_path
// ---------------------------------------------------------------------------

#[test]
fn pip_happy_path() {
    let (_dir, root) = build_3c_pip_dataset();
    let session = DatasetSession::open(&root).unwrap();

    // Outlet inside atom 2's polygon [1.0, 0.0, 1.4, 0.4]
    let outlet = GeoCoord::new(1.2, 0.2);
    let config = ResolverConfig::new();

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 2);
    assert!(
        matches!(result.method, ResolutionMethod::PointInPolygon { .. }),
        "expected PointInPolygon method, got {:?}",
        result.method
    );
}

// ---------------------------------------------------------------------------
// Test 7: pip_upstream_area_tie_break
// ---------------------------------------------------------------------------

#[test]
fn pip_upstream_area_tie_break() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    // Two adjacent catchments sharing the edge at lon=1.0.
    // Outlet at exactly (1.0, 0.2) sits on the shared boundary — both
    // catchments intersect it, so the upstream-area tie-break fires.
    let catchments: &[CatchmentSpec] = &[
        (1, 10.0, Some(500.0), 0.5, 0.0, 1.0, 0.4),  // up_area = 500
        (2, 10.0, Some(1000.0), 1.0, 0.0, 1.5, 0.4), // up_area = 1000
    ];
    let ids: Vec<i64> = catchments.iter().map(|c| c.0).collect();

    write_manifest(&root, catchments.len(), false);
    write_graph(&root, &ids);
    write_catchments(&root, catchments);

    let session = DatasetSession::open(&root).unwrap();
    // Outlet on the shared edge — triggers intersects fallback + tie-break
    let outlet = GeoCoord::new(1.0, 0.2);
    let config = ResolverConfig::new();

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 2, "higher upstream_area should win");
    assert!(
        matches!(
            result.method,
            ResolutionMethod::PointInPolygon { tie_break: Some(shed_core::resolver::PipTieBreak::HighestUpstreamArea), .. }
        ),
        "expected HighestUpstreamArea tie-break, got {:?}",
        result.method
    );
}

// ---------------------------------------------------------------------------
// Test 8: pip_outside_all
// ---------------------------------------------------------------------------

#[test]
fn pip_outside_all() {
    let (_dir, root) = build_3c_pip_dataset();
    let session = DatasetSession::open(&root).unwrap();

    let outlet = GeoCoord::new(50.0, 50.0);
    let config = ResolverConfig::new();

    let err = resolve_outlet(&session, outlet, &config).unwrap_err();

    assert!(
        matches!(err, OutletResolutionError::OutsideAllCatchments { .. }),
        "expected OutsideAllCatchments, got {:?}",
        err
    );
}

// ---------------------------------------------------------------------------
// Test 9: dispatch_snap_over_pip
// ---------------------------------------------------------------------------

#[test]
fn dispatch_snap_over_pip() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let catchments = three_catchment_specs();
    let ids: Vec<i64> = catchments.iter().map(|c| c.0).collect();

    write_manifest(&root, catchments.len(), true);
    write_graph(&root, &ids);
    write_catchments(&root, &catchments);

    // Snap target for atom 1 is placed at (1.2, 0.2) — inside atom 2's polygon.
    // PiP would return atom 2 but snap path should return atom 1.
    let g1 = SnapGeom::Point(1.2, 0.2);
    let g2 = SnapGeom::Point(1.2, 0.2); // same position, different catchment_id
    // Give atom 1's target a higher weight so it wins any distance tie.
    let snaps: &[SnapSpec<'_>] = &[
        (1, 1, 200.0, true, &g1), // catchment 1, placed at (1.2, 0.2), weight=200
        (2, 2, 100.0, true, &g2), // catchment 2, same point,            weight=100
    ];
    write_snap(&root, snaps);

    let session = DatasetSession::open(&root).unwrap();
    // Outlet at (1.2, 0.2) — inside atom 2 by PiP, but snap wins with atom 1
    let outlet = GeoCoord::new(1.2, 0.2);
    let config =
        ResolverConfig::new().with_search_radius(SearchRadiusMetres::new(5_000.0));

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 1, "snap path should return atom 1, not PiP's atom 2");
    assert!(
        matches!(result.method, ResolutionMethod::Snap { .. }),
        "expected Snap method"
    );
}

// ---------------------------------------------------------------------------
// Test 10: dispatch_pip_when_no_snap
// ---------------------------------------------------------------------------

#[test]
fn dispatch_pip_when_no_snap() {
    let (_dir, root) = build_3c_pip_dataset();
    let session = DatasetSession::open(&root).unwrap();

    assert!(session.snap().is_none(), "dataset must have no snap file");

    // Outlet inside atom 2
    let outlet = GeoCoord::new(1.2, 0.2);
    let config = ResolverConfig::new();

    let result = resolve_outlet(&session, outlet, &config).unwrap();

    assert_eq!(result.atom_id.get(), 2);
    assert!(
        matches!(result.method, ResolutionMethod::PointInPolygon { .. }),
        "expected PointInPolygon method, got {:?}",
        result.method
    );
}
