use std::path::Path;
use std::sync::Arc;

use arrow::array::{BinaryBuilder, Float32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hfx_core::{BoundingBox, SnapId, StemRole};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde_json::{Value, json};
use shed_core::error::SessionError;
use shed_core::session::DatasetSession;
use shed_core::testutil::DatasetBuilder;

struct SnapFixtureRow {
    id: i64,
    unit_id: i64,
    weight: f32,
    stem_role: Option<&'static str>,
    geometry: Vec<u8>,
}

fn read_manifest(root: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap()
}

fn write_manifest(root: &Path, manifest: &Value) {
    std::fs::write(root.join("manifest.json"), manifest.to_string()).unwrap();
}

fn set_snap_references_levels(root: &Path, levels: Value) {
    let mut manifest = read_manifest(root);
    manifest["auxiliary"][0]["metadata"]["references_levels"] = levels;
    write_manifest(root, &manifest);
}

fn write_snap_fixture(
    root: &Path,
    rows: &[SnapFixtureRow],
    include_stem_role: bool,
    include_bbox: bool,
) {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("unit_id", DataType::Int64, false),
        Field::new("weight", DataType::Float32, false),
    ];
    if include_stem_role {
        fields.push(Field::new("stem_role", DataType::Utf8, true));
    }
    if include_bbox {
        fields.extend([
            Field::new("bbox_minx", DataType::Float32, true),
            Field::new("bbox_miny", DataType::Float32, true),
            Field::new("bbox_maxx", DataType::Float32, true),
            Field::new("bbox_maxy", DataType::Float32, true),
        ]);
    }
    fields.push(Field::new("geometry", DataType::Binary, false));
    let schema = Arc::new(Schema::new(fields));

    let mut id_b = Int64Builder::new();
    let mut unit_id_b = Int64Builder::new();
    let mut weight_b = Float32Builder::new();
    let mut stem_role_b = StringBuilder::new();
    let mut minx_b = Float32Builder::new();
    let mut miny_b = Float32Builder::new();
    let mut maxx_b = Float32Builder::new();
    let mut maxy_b = Float32Builder::new();
    let mut geom_b = BinaryBuilder::new();

    for row in rows {
        id_b.append_value(row.id);
        unit_id_b.append_value(row.unit_id);
        weight_b.append_value(row.weight);
        if include_stem_role {
            match row.stem_role {
                Some(role) => stem_role_b.append_value(role),
                None => stem_role_b.append_null(),
            }
        }
        if include_bbox {
            let (minx, miny, maxx, maxy) = fixture_bbox(row.id);
            minx_b.append_value(minx);
            miny_b.append_value(miny);
            maxx_b.append_value(maxx);
            maxy_b.append_value(maxy);
        }
        geom_b.append_value(&row.geometry);
    }

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(id_b.finish()),
        Arc::new(unit_id_b.finish()),
        Arc::new(weight_b.finish()),
    ];
    if include_stem_role {
        columns.push(Arc::new(stem_role_b.finish()));
    }
    if include_bbox {
        columns.extend([
            Arc::new(minx_b.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(miny_b.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(maxx_b.finish()) as Arc<dyn arrow::array::Array>,
            Arc::new(maxy_b.finish()) as Arc<dyn arrow::array::Array>,
        ]);
    }
    columns.push(Arc::new(geom_b.finish()));

    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let file = std::fs::File::create(root.join("snap.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn fixture_bbox(id: i64) -> (f32, f32, f32, f32) {
    let x = id as f32;
    (x, 0.0, x + 0.25, 0.25)
}

fn point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1);
    wkb.extend_from_slice(&1u32.to_le_bytes());
    wkb.extend_from_slice(&x.to_le_bytes());
    wkb.extend_from_slice(&y.to_le_bytes());
    wkb
}

fn linestring_wkb(x1: f64, y1: f64, x2: f64, y2: f64) -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1);
    wkb.extend_from_slice(&2u32.to_le_bytes());
    wkb.extend_from_slice(&2u32.to_le_bytes());
    for (x, y) in [(x1, y1), (x2, y2)] {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}

fn polygon_wkb() -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1);
    wkb.extend_from_slice(&3u32.to_le_bytes());
    wkb.extend_from_slice(&1u32.to_le_bytes());
    wkb.extend_from_slice(&5u32.to_le_bytes());
    for (x, y) in [
        (0.0_f64, 0.0_f64),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0),
    ] {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}

#[test]
fn snap_aux_loads_four_stem_roles_point_and_linestring_geometry() {
    let (_dir, root) = DatasetBuilder::new(4).with_snap().build();
    write_snap_fixture(
        &root,
        &[
            SnapFixtureRow {
                id: 1,
                unit_id: 1,
                weight: 10.0,
                stem_role: Some("mainstem"),
                geometry: point_wkb(1.1, 0.1),
            },
            SnapFixtureRow {
                id: 2,
                unit_id: 2,
                weight: 9.0,
                stem_role: Some("tributary"),
                geometry: linestring_wkb(2.0, 0.0, 2.2, 0.2),
            },
            SnapFixtureRow {
                id: 3,
                unit_id: 3,
                weight: 8.0,
                stem_role: Some("distributary"),
                geometry: point_wkb(3.1, 0.1),
            },
            SnapFixtureRow {
                id: 4,
                unit_id: 4,
                weight: 7.0,
                stem_role: Some("unknown"),
                geometry: linestring_wkb(4.0, 0.0, 4.2, 0.2),
            },
        ],
        true,
        true,
    );

    let session = DatasetSession::open_path(&root).unwrap();
    let snap = session.snap().unwrap();
    let bbox = BoundingBox::new(0.0, -1.0, 5.0, 1.0).unwrap();
    let mut results = snap.query_by_bbox(&bbox).unwrap();
    results.sort_by_key(|target| target.id().get());

    let roles: Vec<_> = results.iter().map(|target| target.stem_role()).collect();
    assert_eq!(
        roles,
        vec![
            Some(StemRole::Mainstem),
            Some(StemRole::Tributary),
            Some(StemRole::Distributary),
            Some(StemRole::Unknown),
        ]
    );
}

#[test]
fn snap_aux_accepts_absent_stem_role_column_and_absent_bbox_columns() {
    let (_dir, root) = DatasetBuilder::new(1).with_snap().build();
    write_snap_fixture(
        &root,
        &[SnapFixtureRow {
            id: 7,
            unit_id: 1,
            weight: 1.0,
            stem_role: None,
            geometry: point_wkb(0.1, 0.1),
        }],
        false,
        false,
    );

    let session = DatasetSession::open_path(&root).unwrap();
    let bbox = BoundingBox::new(0.0, 0.0, 0.2, 0.2).unwrap();
    let results = session.snap().unwrap().query_by_bbox(&bbox).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), SnapId::new(7).unwrap());
    assert_eq!(results[0].stem_role(), None);
}

#[test]
fn snap_aux_invalid_stem_role_is_typed() {
    let (_dir, root) = DatasetBuilder::new(1).with_snap().build();
    write_snap_fixture(
        &root,
        &[SnapFixtureRow {
            id: 9,
            unit_id: 1,
            weight: 1.0,
            stem_role: Some("channel-boss"),
            geometry: point_wkb(0.1, 0.1),
        }],
        true,
        true,
    );

    let err = DatasetSession::open_path(&root).unwrap_err();

    assert!(matches!(
        err,
        SessionError::InvalidStemRole {
            row: 0,
            ref value
        } if value == "channel-boss"
    ));
}

#[test]
fn snap_aux_missing_unit_id_reports_real_snap_id() {
    let (_dir, root) = DatasetBuilder::new(1).with_snap().build();
    write_snap_fixture(
        &root,
        &[SnapFixtureRow {
            id: 42,
            unit_id: 999,
            weight: 1.0,
            stem_role: Some("mainstem"),
            geometry: point_wkb(0.1, 0.1),
        }],
        true,
        true,
    );

    let err = DatasetSession::open_path(&root).unwrap_err();

    assert!(matches!(
        err,
        SessionError::SnapReferentialIntegrity {
            snap_id: 42,
            unit_id: 999,
            ..
        }
    ));
}

#[test]
fn snap_aux_references_levels_mismatch_reports_real_snap_id() {
    let (_dir, root) = DatasetBuilder::new(1).with_snap().build();
    set_snap_references_levels(&root, json!([1]));
    write_snap_fixture(
        &root,
        &[SnapFixtureRow {
            id: 77,
            unit_id: 1,
            weight: 1.0,
            stem_role: Some("mainstem"),
            geometry: point_wkb(0.1, 0.1),
        }],
        true,
        true,
    );

    let err = DatasetSession::open_path(&root).unwrap_err();

    assert!(matches!(
        err,
        SessionError::SnapReferentialIntegrity {
            snap_id: 77,
            unit_id: 1,
            ..
        }
    ));
}

#[test]
fn snap_aux_rejects_non_point_or_linestring_wkb() {
    let (_dir, root) = DatasetBuilder::new(1).with_snap().build();
    write_snap_fixture(
        &root,
        &[SnapFixtureRow {
            id: 88,
            unit_id: 1,
            weight: 1.0,
            stem_role: Some("mainstem"),
            geometry: polygon_wkb(),
        }],
        true,
        true,
    );

    let err = DatasetSession::open_path(&root).unwrap_err();

    assert!(matches!(
        err,
        SessionError::SnapGeometryInvalid {
            row: 0,
            ref reason
        } if reason.contains("Polygon")
    ));
}
