use serde_json::{json, Value};
use shed_core::error::SessionError;
use shed_core::session::DatasetSession;
use shed_core::testutil::DatasetBuilder;

fn read_manifest(root: &std::path::Path) -> Value {
    serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap()
}

fn write_manifest(root: &std::path::Path, manifest: &Value) {
    std::fs::write(root.join("manifest.json"), manifest.to_string()).unwrap();
}

fn push_auxiliary(root: &std::path::Path, aux: Value) {
    let mut manifest = read_manifest(root);
    manifest["auxiliary"].as_array_mut().unwrap().push(aux);
    write_manifest(root, &manifest);
}

#[test]
fn loads_minimal_v021_dataset() {
    let (_dir, root) = DatasetBuilder::new(3).build();

    let session = DatasetSession::open_path(&root).expect("v0.2.1 fixture should load");

    assert_eq!(session.manifest().format_version().to_string(), "0.2.1");
    assert_eq!(session.manifest().unit_count().get(), 3);
    assert_eq!(session.graph().len(), 3);
}

#[test]
fn manifest_v01_rejected_before_missing_v021_fields() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("manifest.json"),
        r#"{"format_version":"0.1","fabric_name":"testfabric"}"#,
    )
    .unwrap();
    std::fs::write(dir.path().join("graph.parquet"), []).unwrap();
    std::fs::write(dir.path().join("catchments.parquet"), []).unwrap();

    let err = DatasetSession::open_path(dir.path()).unwrap_err();
    assert!(matches!(
        err,
        SessionError::UnsupportedFormatVersion { ref found, .. } if found == "0.1"
    ));
}

#[test]
fn manifest_wrong_version_rejected_before_later_required_fields() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("manifest.json"),
        r#"{"format_version":"0.3.0","auxiliary":[{"artifacts":{}}]}"#,
    )
    .unwrap();
    std::fs::write(dir.path().join("graph.parquet"), []).unwrap();
    std::fs::write(dir.path().join("catchments.parquet"), []).unwrap();

    let err = DatasetSession::open_path(dir.path()).unwrap_err();
    assert!(matches!(
        err,
        SessionError::UnsupportedFormatVersion { ref found, .. } if found == "0.3.0"
    ));
}

#[test]
fn manifest_unsupported_crs_is_typed() {
    let (_dir, root) = DatasetBuilder::new(3).build();
    let mut manifest = read_manifest(&root);
    manifest["crs"] = json!("EPSG:3857");
    write_manifest(&root, &manifest);

    let err = DatasetSession::open_path(&root).unwrap_err();
    assert!(matches!(
        err,
        SessionError::UnsupportedCrs { ref found, .. } if found == "EPSG:3857"
    ));
}

#[test]
fn manifest_unit_count_mismatch_is_typed() {
    let (_dir, root) = DatasetBuilder::new(3).build();
    let mut manifest = read_manifest(&root);
    manifest["unit_count"] = json!(4);
    write_manifest(&root, &manifest);

    let err = DatasetSession::open_path(&root).unwrap_err();
    assert!(matches!(
        err,
        SessionError::UnitCountMismatch {
            manifest_count: 4,
            actual_count: 3
        }
    ));
}

#[test]
fn auxiliary_d8_missing_or_invalid_metadata_is_typed() {
    let cases = [
        (
            "missing encoding",
            json!({
                "schema": "hfx.aux.d8_raster.v1",
                "artifacts": { "flow_dir": "flow_dir.tif", "flow_acc": "flow_acc.tif" },
                "metadata": {}
            }),
        ),
        (
            "invalid encoding",
            json!({
                "schema": "hfx.aux.d8_raster.v1",
                "artifacts": { "flow_dir": "flow_dir.tif", "flow_acc": "flow_acc.tif" },
                "metadata": { "flow_dir_encoding": "bad" }
            }),
        ),
        (
            "missing artifact key",
            json!({
                "schema": "hfx.aux.d8_raster.v1",
                "artifacts": { "flow_dir": "flow_dir.tif" },
                "metadata": { "flow_dir_encoding": "esri" }
            }),
        ),
    ];

    for (case, aux) in cases {
        let (_dir, root) = DatasetBuilder::new(3).build();
        push_auxiliary(&root, aux);

        let err = DatasetSession::open_path(&root).unwrap_err();
        assert!(
            matches!(
                err,
                SessionError::AuxiliaryDeclParse { ref schema, .. }
                    if schema == "hfx.aux.d8_raster.v1"
            ),
            "{case}: got {err}"
        );
    }
}

#[test]
fn auxiliary_d8_path_escape_is_typed() {
    let (_dir, root) = DatasetBuilder::new(3).build();
    push_auxiliary(
        &root,
        json!({
            "schema": "hfx.aux.d8_raster.v1",
            "artifacts": { "flow_dir": "../flow_dir.tif", "flow_acc": "flow_acc.tif" },
            "metadata": { "flow_dir_encoding": "esri" }
        }),
    );

    let err = DatasetSession::open_path(&root).unwrap_err();
    assert!(matches!(
        err,
        SessionError::AuxiliaryPathEscape {
            ref schema,
            ref artifact,
            ..
        } if schema == "hfx.aux.d8_raster.v1" && artifact == "flow_dir"
    ));
}

#[test]
fn auxiliary_d8_declared_but_missing_artifact_is_typed() {
    let (_dir, root) = DatasetBuilder::new(3).build();
    push_auxiliary(
        &root,
        json!({
            "schema": "hfx.aux.d8_raster.v1",
            "artifacts": { "flow_dir": "flow_dir.tif", "flow_acc": "flow_acc.tif" },
            "metadata": { "flow_dir_encoding": "esri" }
        }),
    );

    let err = DatasetSession::open_path(&root).unwrap_err();
    assert!(matches!(
        err,
        SessionError::AuxiliaryArtifactMissing {
            ref schema,
            ref artifact,
            ..
        } if schema == "hfx.aux.d8_raster.v1" && artifact == "flow_acc"
            || schema == "hfx.aux.d8_raster.v1" && artifact == "flow_dir"
    ));
}

#[test]
fn auxiliary_snap_missing_or_invalid_metadata_is_typed() {
    let cases = [
        (
            "missing name",
            json!({
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap.parquet" },
                "metadata": {
                    "description": "Synthetic snap targets.",
                    "references_levels": [0],
                    "weight_semantics": "higher is preferred"
                }
            }),
        ),
        (
            "empty name",
            json!({
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap.parquet" },
                "metadata": {
                    "name": "",
                    "description": "Synthetic snap targets.",
                    "references_levels": [0],
                    "weight_semantics": "higher is preferred"
                }
            }),
        ),
        (
            "missing description",
            json!({
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap.parquet" },
                "metadata": {
                    "name": "test-snap",
                    "references_levels": [0],
                    "weight_semantics": "higher is preferred"
                }
            }),
        ),
        (
            "empty references_levels",
            json!({
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap.parquet" },
                "metadata": {
                    "name": "test-snap",
                    "description": "Synthetic snap targets.",
                    "references_levels": [],
                    "weight_semantics": "higher is preferred"
                }
            }),
        ),
        (
            "negative references_levels",
            json!({
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap.parquet" },
                "metadata": {
                    "name": "test-snap",
                    "description": "Synthetic snap targets.",
                    "references_levels": [-1],
                    "weight_semantics": "higher is preferred"
                }
            }),
        ),
        (
            "non-integer references_levels",
            json!({
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap.parquet" },
                "metadata": {
                    "name": "test-snap",
                    "description": "Synthetic snap targets.",
                    "references_levels": ["0"],
                    "weight_semantics": "higher is preferred"
                }
            }),
        ),
        (
            "missing weight_semantics",
            json!({
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap.parquet" },
                "metadata": {
                    "name": "test-snap",
                    "description": "Synthetic snap targets.",
                    "references_levels": [0]
                }
            }),
        ),
    ];

    for (case, aux) in cases {
        let (_dir, root) = DatasetBuilder::new(3).build();
        push_auxiliary(&root, aux);

        let err = DatasetSession::open_path(&root).unwrap_err();
        assert!(
            matches!(err, SessionError::SnapAuxMetadataInvalid { .. }),
            "{case}: got {err}"
        );
    }
}

#[test]
fn auxiliary_snap_path_escape_is_typed() {
    let (_dir, root) = DatasetBuilder::new(3).build();
    push_auxiliary(
        &root,
        json!({
            "schema": "hfx.aux.snap.v1",
            "artifacts": { "snap": "/tmp/snap.parquet" },
            "metadata": {
                "name": "test-snap",
                "description": "Synthetic snap targets.",
                "references_levels": [0],
                "weight_semantics": "higher is preferred"
            }
        }),
    );

    let err = DatasetSession::open_path(&root).unwrap_err();
    assert!(matches!(
        err,
        SessionError::AuxiliaryPathEscape {
            ref schema,
            ref artifact,
            ..
        } if schema == "hfx.aux.snap.v1" && artifact == "snap"
    ));
}

#[test]
fn auxiliary_generic_reverse_dns_loads_as_uninterpreted_handle() {
    let (_dir, root) = DatasetBuilder::new(3).build();
    let artifact_rel = "extra/custom.bin";
    std::fs::create_dir(root.join("extra")).unwrap();
    std::fs::write(root.join(artifact_rel), b"custom").unwrap();
    push_auxiliary(
        &root,
        json!({
            "schema": "org.example.custom.v1",
            "artifacts": { "data": artifact_rel },
            "metadata": { "name": "not-a-blessed-schema" }
        }),
    );

    let session = DatasetSession::open_path(&root).unwrap();
    let aux = session.auxiliary_declarations();

    assert!(aux.d8_rasters.is_empty());
    assert!(aux.snaps.is_empty());
    assert_eq!(aux.generic.len(), 1);
    assert_eq!(aux.generic[0].schema, "org.example.custom.v1");
    assert_eq!(aux.generic[0].artifacts["data"], artifact_rel);
    assert_eq!(aux.generic[0].metadata["name"], "not-a-blessed-schema");
    assert!(root.join(&aux.generic[0].artifacts["data"]).is_file());
}

#[test]
fn auxiliary_generic_path_escape_is_typed() {
    let (_dir, root) = DatasetBuilder::new(3).build();
    push_auxiliary(
        &root,
        json!({
            "schema": "org.example.custom.v1",
            "artifacts": { "data": "../custom.bin" },
            "metadata": { "name": "not-a-blessed-schema" }
        }),
    );

    let err = DatasetSession::open_path(&root).unwrap_err();
    assert!(matches!(
        err,
        SessionError::AuxiliaryPathEscape {
            ref schema,
            ref artifact,
            ..
        } if schema == "org.example.custom.v1" && artifact == "data"
    ));
}
