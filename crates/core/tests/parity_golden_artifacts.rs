//! Loader-independent validation for committed parity golden artifacts.
//!
//! This test intentionally imports no `DatasetBuilder`, `DatasetSession`,
//! `Engine`, `AtomId`, `hfx_core`, or other v0.1-loader-only type. That
//! invariant is load-bearing: this artifact harness must keep passing after M2
//! deletes the v0.1 loader and leaves the committed M1 goldens as inert bytes.

use std::fs;
use std::path::Path;
use std::process::Command;

use geo::{MultiPolygon, polygon};
use geozero::ToGeo;
use geozero::wkb::Wkb;
use serde::Deserialize;
use shed_core::algo::{
    CANONICAL_WKB_DECIMAL_PRECISION, CANONICAL_WKB_VERSION, canonical_wkb_multi_polygon,
};

const FIXTURE_DIR: &str = "tests/fixtures/parity";

#[derive(Debug, Deserialize)]
struct GoldenRecord {
    #[serde(default)]
    oracle: String,
    #[serde(default)]
    case_name: String,
    canonical_wkb_hex: String,
    area_km2: f64,
    input_outlet: Outlet,
    resolved_outlet: Outlet,
    refined_outlet: Option<Outlet>,
    terminal_id: i64,
    upstream_ids: Vec<i64>,
    resolution_method: String,
    resolver_config: ResolverConfig,
    refinement_outcome: RefinementOutcome,
    canonicalizer_version: String,
    comparison_policy: ComparisonPolicy,
    #[serde(default)]
    remote_input_identity: Option<RemoteInputIdentity>,
    #[serde(default)]
    window_measurement: Option<WindowMeasurement>,
    #[serde(default)]
    carve_measurement: Option<serde_json::Value>,
    #[serde(default)]
    raster_interpretation: Option<RasterInterpretation>,
    #[serde(default)]
    fixture_provenance: Option<FixtureProvenance>,
    #[serde(default)]
    attestation: Option<Attestation>,
}

#[derive(Debug, Deserialize)]
struct Outlet {
    lon: f64,
    lat: f64,
}

#[derive(Debug, Deserialize)]
struct ResolverConfig {
    search_radius_m: f64,
}

#[derive(Debug, Deserialize)]
struct RefinementOutcome {
    status: String,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComparisonPolicy {
    coordinate_abs_epsilon: f64,
    area_km2_abs_epsilon: f64,
    area_km2_rel_epsilon: f64,
}

#[derive(Debug, Deserialize)]
struct RasterInterpretation {
    dimensions: RasterDimensions,
    crs: String,
    transform: [f64; 6],
    origin: String,
    pixel_size_degrees: PixelSize,
    extent: RasterExtent,
    pixel_interpretation: String,
    flow_direction: RasterBandInterpretation,
    flow_accumulation: RasterBandInterpretation,
}

#[derive(Debug, Deserialize)]
struct RasterDimensions {
    columns: usize,
    rows: usize,
}

#[derive(Debug, Deserialize)]
struct PixelSize {
    x: f64,
    y: f64,
}

#[derive(Debug, Deserialize)]
struct RasterExtent {
    x_min: f64,
    x_max: f64,
    y_min: f64,
    y_max: f64,
}

#[derive(Debug, Deserialize)]
struct RasterBandInterpretation {
    sample_type: String,
    encoding: String,
    nodata: String,
}

#[derive(Debug, Deserialize)]
struct FixtureProvenance {
    content_hash_algorithm: String,
    files: Vec<FileProvenance>,
}

#[derive(Debug, Deserialize)]
struct FileProvenance {
    path: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct Attestation {
    local_tiff_raster_source_gdal_tile_parity: String,
    proof_command: String,
}

#[derive(Debug, Deserialize)]
struct RemoteInputIdentity {
    pinned_url: String,
    artifacts: Vec<RemoteArtifactIdentity>,
}

#[derive(Debug, Deserialize)]
struct RemoteArtifactIdentity {
    path: String,
    etag: String,
    content_length: u64,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct WindowMeasurement {
    terminal_bbox: RectRecord,
    search_radius_m: f64,
    flow_dir: RasterWindowStats,
    flow_acc: RasterWindowStats,
    http_total_bytes_in: u64,
    windowing_ceiling_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct RasterWindowStats {
    tile_count: u64,
    bytes: u64,
}

#[derive(Debug, Deserialize)]
struct RectRecord {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

#[test]
fn committed_seed_golden_validates_schema_and_canonical_wkb() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE_DIR)
        .join("seed_golden.json");
    let record: GoldenRecord = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("seed golden fixture should be readable"),
    )
    .expect("seed golden should match the golden schema");

    assert_record_contract(&record);

    let expected_wkb = decode_hex(&record.canonical_wkb_hex);
    let seed_geometry = seed_geometry();
    let actual_wkb =
        canonical_wkb_multi_polygon(&seed_geometry).expect("seed geometry should canonicalize");

    assert_eq!(actual_wkb, expected_wkb);
    assert_canonical_wkb_idempotent(&actual_wkb);
}

#[test]
fn committed_synthetic_refined_b_golden_validates_schema_and_canonical_wkb() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE_DIR)
        .join("goldens/v01_synthetic_refined/oracle_b_synthetic_refined.json");
    let record: GoldenRecord = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("B golden fixture should be readable"),
    )
    .expect("B golden should match the golden schema");

    assert_record_contract(&record);
    assert_synthetic_refined_b_contract(&record);
    assert_b_raster_fixture_bytes_match_recorded_hashes(&record);
    assert_canonical_wkb_idempotent(&decode_hex(&record.canonical_wkb_hex));
}

#[test]
fn committed_grit_nonrefined_a_goldens_validate_schema_and_metadata_offline() {
    let records = read_golden_array("goldens/v01_grit_nonrefined/oracle_a_grit_nonrefined.json");
    assert_eq!(records.len(), 2);
    let names = records
        .iter()
        .map(|record| record.case_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["zurich", "repparfjord"]);

    for record in &records {
        assert_record_contract(record);
        assert_eq!(record.oracle, "A");
        assert_eq!(record.refinement_outcome.status, "NotApplied");
        assert_eq!(
            record.refinement_outcome.reason.as_deref(),
            Some("no rasters available")
        );
        assert!(record.raster_interpretation.is_none());
        assert!(record.window_measurement.is_none());
        assert!(record.carve_measurement.is_none());
        assert_remote_provenance_is_inert(record);
        assert_remote_identity(
            record,
            "https://basin-delineations-public.upstream.tech/grit/1.0.0/",
            &["manifest.json", "catchments.parquet", "graph.arrow"],
        );
        assert_canonical_wkb_idempotent(&decode_hex(&record.canonical_wkb_hex));
    }
}

#[test]
fn committed_merit_refined_c_goldens_validate_schema_and_metadata_offline() {
    let records = read_golden_array("goldens/v01_merit_refined/oracle_c_merit_refined.json");
    assert_eq!(records.len(), 1);
    let names = records
        .iter()
        .map(|record| record.case_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["rhine_basel"]);

    for record in &records {
        assert_record_contract(record);
        assert_eq!(record.oracle, "C");
        assert_eq!(record.case_name, "rhine_basel");
        assert_eq!(record.refinement_outcome.status, "Applied");
        assert!(record.carve_measurement.is_none());
        assert_remote_provenance_is_inert(record);
        assert_remote_identity(
            record,
            "https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/",
            &[
                "manifest.json",
                "catchments.parquet",
                "graph.arrow",
                "snap.parquet",
                "flow_dir.tif",
                "flow_acc.tif",
            ],
        );
        assert_merit_refined_c_contract(record);
        assert_c_raster_remote_identity_has_no_content_hashes(record);
        assert_canonical_wkb_idempotent(&decode_hex(&record.canonical_wkb_hex));
    }
}

#[test]
fn in_test_geometry_canonical_wkb_is_idempotent_without_loader_dependencies() {
    let geometry = MultiPolygon::new(vec![
        polygon![
            (x: 2.0, y: 0.0),
            (x: 0.0, y: 0.0),
            (x: 0.0, y: 1.0),
            (x: 2.0, y: 1.0)
        ],
        polygon![
            (x: -1.0, y: 0.0),
            (x: -1.0, y: 1.0),
            (x: -2.0, y: 1.0),
            (x: -2.0, y: 0.0)
        ],
    ]);

    let canonical =
        canonical_wkb_multi_polygon(&geometry).expect("in-test geometry should canonicalize");
    assert_canonical_wkb_idempotent(&canonical);
}

fn assert_record_contract(record: &GoldenRecord) {
    assert!(!record.canonical_wkb_hex.is_empty());
    assert_eq!(record.canonical_wkb_hex.len() % 2, 0);
    assert!(
        record
            .canonical_wkb_hex
            .chars()
            .all(|c| c.is_ascii_hexdigit())
    );
    assert_eq!(record.canonicalizer_version, CANONICAL_WKB_VERSION);
    assert_eq!(CANONICAL_WKB_DECIMAL_PRECISION, 6);
    assert!(record.area_km2.is_finite() && record.area_km2 > 0.0);
    assert_outlet_finite(&record.input_outlet);
    assert_outlet_finite(&record.resolved_outlet);
    assert!(record.terminal_id >= 0);
    assert!(record.upstream_ids.windows(2).all(|ids| ids[0] < ids[1]));
    assert!(!record.upstream_ids.is_empty());
    assert!(!record.resolution_method.is_empty());
    assert!(record.resolver_config.search_radius_m.is_finite());
    assert!(record.resolver_config.search_radius_m >= 0.0);
    assert!(!record.refinement_outcome.status.is_empty());
    if record.refinement_outcome.status == "Applied" {
        assert_outlet_finite(
            record
                .refined_outlet
                .as_ref()
                .expect("Applied refinement should record refined outlet"),
        );
    } else {
        assert!(record.refined_outlet.is_none());
    }
    if let Some(reason) = &record.refinement_outcome.reason {
        assert!(!reason.is_empty());
    }
    assert_eq!(record.comparison_policy.coordinate_abs_epsilon, 0.000001);
    assert!(record.comparison_policy.area_km2_abs_epsilon > 0.0);
    assert!(record.comparison_policy.area_km2_rel_epsilon > 0.0);
}

fn assert_synthetic_refined_b_contract(record: &GoldenRecord) {
    assert_eq!(record.refinement_outcome.status, "Applied");
    assert_eq!(record.terminal_id, 1);
    assert_eq!(record.upstream_ids, [1]);
    assert!(record.area_km2 > 0.0);

    let raster = record
        .raster_interpretation
        .as_ref()
        .expect("B golden should record raster interpretation metadata");
    assert_eq!(raster.dimensions.columns, 5);
    assert_eq!(raster.dimensions.rows, 5);
    assert_eq!(raster.crs, "EPSG:4326");
    assert_eq!(raster.transform, [0.0, 1.0, 0.0, 0.0, 0.0, -1.0]);
    assert!(!raster.origin.is_empty());
    assert_eq!(raster.pixel_size_degrees.x, 1.0);
    assert_eq!(raster.pixel_size_degrees.y, -1.0);
    assert_eq!(raster.extent.x_min, 0.0);
    assert_eq!(raster.extent.x_max, 5.0);
    assert_eq!(raster.extent.y_min, -5.0);
    assert_eq!(raster.extent.y_max, 0.0);
    assert_outlet_in_raster_extent(
        record
            .refined_outlet
            .as_ref()
            .expect("B golden should record refined outlet"),
        &raster.extent,
    );
    assert!(raster.pixel_interpretation.contains("PixelIsArea"));
    assert_eq!(raster.flow_direction.sample_type, "uint8");
    assert_eq!(raster.flow_direction.encoding, "ESRI D8");
    assert_eq!(raster.flow_direction.nodata, "255");
    assert_eq!(raster.flow_accumulation.sample_type, "float32");
    assert_eq!(raster.flow_accumulation.encoding, "accumulation");
    assert!(raster.flow_accumulation.nodata.contains("NaN"));

    let provenance = record
        .fixture_provenance
        .as_ref()
        .expect("B golden should record inert fixture provenance");
    assert_eq!(provenance.content_hash_algorithm, "sha256");
    let paths = provenance
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        [
            "manifest.json",
            "catchments.parquet",
            "graph.arrow",
            "flow_dir.tif",
            "flow_acc.tif"
        ]
    );
    for file in &provenance.files {
        assert!(file.size_bytes > 0);
        assert_eq!(file.sha256.len(), 64);
        assert!(file.sha256.chars().all(|c| c.is_ascii_hexdigit()));
    }
    assert_b_fixture_file_metadata(record, "flow_dir.tif", 371);
    assert_b_fixture_file_metadata(record, "flow_acc.tif", 446);

    let attestation = record
        .attestation
        .as_ref()
        .expect("B golden should record raster decode parity attestation");
    assert!(
        attestation
            .local_tiff_raster_source_gdal_tile_parity
            .contains("tile-identical")
    );
    assert!(attestation.proof_command.contains("shed-gdal"));
}

fn assert_remote_identity(record: &GoldenRecord, pinned_url: &str, expected_paths: &[&str]) {
    let identity = record
        .remote_input_identity
        .as_ref()
        .expect("real oracle should record remote input identity");
    assert_eq!(identity.pinned_url, pinned_url);
    let paths = identity
        .artifacts
        .iter()
        .map(|artifact| artifact.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(paths, expected_paths);
    for artifact in &identity.artifacts {
        assert!(!artifact.etag.is_empty());
        assert!(artifact.content_length > 0);
        assert!(
            artifact.extra.is_empty(),
            "unexpected remote identity metadata for {}: {:?}",
            artifact.path,
            artifact.extra
        );
    }
}

fn assert_merit_refined_c_contract(record: &GoldenRecord) {
    assert!(record.area_km2 > 0.0);

    let raster = record
        .raster_interpretation
        .as_ref()
        .expect("C golden should record MERIT raster interpretation");
    assert!(raster.origin.contains("remote COG"));
    assert!(raster.pixel_interpretation.contains("PixelIsArea"));
    assert_eq!(raster.flow_direction.sample_type, "uint8");
    assert_eq!(raster.flow_direction.encoding, "ESRI D8");
    assert_eq!(raster.flow_accumulation.sample_type, "float32");

    let window = record
        .window_measurement
        .as_ref()
        .expect("C golden should record window measurement");
    assert_eq!(window.search_radius_m, 5000.0);
    assert_eq!(
        window.search_radius_m,
        record.resolver_config.search_radius_m
    );
    assert!(window.flow_dir.tile_count > 0);
    assert!(window.flow_acc.tile_count > 0);
    assert!(window.flow_dir.bytes > 0);
    assert!(window.flow_acc.bytes > 0);
    assert!(window.http_total_bytes_in <= window.windowing_ceiling_bytes);
    assert!(window.windowing_ceiling_bytes <= 500 * 1024 * 1024);
    assert_rect_valid(&window.terminal_bbox);

    let attestation = record
        .attestation
        .as_ref()
        .expect("C golden should record GDAL parity attestation");
    assert!(
        attestation
            .local_tiff_raster_source_gdal_tile_parity
            .contains("tile-identical")
    );
    assert!(
        attestation
            .proof_command
            .contains("merit_c_windows_tiff_match_gdal")
    );
}

fn assert_outlet_finite(outlet: &Outlet) {
    assert!(outlet.lon.is_finite());
    assert!(outlet.lat.is_finite());
    assert!((-180.0..=180.0).contains(&outlet.lon));
    assert!((-90.0..=90.0).contains(&outlet.lat));
}

fn assert_outlet_in_raster_extent(outlet: &Outlet, extent: &RasterExtent) {
    assert!((extent.x_min..=extent.x_max).contains(&outlet.lon));
    assert!((extent.y_min..=extent.y_max).contains(&outlet.lat));
}

fn assert_remote_provenance_is_inert(record: &GoldenRecord) {
    let provenance = record
        .fixture_provenance
        .as_ref()
        .expect("remote oracle should record inert fixture provenance marker");
    assert_eq!(provenance.content_hash_algorithm, "sha256");
    assert!(
        provenance.files.is_empty(),
        "remote oracle provenance must remain inert and must not carry hashes"
    );
}

fn assert_b_fixture_file_metadata(record: &GoldenRecord, path: &str, size_bytes: u64) {
    let file = find_fixture_file(record, path);
    assert_eq!(file.size_bytes, size_bytes);
    assert_eq!(file.sha256.len(), 64);
    assert!(file.sha256.chars().all(|c| c.is_ascii_hexdigit()));
}

fn assert_b_raster_fixture_bytes_match_recorded_hashes(record: &GoldenRecord) {
    for raster_path in ["flow_dir.tif", "flow_acc.tif"] {
        let recorded = find_fixture_file(record, raster_path);
        let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(FIXTURE_DIR)
            .join("v01_synthetic_refined")
            .join(raster_path);
        let metadata = fs::metadata(&fixture_path).unwrap_or_else(|error| {
            panic!("B raster fixture should exist at {fixture_path:?}: {error}")
        });
        assert_eq!(metadata.len(), recorded.size_bytes);
        assert_eq!(sha256_file(&fixture_path), recorded.sha256);
    }
}

fn find_fixture_file<'a>(record: &'a GoldenRecord, path: &str) -> &'a FileProvenance {
    record
        .fixture_provenance
        .as_ref()
        .expect("B golden should record fixture provenance")
        .files
        .iter()
        .find(|file| file.path == path)
        .unwrap_or_else(|| panic!("B fixture provenance should record {path}"))
}

fn assert_c_raster_remote_identity_has_no_content_hashes(record: &GoldenRecord) {
    let identity = record
        .remote_input_identity
        .as_ref()
        .expect("C golden should record remote input identity");
    for raster_path in ["flow_dir.tif", "flow_acc.tif"] {
        let artifact = identity
            .artifacts
            .iter()
            .find(|artifact| artifact.path == raster_path)
            .unwrap_or_else(|| panic!("C remote identity should record {raster_path}"));
        assert!(artifact.etag.starts_with('"'));
        assert!(
            artifact.etag.trim_matches('"').contains('-'),
            "C raster ETag should be recorded as the remote multipart ETag"
        );
        assert!(artifact.content_length > 0);
        assert!(
            artifact.extra.is_empty(),
            "C raster identity must be ETag plus Content-Length only"
        );
    }
}

fn assert_rect_valid(rect: &RectRecord) {
    assert!(rect.min_x.is_finite());
    assert!(rect.min_y.is_finite());
    assert!(rect.max_x.is_finite());
    assert!(rect.max_y.is_finite());
    assert!(rect.min_x < rect.max_x);
    assert!(rect.min_y < rect.max_y);
}

fn read_golden_array(relative: &str) -> Vec<GoldenRecord> {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE_DIR)
        .join(relative);
    serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("golden fixture should be readable"),
    )
    .expect("golden array should match the golden schema")
}

fn assert_canonical_wkb_idempotent(canonical: &[u8]) {
    assert_eq!(canonical[0], 0x01);

    let decoded = match Wkb(canonical)
        .to_geo()
        .expect("canonical WKB should decode")
    {
        geo::Geometry::MultiPolygon(mp) => mp,
        other => panic!("expected canonical MultiPolygon WKB, got {other:?}"),
    };
    let normalized_again =
        canonical_wkb_multi_polygon(&decoded).expect("decoded canonical WKB should recanonicalize");

    assert_eq!(canonical, normalized_again);
}

fn seed_geometry() -> MultiPolygon<f64> {
    MultiPolygon::new(vec![polygon![
        (x: 1.0, y: 0.0),
        (x: 1.0, y: 1.0),
        (x: 0.0, y: 1.0),
        (x: 0.0, y: 0.0)
    ]])
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0]);
            let low = hex_digit(pair[1]);
            (high << 4) | low
        })
        .collect()
}

fn sha256_file(path: &Path) -> String {
    let output = Command::new("shasum")
        .args(["-a", "256"])
        .arg(path)
        .output()
        .unwrap_or_else(|error| panic!("shasum should run for {path:?}: {error}"));
    assert!(
        output.status.success(),
        "shasum failed for {path:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("shasum output should be utf8")
        .split_whitespace()
        .next()
        .expect("shasum output should include a hash")
        .to_string()
}

fn hex_digit(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        other => panic!("invalid hex digit {other}"),
    }
}
