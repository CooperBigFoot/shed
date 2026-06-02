//! Capture-time v0.1 parity oracle tests.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use geo::{Area, BoundingRect, Rect};
use hfx_core::AtomId;
use serde::{Deserialize, Serialize};
use shed_core::algo::{
    CANONICAL_WKB_VERSION, GeoCoord, SnapThreshold, canonical_wkb_multi_polygon,
};
use shed_core::session::DatasetSession;
use shed_core::test_raster_source::LocalTiffRasterSource;
use shed_core::{
    DelineationOptions, DelineationResult, Engine, PipTieBreak, RefinementOutcome,
    ResolutionMethod, ResolverConfig, SearchRadiusMetres, SnapStrategy,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use tracing_core::span::Current;

const FIXTURE_ROOT: &str = "tests/fixtures/parity/v01_synthetic_refined";
const GOLDEN_ROOT: &str = "tests/fixtures/parity/goldens/v01_synthetic_refined";
const GOLDEN_FILE: &str = "oracle_b_synthetic_refined.json";
const GRIT_URL: &str = "https://basin-delineations-public.upstream.tech/grit/1.0.0/";
const MERIT_URL: &str = "https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/";
const GRIT_GOLDEN_ROOT: &str = "tests/fixtures/parity/goldens/v01_grit_nonrefined";
const MERIT_GOLDEN_ROOT: &str = "tests/fixtures/parity/goldens/v01_merit_refined";
const GRIT_GOLDEN_FILE: &str = "oracle_a_grit_nonrefined.json";
const MERIT_GOLDEN_FILE: &str = "oracle_c_merit_refined.json";
const TERMINAL_AREA: f64 = 25.0;
const SYNTHETIC_OUTLET: GeoCoord = GeoCoord {
    lon: 2.5,
    lat: -2.5,
};
const STABILITY_RUNS: usize = 3;
const NETWORK_CAPTURE_ENV: &str = "SHED_PARITY_R2_CAPTURE";
const HTTP_STATS_ENV: &str = "PYSHED_BENCH_NET";
const WINDOWING_CEILING_BYTES: u64 = 500 * 1024 * 1024;
const COORDINATE_ABS_EPSILON: f64 = 0.000001;
const AREA_KM2_ABS_EPSILON: f64 = 0.000001;
const AREA_KM2_REL_EPSILON: f64 = 0.000001;

#[test]
fn synthetic_fixture_smoke() {
    let result = delineate_synthetic_refined();

    assert!(
        matches!(result.refinement(), RefinementOutcome::Applied { .. }),
        "expected Applied refinement, got {:?}",
        result.refinement()
    );

    let refined_area = result.geometry().unsigned_area();
    assert!(
        refined_area > 0.0 && refined_area < TERMINAL_AREA,
        "expected strict shrink: 0 < refined_area < {TERMINAL_AREA}, got {refined_area}"
    );

    let terminal_bbox = Rect::new(
        geo::coord! { x: 0.0, y: -5.0 },
        geo::coord! { x: 5.0, y: 0.0 },
    );
    let refined_bbox = result
        .geometry()
        .bounding_rect()
        .expect("refined geometry should have a bbox");
    assert!(
        rect_contains_rect(&terminal_bbox, &refined_bbox),
        "refined bbox {refined_bbox:?} must be contained by terminal bbox {terminal_bbox:?}"
    );
}

#[test]
fn synthetic_refined_matches_committed_golden() {
    let golden = read_golden_record();
    let current = capture_synthetic_refined();

    assert_golden_matches_current(&golden, &current);
}

#[test]
fn synthetic_stability_check() {
    let first = capture_synthetic_refined();

    for run_index in 2..=STABILITY_RUNS {
        let current = capture_synthetic_refined();
        assert_golden_matches_current(&first, &current);
        assert_eq!(
            first.canonical_wkb_hex, current.canonical_wkb_hex,
            "canonical WKB changed on stability run {run_index}"
        );
    }
}

#[test]
fn bless_synthetic_refined() {
    if env::var_os("SHED_PARITY_BLESS").is_none() {
        let golden = read_golden_record();
        let current = capture_synthetic_refined();
        assert_golden_matches_current(&golden, &current);
        return;
    }

    synthetic_stability_check();

    let golden = capture_synthetic_refined_for_bless();
    let golden_path = golden_path();
    fs::create_dir_all(
        golden_path
            .parent()
            .expect("golden file should have a parent"),
    )
    .expect("golden directory should be creatable");
    fs::write(
        golden_path,
        serde_json::to_string_pretty(&golden).expect("golden should serialize") + "\n",
    )
    .expect("golden should be writable");
}

#[test]
#[ignore = "network-gated real-data capture; set SHED_PARITY_R2_CAPTURE=1"]
fn bless_real_oracles_a_and_c() {
    assert_network_capture_enabled();
    unsafe {
        env::set_var(HTTP_STATS_ENV, "1");
    }

    let grit_identity = RemoteInputIdentity::read(
        GRIT_URL,
        &["manifest.json", "catchments.parquet", "graph.arrow"],
    );
    let merit_identity = RemoteInputIdentity::read(
        MERIT_URL,
        &[
            "manifest.json",
            "catchments.parquet",
            "graph.arrow",
            "snap.parquet",
            "flow_dir.tif",
            "flow_acc.tif",
        ],
    );

    let oracle_a_cases = [
        OracleCase::new("zurich", GeoCoord::new(8.5417, 47.3769), 1000.0),
        OracleCase::new("repparfjord", GeoCoord::new(23.04, 69.97), 50000.0),
    ];
    let oracle_a_records = oracle_a_cases
        .iter()
        .map(|case| capture_stable_grit_case(case, &grit_identity))
        .collect::<Vec<_>>();
    write_json(grit_golden_path(), &oracle_a_records);

    let oracle_c_cases = [OracleCase::new(
        "rhine_basel",
        GeoCoord::new(7.5890, 47.5596),
        5000.0,
    )];
    let oracle_c_records = oracle_c_cases
        .iter()
        .map(|case| capture_stable_merit_case(case, &merit_identity))
        .collect::<Vec<_>>();
    write_json(merit_golden_path(), &oracle_c_records);
}

fn assert_network_capture_enabled() {
    assert_eq!(
        env::var(NETWORK_CAPTURE_ENV).as_deref(),
        Ok("1"),
        "{NETWORK_CAPTURE_ENV}=1 is required for real-data oracle capture"
    );
}

fn capture_stable_grit_case(case: &OracleCase, identity: &RemoteInputIdentity) -> GoldenRecord {
    let first = capture_grit_case(case, identity);
    assert_eq!(
        first.refinement_outcome,
        RefinementOutcomeRecord {
            status: "NotApplied".to_string(),
            reason: Some("no rasters available".to_string()),
        },
        "Oracle A must truthfully capture grit/1.0.0 has_rasters=false"
    );
    for run_index in 2..=STABILITY_RUNS {
        let current = capture_grit_case(case, identity);
        assert_oracle_case_matches(case, run_index, &first, &current);
    }
    first
}

fn capture_grit_case(case: &OracleCase, identity: &RemoteInputIdentity) -> GoldenRecord {
    let session = DatasetSession::open(GRIT_URL).expect("grit/1.0.0 remote session should open");
    let engine = Engine::builder(session).build();
    let result = engine
        .delineate(case.outlet, &case.options())
        .unwrap_or_else(|error| panic!("Oracle A {} should delineate: {error}", case.name));
    GoldenRecord::from_result_with_context(
        &result,
        case,
        OracleMetadata::grit_nonrefined(identity.clone()),
        None,
    )
}

fn capture_stable_merit_case(case: &OracleCase, identity: &RemoteInputIdentity) -> GoldenRecord {
    let first = capture_merit_case(case, identity);
    let metrics = first
        .window_measurement
        .as_ref()
        .expect("Oracle C should record window measurements");
    assert!(
        metrics.http_total_bytes_in <= WINDOWING_CEILING_BYTES,
        "Oracle C {} exceeded windowing ceiling: {:?}",
        case.name,
        metrics
    );
    assert!(metrics.flow_dir.tile_count > 0);
    assert!(metrics.flow_acc.tile_count > 0);

    assert_refined_outlet_in_terminal_bbox(case, &first, &metrics.terminal_bbox);

    for run_index in 2..=STABILITY_RUNS {
        let current = capture_merit_case(case, identity);
        assert_oracle_case_matches(case, run_index, &first, &current);
    }
    first
}

fn capture_merit_case(case: &OracleCase, identity: &RemoteInputIdentity) -> GoldenRecord {
    let session =
        DatasetSession::open(MERIT_URL).expect("merit-basins/0.1.0 remote session should open");
    let engine = Engine::builder(session)
        .with_raster_source(LocalTiffRasterSource)
        .build();
    let telemetry = RecordingSubscriber::default();
    let telemetry_state = telemetry.state.clone();
    let dispatch = tracing::Dispatch::new(telemetry);
    let result = tracing::dispatcher::with_default(&dispatch, || {
        engine
            .delineate(case.outlet, &case.options())
            .unwrap_or_else(|error| panic!("Oracle C {} should delineate: {error}", case.name))
    });
    assert!(
        matches!(result.refinement(), RefinementOutcome::Applied { .. }),
        "Oracle C {} expected Applied refinement, got {:?}",
        case.name,
        result.refinement()
    );
    assert_valid_public_merit_result(case, &result);
    let terminal_bbox = terminal_bbox(&engine, &result);
    assert_refined_outlet_within_bbox(case, result.refinement(), &terminal_bbox);
    let window_measurement = window_measurement(
        case,
        &terminal_bbox,
        &engine
            .http_stats()
            .expect("PYSHED_BENCH_NET=1 should enable http stats"),
        &recorded_stage_spans(&telemetry_state),
    );
    GoldenRecord::from_result_with_context(
        &result,
        case,
        OracleMetadata::merit_refined(identity.clone()),
        Some(window_measurement),
    )
}

fn rect_contains_rect(outer: &Rect<f64>, inner: &Rect<f64>) -> bool {
    inner.min().x >= outer.min().x
        && inner.max().x <= outer.max().x
        && inner.min().y >= outer.min().y
        && inner.max().y <= outer.max().y
}

fn assert_valid_public_merit_result(case: &OracleCase, result: &DelineationResult) {
    let area_km2 = result.area_km2().as_f64();
    assert!(
        area_km2.is_finite() && area_km2 > 0.0,
        "Oracle C {} geodesic area_km2 must be finite and positive, got {area_km2}",
        case.name
    );
    assert!(
        !result.geometry().0.is_empty(),
        "Oracle C {} final watershed geometry must be non-empty",
        case.name
    );
    assert!(
        result.geometry().bounding_rect().is_some(),
        "Oracle C {} final watershed geometry must have a bbox",
        case.name
    );
    canonical_wkb_multi_polygon(result.geometry()).unwrap_or_else(|error| {
        panic!("Oracle C {} geometry must canonicalize: {error}", case.name)
    });
}

fn assert_refined_outlet_in_terminal_bbox(
    case: &OracleCase,
    record: &GoldenRecord,
    terminal_bbox: &RectRecord,
) {
    let refined_outlet = record
        .refined_outlet
        .as_ref()
        .unwrap_or_else(|| panic!("Oracle C {} should record refined_outlet", case.name));
    assert!(
        refined_outlet.lon.is_finite() && refined_outlet.lat.is_finite(),
        "Oracle C {} refined_outlet must be finite: {:?}",
        case.name,
        refined_outlet
    );
    assert!(
        rect_record_contains_point(terminal_bbox, refined_outlet, COORDINATE_ABS_EPSILON),
        "Oracle C {} refined_outlet must lie within terminal bbox; outlet={:?}, bbox={:?}",
        case.name,
        refined_outlet,
        terminal_bbox
    );
}

fn assert_refined_outlet_within_bbox(
    case: &OracleCase,
    outcome: &RefinementOutcome,
    terminal_bbox: &RectRecord,
) {
    let RefinementOutcome::Applied { refined_outlet } = outcome else {
        panic!(
            "Oracle C {} expected Applied refinement, got {:?}",
            case.name, outcome
        );
    };
    let refined_outlet = Outlet::from(*refined_outlet);
    assert!(
        refined_outlet.lon.is_finite() && refined_outlet.lat.is_finite(),
        "Oracle C {} refined_outlet must be finite: {:?}",
        case.name,
        refined_outlet
    );
    assert!(
        rect_record_contains_point(terminal_bbox, &refined_outlet, COORDINATE_ABS_EPSILON),
        "Oracle C {} refined_outlet must lie within terminal bbox; outlet={:?}, bbox={:?}",
        case.name,
        refined_outlet,
        terminal_bbox
    );
}

fn rect_record_contains_point(rect: &RectRecord, point: &Outlet, tolerance: f64) -> bool {
    point.lon >= rect.min_x - tolerance
        && point.lon <= rect.max_x + tolerance
        && point.lat >= rect.min_y - tolerance
        && point.lat <= rect.max_y + tolerance
}

fn delineate_synthetic_refined() -> DelineationResult {
    let session =
        DatasetSession::open_path(&fixture_root()).expect("v0.1 synthetic refined fixture opens");
    let engine = Engine::builder(session)
        .with_raster_source(LocalTiffRasterSource)
        .build();

    engine
        .delineate(SYNTHETIC_OUTLET, &synthetic_options())
        .expect("synthetic refined fixture should delineate")
}

fn synthetic_options() -> DelineationOptions {
    DelineationOptions::default().with_snap_threshold(SnapThreshold::new(500))
}

fn capture_synthetic_refined() -> GoldenRecord {
    GoldenRecord::from_result(
        &delineate_synthetic_refined(),
        FixtureProvenance::not_read(),
    )
}

fn capture_synthetic_refined_for_bless() -> GoldenRecord {
    GoldenRecord::from_result(
        &delineate_synthetic_refined(),
        FixtureProvenance::read_from_fixture(),
    )
}

fn read_golden_record() -> GoldenRecord {
    serde_json::from_str(
        &fs::read_to_string(golden_path()).expect("B golden should be committed and readable"),
    )
    .expect("B golden should match the golden schema")
}

fn assert_golden_matches_current(golden: &GoldenRecord, current: &GoldenRecord) {
    assert!(
        golden.canonical_wkb_hex == current.canonical_wkb_hex,
        "canonical WKB changed: expected_len={}, actual_len={}, expected_fingerprint={}, actual_fingerprint={}",
        golden.canonical_wkb_hex.len(),
        current.canonical_wkb_hex.len(),
        string_fingerprint(&golden.canonical_wkb_hex),
        string_fingerprint(&current.canonical_wkb_hex)
    );
    assert_close(
        "area_km2",
        golden.area_km2,
        current.area_km2,
        golden.comparison_policy.area_km2_abs_epsilon,
        golden.comparison_policy.area_km2_rel_epsilon,
    );
    assert_outlet_close(
        "input_outlet",
        &golden.input_outlet,
        &current.input_outlet,
        golden,
    );
    assert_outlet_close(
        "resolved_outlet",
        &golden.resolved_outlet,
        &current.resolved_outlet,
        golden,
    );
    assert_eq!(
        golden.refined_outlet.is_some(),
        current.refined_outlet.is_some()
    );
    if let (Some(golden_refined), Some(current_refined)) =
        (&golden.refined_outlet, &current.refined_outlet)
    {
        assert_outlet_close("refined_outlet", golden_refined, current_refined, golden);
    }
    assert_eq!(golden.terminal_id, current.terminal_id);
    assert_eq!(golden.upstream_ids, current.upstream_ids);
    assert_eq!(golden.resolution_method, current.resolution_method);
    assert_eq!(golden.resolver_config, current.resolver_config);
    assert_eq!(golden.refinement_outcome, current.refinement_outcome);
    assert_eq!(golden.canonicalizer_version, current.canonicalizer_version);
}

fn assert_oracle_case_matches(
    case: &OracleCase,
    run_index: usize,
    expected: &GoldenRecord,
    actual: &GoldenRecord,
) {
    if let Err(payload) =
        std::panic::catch_unwind(|| assert_golden_matches_current(expected, actual))
    {
        panic!(
            "{} stability run {run_index} failed: {}",
            case.name,
            panic_message(&payload)
        );
    }
}

fn string_fingerprint(value: &str) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn assert_outlet_close(name: &str, golden: &Outlet, current: &Outlet, record: &GoldenRecord) {
    let epsilon = record.comparison_policy.coordinate_abs_epsilon;
    assert_abs_close(&format!("{name}.lon"), golden.lon, current.lon, epsilon);
    assert_abs_close(&format!("{name}.lat"), golden.lat, current.lat, epsilon);
}

fn assert_close(name: &str, expected: f64, actual: f64, abs_epsilon: f64, rel_epsilon: f64) {
    let diff = (expected - actual).abs();
    let rel_allowed = expected.abs().max(actual.abs()) * rel_epsilon;
    assert!(
        diff <= abs_epsilon.max(rel_allowed),
        "{name} expected {expected}, got {actual}, diff {diff}"
    );
}

fn assert_abs_close(name: &str, expected: f64, actual: f64, epsilon: f64) {
    let diff = (expected - actual).abs();
    assert!(
        diff <= epsilon,
        "{name} expected {expected}, got {actual}, diff {diff}"
    );
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}

fn golden_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(GOLDEN_ROOT)
        .join(GOLDEN_FILE)
}

fn grit_golden_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(GRIT_GOLDEN_ROOT)
        .join(GRIT_GOLDEN_FILE)
}

fn merit_golden_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(MERIT_GOLDEN_ROOT)
        .join(MERIT_GOLDEN_FILE)
}

fn write_json<T: Serialize>(path: PathBuf, value: &T) {
    fs::create_dir_all(path.parent().expect("golden path should have a parent"))
        .expect("golden directory should be creatable");
    fs::write(
        path,
        serde_json::to_string_pretty(value).expect("golden should serialize") + "\n",
    )
    .expect("golden should be writable");
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct GoldenRecord {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    oracle: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    case_name: String,
    canonical_wkb_hex: String,
    area_km2: f64,
    input_outlet: Outlet,
    resolved_outlet: Outlet,
    refined_outlet: Option<Outlet>,
    terminal_id: i64,
    upstream_ids: Vec<i64>,
    resolution_method: String,
    resolver_config: ResolverConfigRecord,
    refinement_outcome: RefinementOutcomeRecord,
    canonicalizer_version: String,
    comparison_policy: ComparisonPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remote_input_identity: Option<RemoteInputIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    window_measurement: Option<WindowMeasurement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    raster_interpretation: Option<RasterInterpretation>,
    fixture_provenance: FixtureProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attestation: Option<Attestation>,
}

impl GoldenRecord {
    fn from_result(result: &DelineationResult, fixture_provenance: FixtureProvenance) -> Self {
        Self::from_result_inner(
            result,
            "",
            "",
            ResolverConfigRecord::from(ResolverConfig::new()),
            Some(RasterInterpretation::synthetic_refined()),
            fixture_provenance,
            Some(Attestation::synthetic_refined()),
            None,
            None,
        )
    }

    fn from_result_with_context(
        result: &DelineationResult,
        case: &OracleCase,
        metadata: OracleMetadata,
        window_measurement: Option<WindowMeasurement>,
    ) -> Self {
        Self::from_result_inner(
            result,
            metadata.oracle,
            case.name,
            ResolverConfigRecord {
                search_radius_m: case.search_radius_m,
            },
            metadata.raster_interpretation,
            FixtureProvenance::not_read(),
            metadata.attestation,
            Some(metadata.remote_input_identity),
            window_measurement,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_result_inner(
        result: &DelineationResult,
        oracle: &str,
        case_name: &str,
        resolver_config: ResolverConfigRecord,
        raster_interpretation: Option<RasterInterpretation>,
        fixture_provenance: FixtureProvenance,
        attestation: Option<Attestation>,
        remote_input_identity: Option<RemoteInputIdentity>,
        window_measurement: Option<WindowMeasurement>,
    ) -> Self {
        let mut upstream_ids = result
            .upstream_atom_ids()
            .iter()
            .map(|atom_id| i64::try_from(atom_id.get()).expect("atom id should fit in i64"))
            .collect::<Vec<_>>();
        upstream_ids.sort_unstable();
        upstream_ids.dedup();

        Self {
            oracle: oracle.to_string(),
            case_name: case_name.to_string(),
            canonical_wkb_hex: encode_hex(
                &canonical_wkb_multi_polygon(result.geometry())
                    .expect("engine geometry should canonicalize"),
            ),
            area_km2: result.area_km2().as_f64(),
            input_outlet: Outlet::from(result.input_outlet()),
            resolved_outlet: Outlet::from(result.resolved_outlet()),
            refined_outlet: refined_outlet(result.refinement()),
            terminal_id: i64::try_from(result.terminal_atom_id().get())
                .expect("terminal atom id should fit in i64"),
            upstream_ids,
            resolution_method: resolution_method_label(result.resolution_method()),
            resolver_config,
            refinement_outcome: RefinementOutcomeRecord::from(result.refinement()),
            canonicalizer_version: CANONICAL_WKB_VERSION.to_string(),
            comparison_policy: ComparisonPolicy::default(),
            remote_input_identity,
            window_measurement,
            raster_interpretation,
            fixture_provenance,
            attestation,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Outlet {
    lon: f64,
    lat: f64,
}

impl From<GeoCoord> for Outlet {
    fn from(coord: GeoCoord) -> Self {
        Self {
            lon: coord.lon,
            lat: coord.lat,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ResolverConfigRecord {
    search_radius_m: f64,
}

impl From<ResolverConfig> for ResolverConfigRecord {
    fn from(config: ResolverConfig) -> Self {
        Self {
            search_radius_m: config.search_radius().as_f64(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct RefinementOutcomeRecord {
    status: String,
    reason: Option<String>,
}

impl From<&RefinementOutcome> for RefinementOutcomeRecord {
    fn from(outcome: &RefinementOutcome) -> Self {
        match outcome {
            RefinementOutcome::Applied { .. } => Self {
                status: "Applied".to_string(),
                reason: None,
            },
            RefinementOutcome::NoRastersAvailable => Self {
                status: "NotApplied".to_string(),
                reason: Some("no rasters available".to_string()),
            },
            RefinementOutcome::NoRasterSourceProvided => Self {
                status: "NotApplied".to_string(),
                reason: Some("no raster source provided".to_string()),
            },
            RefinementOutcome::Disabled => Self {
                status: "NotApplied".to_string(),
                reason: Some("refinement disabled".to_string()),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ComparisonPolicy {
    coordinate_abs_epsilon: f64,
    area_km2_abs_epsilon: f64,
    area_km2_rel_epsilon: f64,
}

impl Default for ComparisonPolicy {
    fn default() -> Self {
        Self {
            coordinate_abs_epsilon: COORDINATE_ABS_EPSILON,
            area_km2_abs_epsilon: AREA_KM2_ABS_EPSILON,
            area_km2_rel_epsilon: AREA_KM2_REL_EPSILON,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
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

impl RasterInterpretation {
    fn synthetic_refined() -> Self {
        Self {
            dimensions: RasterDimensions {
                columns: 5,
                rows: 5,
            },
            crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 0.0, 0.0, -1.0],
            origin: "upper-left PixelIsArea corner (0, 0)".to_string(),
            pixel_size_degrees: PixelSize { x: 1.0, y: -1.0 },
            extent: RasterExtent {
                x_min: 0.0,
                x_max: 5.0,
                y_min: -5.0,
                y_max: 0.0,
            },
            pixel_interpretation:
                "GeoTIFF GTRasterTypeGeoKey=PixelIsArea; refinement uses pixel centers".to_string(),
            flow_direction: RasterBandInterpretation {
                sample_type: "uint8".to_string(),
                encoding: "ESRI D8".to_string(),
                nodata: "255".to_string(),
            },
            flow_accumulation: RasterBandInterpretation {
                sample_type: "float32".to_string(),
                encoding: "accumulation".to_string(),
                nodata: "-1 decoded as NaN".to_string(),
            },
        }
    }

    fn merit_refined() -> Self {
        Self {
            dimensions: RasterDimensions {
                columns: 1_296_000,
                rows: 432_000,
            },
            crs: "EPSG:4326".to_string(),
            transform: [-180.0, 1.0 / 3600.0, 0.0, 60.0, 0.0, -1.0 / 3600.0],
            origin: "remote COG localized to a plain north-up EPSG:4326 GeoTIFF window"
                .to_string(),
            pixel_size_degrees: PixelSize {
                x: 1.0 / 3600.0,
                y: -1.0 / 3600.0,
            },
            extent: RasterExtent {
                x_min: -180.0,
                x_max: 180.0,
                y_min: -60.0,
                y_max: 60.0,
            },
            pixel_interpretation:
                "Remote COG; localized plain north-up EPSG:4326 GeoTIFF window; PixelIsArea; refinement uses pixel centers".to_string(),
            flow_direction: RasterBandInterpretation {
                sample_type: "uint8".to_string(),
                encoding: "ESRI D8".to_string(),
                nodata: "255".to_string(),
            },
            flow_accumulation: RasterBandInterpretation {
                sample_type: "float32".to_string(),
                encoding: "accumulation".to_string(),
                nodata: "source nodata decoded as NaN".to_string(),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct RasterDimensions {
    columns: usize,
    rows: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct PixelSize {
    x: f64,
    y: f64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct RasterExtent {
    x_min: f64,
    x_max: f64,
    y_min: f64,
    y_max: f64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct RasterBandInterpretation {
    sample_type: String,
    encoding: String,
    nodata: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct FixtureProvenance {
    content_hash_algorithm: String,
    files: Vec<FileProvenance>,
}

impl FixtureProvenance {
    fn not_read() -> Self {
        Self {
            content_hash_algorithm: "sha256".to_string(),
            files: Vec::new(),
        }
    }

    fn read_from_fixture() -> Self {
        Self {
            content_hash_algorithm: "sha256".to_string(),
            files: [
                "manifest.json",
                "catchments.parquet",
                "graph.arrow",
                "flow_dir.tif",
                "flow_acc.tif",
            ]
            .iter()
            .map(|name| FileProvenance::read(name))
            .collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct FileProvenance {
    path: String,
    size_bytes: u64,
    sha256: String,
}

impl FileProvenance {
    fn read(name: &str) -> Self {
        let path = fixture_root().join(name);
        Self {
            path: name.to_string(),
            size_bytes: fs::metadata(&path)
                .unwrap_or_else(|error| panic!("fixture file {name} should have metadata: {error}"))
                .len(),
            sha256: sha256_file(&path),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Attestation {
    local_tiff_raster_source_gdal_tile_parity: String,
    proof_command: String,
}

impl Attestation {
    fn synthetic_refined() -> Self {
        Self {
            local_tiff_raster_source_gdal_tile_parity:
                "Step 2 proved LocalTiffRasterSource tile-identical to GdalRasterSource for the B fixture window before this golden was blessed".to_string(),
            proof_command:
                "cargo test -p shed-gdal --test raster_decode_parity synthetic_b_tiff_matches_gdal -- --ignored --nocapture".to_string(),
        }
    }

    fn merit_refined() -> Self {
        Self {
            local_tiff_raster_source_gdal_tile_parity:
                "rhine_basel localized windows were materialized by capture delineate() and proven tile-identical through LocalTiffRasterSource and GdalRasterSource, including tile geotransforms, sample values, nodata handling, and direct terminal-carve output before this golden was blessed".to_string(),
            proof_command:
                "SHED_PARITY_R2_CAPTURE=1 cargo test -p shed-gdal --test raster_decode_parity merit_c_windows_tiff_match_gdal -- --ignored --nocapture".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RemoteInputIdentity {
    pinned_url: String,
    artifacts: Vec<RemoteArtifactIdentity>,
}

impl RemoteInputIdentity {
    fn read(base_url: &str, artifacts: &[&str]) -> Self {
        Self {
            pinned_url: base_url.to_string(),
            artifacts: artifacts
                .iter()
                .map(|artifact| RemoteArtifactIdentity::head(base_url, artifact))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RemoteArtifactIdentity {
    path: String,
    etag: String,
    content_length: u64,
}

impl RemoteArtifactIdentity {
    fn head(base_url: &str, artifact: &str) -> Self {
        let url = format!("{base_url}{artifact}");
        let output = Command::new("curl")
            .args(["-fsSI", &url])
            .output()
            .unwrap_or_else(|error| panic!("curl HEAD should run for {url}: {error}"));
        assert!(
            output.status.success(),
            "curl HEAD failed for {url}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let headers = String::from_utf8(output.stdout).expect("curl HEAD output should be utf8");
        let etag = header_value(&headers, "etag")
            .unwrap_or_else(|| panic!("HEAD {url} should include ETag"));
        let content_length = header_value(&headers, "content-length")
            .unwrap_or_else(|| panic!("HEAD {url} should include Content-Length"))
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("HEAD Content-Length for {url} should parse: {error}"));
        Self {
            path: artifact.to_string(),
            etag,
            content_length,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct WindowMeasurement {
    terminal_bbox: RectRecord,
    search_radius_m: f64,
    flow_dir: RasterWindowStats,
    flow_acc: RasterWindowStats,
    http_total_bytes_in: u64,
    windowing_ceiling_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RasterWindowStats {
    tile_count: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RectRecord {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

struct OracleMetadata {
    oracle: &'static str,
    remote_input_identity: RemoteInputIdentity,
    raster_interpretation: Option<RasterInterpretation>,
    attestation: Option<Attestation>,
}

impl OracleMetadata {
    fn grit_nonrefined(remote_input_identity: RemoteInputIdentity) -> Self {
        Self {
            oracle: "A",
            remote_input_identity,
            raster_interpretation: None,
            attestation: None,
        }
    }

    fn merit_refined(remote_input_identity: RemoteInputIdentity) -> Self {
        Self {
            oracle: "C",
            remote_input_identity,
            raster_interpretation: Some(RasterInterpretation::merit_refined()),
            attestation: Some(Attestation::merit_refined()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OracleCase {
    name: &'static str,
    outlet: GeoCoord,
    search_radius_m: f64,
}

impl OracleCase {
    fn new(name: &'static str, outlet: GeoCoord, search_radius_m: f64) -> Self {
        Self {
            name,
            outlet,
            search_radius_m,
        }
    }

    fn options(&self) -> DelineationOptions {
        let search_radius = SearchRadiusMetres::new(self.search_radius_m)
            .expect("oracle search radius should be positive");
        DelineationOptions::default()
            .with_resolver_config(ResolverConfig::new().with_search_radius(search_radius))
    }
}

fn terminal_atom(session_url: &str, result: &DelineationResult) -> hfx_core::CatchmentAtom {
    let session =
        DatasetSession::open(session_url).expect("remote session should reopen for terminal query");
    let terminal = AtomId::new(result.terminal_atom_id().get())
        .expect("result terminal atom id should be valid");
    session
        .catchments()
        .query_by_ids(&[terminal])
        .expect("terminal catchment should query by id")
        .into_iter()
        .next()
        .expect("terminal catchment should exist")
}

fn terminal_bbox(_engine: &Engine, result: &DelineationResult) -> RectRecord {
    let atom = terminal_atom(MERIT_URL, result);
    let bbox = atom.bbox();
    RectRecord {
        min_x: f64::from(bbox.min_x().get()),
        min_y: f64::from(bbox.min_y().get()),
        max_x: f64::from(bbox.max_x().get()),
        max_y: f64::from(bbox.max_y().get()),
    }
}

fn window_measurement(
    case: &OracleCase,
    terminal_bbox: &RectRecord,
    stats: &shed_core::source_telemetry::HttpStatsSnapshot,
    spans: &[RecordedSpan],
) -> WindowMeasurement {
    WindowMeasurement {
        terminal_bbox: terminal_bbox.clone(),
        search_radius_m: case.search_radius_m,
        flow_dir: raster_window_stats(spans, "raster_localize_flow_dir"),
        flow_acc: raster_window_stats(spans, "raster_localize_flow_acc"),
        http_total_bytes_in: stats.total_bytes_in,
        windowing_ceiling_bytes: WINDOWING_CEILING_BYTES,
    }
}

fn raster_window_stats(spans: &[RecordedSpan], stage: &str) -> RasterWindowStats {
    let span = spans
        .iter()
        .rev()
        .find(|span| span.fields.get("stage").map(String::as_str) == Some(stage))
        .unwrap_or_else(|| panic!("telemetry should include {stage}"));
    RasterWindowStats {
        tile_count: span_u64(span, "requests"),
        bytes: span_u64(span, "bytes"),
    }
}

fn span_u64(span: &RecordedSpan, field: &str) -> u64 {
    span.fields
        .get(field)
        .unwrap_or_else(|| panic!("span should record {field}"))
        .parse::<u64>()
        .unwrap_or_else(|error| panic!("span {field} should parse as u64: {error}"))
}

fn header_value(headers: &str, name: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.eq_ignore_ascii_case(name) {
            Some(value.trim().to_string())
        } else {
            None
        }
    })
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else {
        "non-string panic payload".to_string()
    }
}

#[derive(Debug, Clone)]
struct RecordedSpan {
    metadata: &'static Metadata<'static>,
    fields: std::collections::HashMap<String, String>,
}

#[derive(Debug, Default)]
struct RecordingState {
    next_id: u64,
    spans: std::collections::HashMap<u64, RecordedSpan>,
    stack: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
struct RecordingSubscriber {
    state: Arc<Mutex<RecordingState>>,
}

struct FieldVisitor<'a> {
    fields: &'a mut std::collections::HashMap<String, String>,
}

impl Visit for FieldVisitor<'_> {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl Subscriber for RecordingSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        let mut state = self.state.lock().expect("recording state should lock");
        state.next_id += 1;
        let id = state.next_id;
        let mut recorded = RecordedSpan {
            metadata: span.metadata(),
            fields: std::collections::HashMap::new(),
        };
        span.record(&mut FieldVisitor {
            fields: &mut recorded.fields,
        });
        state.spans.insert(id, recorded);
        Id::from_u64(id)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        let mut state = self.state.lock().expect("recording state should lock");
        let recorded = state
            .spans
            .get_mut(&span.into_u64())
            .expect("span should have been created before recording");
        values.record(&mut FieldVisitor {
            fields: &mut recorded.fields,
        });
    }

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        let mut state = self.state.lock().expect("recording state should lock");
        state.stack.push(span.into_u64());
    }

    fn exit(&self, span: &Id) {
        let mut state = self.state.lock().expect("recording state should lock");
        let popped = state.stack.pop();
        assert_eq!(popped, Some(span.into_u64()));
    }

    fn current_span(&self) -> Current {
        let state = self.state.lock().expect("recording state should lock");
        state
            .stack
            .last()
            .and_then(|id| {
                state
                    .spans
                    .get(id)
                    .map(|span| Current::new(Id::from_u64(*id), span.metadata))
            })
            .unwrap_or_else(Current::none)
    }
}

fn recorded_stage_spans(state: &Arc<Mutex<RecordingState>>) -> Vec<RecordedSpan> {
    let state = state.lock().expect("recording state should lock");
    state
        .spans
        .values()
        .filter(|span| span.fields.contains_key("stage"))
        .cloned()
        .collect()
}

fn refined_outlet(outcome: &RefinementOutcome) -> Option<Outlet> {
    match outcome {
        RefinementOutcome::Applied { refined_outlet } => Some(Outlet::from(*refined_outlet)),
        RefinementOutcome::NoRastersAvailable
        | RefinementOutcome::NoRasterSourceProvided
        | RefinementOutcome::Disabled => None,
    }
}

fn resolution_method_label(method: &ResolutionMethod) -> String {
    match method {
        ResolutionMethod::PointInPolygon {
            candidates_considered,
            tie_break,
        } => format!(
            "point-in-polygon(candidates_considered={candidates_considered},tie_break={})",
            pip_tie_break_label(tie_break.as_ref())
        ),
        ResolutionMethod::Snap {
            strategy,
            snap_id,
            distance_m,
            weight,
            mainstem_status,
            candidates_considered,
        } => format!(
            "snap(strategy={},snap_id={},distance_m={distance_m},weight={},mainstem_status={mainstem_status:?},candidates_considered={candidates_considered})",
            snap_strategy_label(*strategy),
            snap_id.get(),
            weight.get()
        ),
    }
}

fn pip_tie_break_label(tie_break: Option<&PipTieBreak>) -> String {
    match tie_break {
        Some(PipTieBreak::HighestUpstreamArea) => "highest-upstream-area".to_string(),
        Some(PipTieBreak::HighestLocalArea) => "highest-local-area".to_string(),
        Some(PipTieBreak::LowestAtomId) => "lowest-atom-id".to_string(),
        None => "none".to_string(),
    }
}

fn snap_strategy_label(strategy: SnapStrategy) -> &'static str {
    match strategy {
        SnapStrategy::DistanceFirst => "distance-first",
        SnapStrategy::WeightFirst => "weight-first",
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        hex.push(DIGITS[(byte >> 4) as usize] as char);
        hex.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    hex
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
