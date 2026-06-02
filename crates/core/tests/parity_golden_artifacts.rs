//! Loader-independent validation for committed parity golden artifacts.

use std::fs;
use std::path::Path;

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
    assert_eq!(record.canonicalizer_version, CANONICAL_WKB_VERSION);
    assert_eq!(CANONICAL_WKB_DECIMAL_PRECISION, 6);
    assert!(record.area_km2.is_finite() && record.area_km2 >= 0.0);
    assert!(record.input_outlet.lon.is_finite());
    assert!(record.input_outlet.lat.is_finite());
    assert!(record.resolved_outlet.lon.is_finite());
    assert!(record.resolved_outlet.lat.is_finite());
    assert!(record.terminal_id >= 0);
    assert!(record.upstream_ids.windows(2).all(|ids| ids[0] < ids[1]));
    assert!(!record.upstream_ids.is_empty());
    assert!(!record.resolution_method.is_empty());
    assert!(record.resolver_config.search_radius_m.is_finite());
    assert!(record.resolver_config.search_radius_m >= 0.0);
    assert!(!record.refinement_outcome.status.is_empty());
    if record.refinement_outcome.status == "Applied" {
        assert!(record.refined_outlet.is_some());
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

fn hex_digit(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        other => panic!("invalid hex digit {other}"),
    }
}
