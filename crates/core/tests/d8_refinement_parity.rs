use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use shed_core::algo::SnapThreshold;
use shed_core::algo::canonical_wkb_multi_polygon;
use shed_core::algo::coord::GeoCoord;
use shed_core::session::DatasetSession;
use shed_core::test_raster_source::LocalTiffRasterSource;
use shed_core::{
    AppliedRefinementReason, DelineationOptions, Engine, LevelSelection, PreMergeDrainageUnit,
    PreMergeDrainageUnits, RefinementOutcome, RefinementProvenance, RefinementStrategyName,
    TerminalRefinement,
};

const PARITY_FIXTURE_DIR: &str = "tests/fixtures/parity";
const V021_SYNTHETIC_REFINED_DIR: &str = "v021_synthetic_refined";
const M1_SYNTHETIC_REFINED_GOLDEN: &str =
    "goldens/v01_synthetic_refined/oracle_b_synthetic_refined.json";

#[test]
fn v021_synthetic_d8_refinement_matches_m1_b_golden() {
    let golden = read_golden(M1_SYNTHETIC_REFINED_GOLDEN);
    let session = DatasetSession::open_path(&parity_fixture_path(V021_SYNTHETIC_REFINED_DIR))
        .expect("v0.2.1 converted parity fixture should open");
    let engine = Engine::builder(session)
        .with_raster_source(LocalTiffRasterSource)
        .build();
    let outlet = GeoCoord::new(golden.input_outlet.lon, golden.input_outlet.lat);
    let options = b_oracle_options();

    let result = engine
        .delineate(outlet, &options)
        .expect("D8-refined delineation should succeed");
    let canonical = canonical_wkb_multi_polygon(result.geometry())
        .expect("D8-refined geometry should canonicalize");

    assert_eq!(canonical, decode_hex(&golden.canonical_wkb_hex));
    assert_area_within_golden_policy(result.area_km2().as_f64(), golden.area_km2, &golden);
    assert_eq!(result.terminal_unit_id().get(), golden.terminal_id);
    assert_eq!(
        result
            .upstream_unit_ids()
            .iter()
            .map(|id| id.get())
            .collect::<Vec<_>>(),
        golden.upstream_ids
    );
    assert_eq!(
        result.refinement(),
        &RefinementOutcome::Applied {
            refined_outlet: GeoCoord::new(golden.refined_outlet.lon, golden.refined_outlet.lat),
            provenance: RefinementProvenance::Applied {
                strategy: RefinementStrategyName::BuiltInD8,
                why: AppliedRefinementReason::D8AuxMatchedTerminalBbox {
                    declaration_index: 0,
                },
            },
        }
    );
}

#[test]
fn applied_d8_carve_replaces_whole_terminal_in_final_dissolve() {
    let golden = read_golden(M1_SYNTHETIC_REFINED_GOLDEN);
    let session = DatasetSession::open_path(&parity_fixture_path(V021_SYNTHETIC_REFINED_DIR))
        .expect("v0.2.1 converted parity fixture should open");
    let engine = Engine::builder(session)
        .with_raster_source(LocalTiffRasterSource)
        .build();
    let outlet = GeoCoord::new(golden.input_outlet.lon, golden.input_outlet.lat);
    let options = b_oracle_options();

    let selected = engine
        .select_level(LevelSelection::Finest)
        .expect("finest level should resolve");
    let resolved = engine
        .resolve_outlet_at_level(outlet, selected, options.resolver_config())
        .expect("fixture outlet should resolve");
    let upstream = engine
        .traverse_upstream_at_level(&resolved)
        .expect("same-level traversal should succeed");
    let pre_merge = engine
        .produce_pre_merge_units(&upstream)
        .expect("pre-merge records should materialize");
    let whole_terminal = pre_merge
        .terminal_unit()
        .expect("terminal record should exist")
        .geometry();

    let refinement = engine
        .refine_terminal_placeholder(&resolved, &pre_merge, &options)
        .expect("D8 refinement should apply");
    let TerminalRefinement::Applied { geometry, .. } = &refinement else {
        panic!("expected applied D8 refinement, got {refinement:?}");
    };
    let dissolved = engine
        .dissolve_watershed(&pre_merge, &refinement, &options)
        .expect("applied D8 dissolve should succeed");
    let whole_terminal_dissolved = engine
        .dissolve_watershed(&pre_merge, &TerminalRefinement::Disabled, &options)
        .expect("whole-terminal dissolve should succeed");
    let replacement_pre_merge =
        pre_merge_with_terminal_geometry(&pre_merge, geometry.polygon().clone());
    let replacement_dissolved = engine
        .dissolve_watershed(
            &replacement_pre_merge,
            &TerminalRefinement::Disabled,
            &options,
        )
        .expect("carved-terminal replacement dissolve should succeed");

    let final_canonical = canonical_wkb_multi_polygon(dissolved.geometry())
        .expect("final geometry should canonicalize");
    let replacement_canonical = canonical_wkb_multi_polygon(replacement_dissolved.geometry())
        .expect("replacement geometry should canonicalize");
    let whole_terminal_dissolved_canonical =
        canonical_wkb_multi_polygon(whole_terminal_dissolved.geometry())
            .expect("whole-terminal dissolved geometry should canonicalize");
    let whole_terminal_canonical = canonical_wkb_multi_polygon(whole_terminal)
        .expect("whole terminal geometry should canonicalize");

    // R3: pre-merge unit records stay pristine. Their area/geometry may
    // intentionally disagree with final refined output; final geometry is
    // assembled only after excluding the whole terminal and inserting the carve.
    assert_ne!(final_canonical, whole_terminal_canonical);
    assert_ne!(final_canonical, whole_terminal_dissolved_canonical);
    assert_eq!(final_canonical, replacement_canonical);
    assert_eq!(final_canonical, decode_hex(&golden.canonical_wkb_hex));
}

fn parity_fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(PARITY_FIXTURE_DIR)
        .join(name)
}

fn read_golden(name: &str) -> GoldenRecord {
    let path = parity_fixture_path(name);
    serde_json::from_str(&fs::read_to_string(path).expect("golden should be readable"))
        .expect("golden should match test schema")
}

fn b_oracle_options() -> DelineationOptions {
    DelineationOptions::default().with_snap_threshold(SnapThreshold::new(500))
}

fn pre_merge_with_terminal_geometry(
    pre_merge: &PreMergeDrainageUnits,
    terminal_geometry: geo::MultiPolygon<f64>,
) -> PreMergeDrainageUnits {
    let units = pre_merge
        .units()
        .iter()
        .map(|unit| {
            let geometry = if unit.id() == pre_merge.terminal() {
                terminal_geometry.clone()
            } else {
                unit.geometry().clone()
            };
            PreMergeDrainageUnit::new_for_test(
                unit.id(),
                unit.level(),
                unit.area(),
                unit.up_area(),
                unit.outlet(),
                geometry,
            )
        })
        .collect();
    PreMergeDrainageUnits::new_for_test(pre_merge.terminal(), pre_merge.selected_level(), units)
}

fn assert_area_within_golden_policy(actual: f64, expected: f64, golden: &GoldenRecord) {
    let tolerance = golden
        .comparison_policy
        .area_km2_abs_epsilon
        .max(expected.abs() * golden.comparison_policy.area_km2_rel_epsilon);
    assert!(
        (actual - expected).abs() <= tolerance,
        "area {actual} differs from golden {expected} beyond tolerance {tolerance}"
    );
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (hex_digit(pair[0]) << 4) | hex_digit(pair[1]))
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

#[derive(Debug, Deserialize)]
struct GoldenRecord {
    canonical_wkb_hex: String,
    area_km2: f64,
    input_outlet: GoldenOutlet,
    refined_outlet: GoldenOutlet,
    terminal_id: i64,
    upstream_ids: Vec<i64>,
    comparison_policy: GoldenComparisonPolicy,
}

#[derive(Debug, Deserialize)]
struct GoldenOutlet {
    lon: f64,
    lat: f64,
}

#[derive(Debug, Deserialize)]
struct GoldenComparisonPolicy {
    area_km2_abs_epsilon: f64,
    area_km2_rel_epsilon: f64,
}
