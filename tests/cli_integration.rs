use std::io::Write;

use assert_cmd::Command;
use serde_json::Value;
use shed_core::testutil::DatasetBuilder;
use tempfile::NamedTempFile;

// ── helpers ───────────────────────────────────────────────────────────────────

fn shed() -> Command {
    Command::cargo_bin("shed").unwrap()
}

// ── help flags ────────────────────────────────────────────────────────────────

#[test]
fn cli_help_succeeds() {
    let output = shed()
        .arg("--help")
        .output()
        .expect("failed to execute shed --help");

    assert!(
        output.status.success(),
        "exit code should be 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("shed"), "stdout should mention 'shed': {stdout}");
}

#[test]
fn cli_delineate_help_succeeds() {
    let output = shed()
        .args(["delineate", "--help"])
        .output()
        .expect("failed to execute shed delineate --help");

    assert!(
        output.status.success(),
        "exit code should be 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dataset"), "stdout should mention 'dataset': {stdout}");
}

// ── single-outlet GeoJSON ─────────────────────────────────────────────────────

#[test]
fn cli_single_outlet_geojson() {
    let (_dir, root) = DatasetBuilder::new(3).build();

    let output = shed()
        .args([
            "delineate",
            "--dataset",
            root.to_str().unwrap(),
            "--lat",
            "0.20",
            "--lon",
            "1.70",
        ])
        .output()
        .expect("failed to execute shed delineate");

    assert!(
        output.status.success(),
        "exit code should be 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout should be valid JSON: {e}\nstdout={stdout}"));
    assert_eq!(json["type"], "FeatureCollection");
    let features = json["features"].as_array().expect("features should be an array");
    assert_eq!(features.len(), 1, "expected 1 feature, got {}", features.len());
    assert_eq!(features[0]["geometry"]["type"], "MultiPolygon");
    assert!(
        features[0]["properties"]["area_km2"].as_f64().unwrap() > 0.0,
        "area_km2 should be positive"
    );
}

// ── invalid coordinate ────────────────────────────────────────────────────────

#[test]
fn cli_single_outlet_invalid_coord() {
    let (_dir, root) = DatasetBuilder::new(3).build();

    let output = shed()
        .args([
            "delineate",
            "--dataset",
            root.to_str().unwrap(),
            "--lat",
            "999.0",
            "--lon",
            "999.0",
        ])
        .output()
        .expect("failed to execute shed delineate");

    assert!(
        !output.status.success(),
        "exit code should be non-zero for an out-of-range coordinate"
    );
}

// ── batch CSV ─────────────────────────────────────────────────────────────────

#[test]
fn cli_batch_csv() {
    let (_dir, root) = DatasetBuilder::new(3).build();

    let mut csv_file = NamedTempFile::new().unwrap();
    writeln!(csv_file, "lat,lon").unwrap();
    writeln!(csv_file, "0.20,1.70").unwrap();
    writeln!(csv_file, "0.20,0.70").unwrap();
    csv_file.flush().unwrap();

    let output = shed()
        .args([
            "delineate",
            "--dataset",
            root.to_str().unwrap(),
            "--outlets",
            csv_file.path().to_str().unwrap(),
        ])
        .output()
        .expect("failed to execute shed delineate --outlets");

    assert!(
        output.status.success(),
        "exit code should be 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout should be valid JSON: {e}\nstdout={stdout}"));
    assert_eq!(json["type"], "FeatureCollection");
    let features = json["features"].as_array().expect("features should be an array");
    assert_eq!(features.len(), 2, "expected 2 features, got {}", features.len());
}

// ── missing dataset path ──────────────────────────────────────────────────────

#[test]
fn cli_missing_dataset() {
    let output = shed()
        .args([
            "delineate",
            "--dataset",
            "/tmp/nonexistent_shed_test_path",
            "--lat",
            "0.0",
            "--lon",
            "0.0",
        ])
        .output()
        .expect("failed to execute shed delineate");

    assert!(
        !output.status.success(),
        "exit code should be non-zero for a missing dataset"
    );
}

// ── --json envelope ───────────────────────────────────────────────────────────

#[test]
fn cli_json_envelope() {
    let (_dir, root) = DatasetBuilder::new(3).build();

    let output = shed()
        .args([
            "delineate",
            "--dataset",
            root.to_str().unwrap(),
            "--lat",
            "0.20",
            "--lon",
            "1.70",
            "--json",
        ])
        .output()
        .expect("failed to execute shed delineate --json");

    assert!(
        output.status.success(),
        "exit code should be 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout should be valid JSON: {e}\nstdout={stdout}"));
    let successes = json["successes"].as_array().expect("successes should be an array");
    assert_eq!(successes.len(), 1, "expected 1 success, got {}", successes.len());
    assert_eq!(json["failed"].as_u64().unwrap(), 0, "failed should be 0");
}

// ── --json with mixed batch results ──────────────────────────────────────────

#[test]
fn cli_json_batch_with_failure() {
    let (_dir, root) = DatasetBuilder::new(3).build();

    // CSV with one valid coordinate (inside atom 3) and one invalid coordinate
    // (far outside any catchment).
    let mut csv_file = NamedTempFile::new().unwrap();
    writeln!(csv_file, "lat,lon").unwrap();
    writeln!(csv_file, "0.20,1.70").unwrap(); // valid — inside atom 3
    writeln!(csv_file, "50.0,50.0").unwrap(); // valid coord but outside all catchments
    csv_file.flush().unwrap();

    let output = shed()
        .args([
            "delineate",
            "--dataset",
            root.to_str().unwrap(),
            "--outlets",
            csv_file.path().to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("failed to execute shed delineate --json");

    // Exit code must be non-zero because one outlet failed.
    assert!(
        !output.status.success(),
        "exit code should be non-zero when some outlets fail"
    );

    // stdout must be exactly one valid JSON document.
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout must be a single valid JSON document: {e}\nstdout={stdout}"));

    assert_eq!(
        json["succeeded"].as_u64().unwrap(),
        1,
        "expected 1 success, got {}",
        json["succeeded"]
    );
    assert_eq!(
        json["failed"].as_u64().unwrap(),
        1,
        "expected 1 failure, got {}",
        json["failed"]
    );
    assert_eq!(
        json["total"].as_u64().unwrap(),
        2,
        "expected total=2, got {}",
        json["total"]
    );

    // Verify the successes and failures arrays are present and correctly sized.
    let successes = json["successes"].as_array().expect("successes must be an array");
    assert_eq!(successes.len(), 1, "successes array must have 1 entry");
    let failures = json["failures"].as_array().expect("failures must be an array");
    assert_eq!(failures.len(), 1, "failures array must have 1 entry");

    // The failure entry must contain an error message.
    assert!(
        failures[0]["error"].as_str().is_some(),
        "failure entry must have an 'error' string field"
    );
}

// ── --no-refine flag ──────────────────────────────────────────────────────────

#[test]
fn cli_no_refine_flag() {
    let (_dir, root) = DatasetBuilder::new(3).build();

    let output = shed()
        .args([
            "delineate",
            "--dataset",
            root.to_str().unwrap(),
            "--lat",
            "0.20",
            "--lon",
            "1.70",
            "--no-refine",
        ])
        .output()
        .expect("failed to execute shed delineate --no-refine");

    assert!(
        output.status.success(),
        "exit code should be 0, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout should be valid JSON: {e}\nstdout={stdout}"));
    assert_eq!(json["type"], "FeatureCollection", "should emit a FeatureCollection");
    let features = json["features"].as_array().expect("features should be an array");
    assert!(!features.is_empty(), "expected at least one feature");
}
