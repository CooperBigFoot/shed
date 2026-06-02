# M1 Step 4 C Raster Coordinate Investigation

## Verdict

This note is the historical diagnosis trail for the Step 4 C measurement bug.
It predates the final Step 4b attestation recorded in the committed C golden and
fixture README. The padded-vs-unpadded terminal-bbox tile comparison below is
not the final accepted GDAL-vs-TIFF proof.

The decisive GDAL-vs-LocalTiff run does **not** isolate the symptom to `LocalTiffRasterSource`: both sources produce the same final `Engine::delineate()` bbox for `rhine_basel`.

However, the premise of a raster coordinate-transform defect is false. The out-of-terminal bbox being measured is the **final assembled watershed geometry**, not the raster-refined terminal polygon. The actual direct terminal carve is contained inside the terminal bbox for both raster sources.

Scope implication: this is an M1 capture/test measurement bug, not a shared core raster/cache behavior bug and not an engine behavior change.

## Reproduction

Fresh cache:

```bash
SSL_CERT_FILE=/opt/homebrew/etc/ca-certificates/cert.pem \
AWS_CA_BUNDLE=/opt/homebrew/etc/ca-certificates/cert.pem \
HFX_CACHE_DIR=/private/tmp/shed-m1c-investigation-cache \
SHED_PARITY_R2_CAPTURE=1 \
PYSHED_BENCH_NET=1 \
cargo test -p shed-gdal --test raster_decode_parity \
  investigate_rhine_merit_window_coordinate_path -- --ignored --nocapture
```

Notes:
- The sandboxed run failed DNS resolution.
- The approved network run then needed explicit CA bundle env due `InvalidCertificate(UnknownIssuer)`.
- The diagnostic test was temporary and has been removed.

Measured `rhine_basel` final `Engine::delineate()` results:

| Source | Final assembled bbox | Final planar area |
|---|---:|---:|
| `LocalTiffRasterSource` | `(6.055416669814,46.325416669961) -> (10.230416666834,48.023750000414)` | `4.310016665044` |
| `GdalRasterSource` | `(6.055416669810,46.325416669950) -> (10.230416666830,48.023750000403)` | `4.310016666026` |

Terminal catchment:

```text
bbox=(7.567083358765,47.587917327881) -> (7.638750076294,47.625415802002)
area_km2=14.931484222412
```

Direct terminal-only raster refine on the same cached TIFFs:

| Source | Direct refined terminal bbox | Direct terminal planar area | Snapped outlet |
|---|---:|---:|---:|
| `LocalTiffRasterSource` | `(7.587916666667,47.587916666667) -> (7.589583333333,47.589583333333)` | `0.000002083333` | `(7.589166666667,47.589166666667)` |
| `GdalRasterSource` | same | same | same |

The direct terminal carve is inside the terminal bbox. The larger bbox is from upstream watershed assembly, not raster escape.

## Tile Comparison

Cached window files:

```text
/private/tmp/shed-m1c-investigation-cache/merit_basins/0.1.0/raster-windows/flow-dir.15027405356408281945.x225080-y44848-w88-h47.tif
/private/tmp/shed-m1c-investigation-cache/merit_basins/0.1.0/raster-windows/flow-acc.8886301043446394911.x225080-y44848-w88-h47.tif
```

Raw GDAL transform for both cached TIFFs:

```text
dims=47x88
gt=[7.566250000000, 0.000833333333333, 0, 47.626250000000, 0, -0.000833333333333]
```

Terminal-bbox tile reads are **not tile-identical**:

| Kind | Source | Dims | Geo | Checksum | First samples |
|---|---|---:|---|---:|---|
| flow_dir | LocalTiff | `47x88` | `(7.566250000000,47.626250000000,0.000833333333333,-0.000833333333333)` | `262958539` | `[16, 4, 128, 128, 1, 4, 16, 8, 8, 32, 128, 128, 64, 128, 64, 32]` |
| flow_dir | GDAL | `45x87` | `(7.567083333333,47.625416666667,0.000833333333333,-0.000833333333333)` | `235782626` | `[16, 16, 128, 1, 4, 16, 16, 16, 128, 128, 128, 64, 1, 128, 64, 32]` |
| flow_acc | LocalTiff | `47x88` | `(7.566250000000,47.626250000000,0.000833333333333,-0.000833333333333)` | `9253434606332816` | `[1.0, 1.0, 1.0, 1.0, 2.0, 27268.0, 6.0, 1.0, 1.0, 1.0, 2.0, 5.0, 3.0, 1.0, 1.0, 280.0]` |
| flow_acc | GDAL | `45x87` | `(7.567083333333,47.625416666667,0.000833333333333,-0.000833333333333)` | `8288807119381538` | `[5.0, 1.0, 1.0, 2.0, 27279.0, 5.0, 3.0, 1.0, 1.0, 4.0, 1.0, 1.0, 2.0, 6.0, 266.0, 2.0]` |

The divergence is expected from different bbox padding policy:
- `LocalTiffRasterSource` reuses `RasterPixelWindow::from_bbox`, which pads by one pixel in `crates/core/src/cog.rs:146`.
- `GdalRasterSource` uses unpadded `bbox_to_pixel_window` in `crates/gdal/src/raster_reader.rs:222`.

Despite this tile-window difference, direct terminal refine returns identical terminal-contained geometry for both sources.

## Root Cause

The wrong measurement is in [parity_v01_oracle_capture.rs](/Users/nicolaslazaro/Desktop/work/shed/crates/core/tests/parity_v01_oracle_capture.rs:300):

```rust
let refined_bbox = RectRecord::from_rect(
    &result.geometry().bounding_rect().expect("refined Oracle C geometry should have a bbox"),
);
```

`result.geometry()` is not the refined terminal polygon. It is the final watershed after:
- `Engine::try_refine()` returns `Some(refined_polygon)` only as the terminal override in [engine.rs](/Users/nicolaslazaro/Desktop/work/shed/crates/core/src/engine.rs:627).
- `Engine::delineate()` passes that override into `assemble_watershed()` in [engine.rs](/Users/nicolaslazaro/Desktop/work/shed/crates/core/src/engine.rs:416).
- `assemble_watershed()` fetches all upstream non-terminal catchments and inserts the refined terminal override in [assembly.rs](/Users/nicolaslazaro/Desktop/work/shed/crates/core/src/assembly.rs:159).

So comparing `result.geometry().bounding_rect()` to the terminal bbox in [parity_v01_oracle_capture.rs](/Users/nicolaslazaro/Desktop/work/shed/crates/core/tests/parity_v01_oracle_capture.rs:259) asserts that the entire upstream watershed is inside its outlet catchment, which is not a valid invariant.

There is also a unit mixup in [parity_v01_oracle_capture.rs](/Users/nicolaslazaro/Desktop/work/shed/crates/core/tests/parity_v01_oracle_capture.rs:314): `terminal_area()` is HFX km², while `result.geometry().unsigned_area()` is planar degree².

## Hypotheses

Refuted:
- Missing offset in cached TIFF: raw GDAL transform for the localized file is near the terminal window, not full-raster origin.
- Double offset in cached TIFF: direct terminal refine returns a contained bbox and correct snapped outlet.
- Pixel-height sign error: cached GDAL transform has negative y pixel size, and both readers report `-0.000833333333333`.

Confirmed issue:
- Capture code labels final assembled watershed bbox/area as refined terminal carve bbox/area.

## Recommended Fix

Fix location: `crates/core/tests/parity_v01_oracle_capture.rs`.

One-line fix: measure Oracle C containment/area on the terminal refinement result before watershed assembly, or rename/remove the containment assertion if it intentionally records final assembled watershed geometry.

This is in M1 scope as a capture/test correction. It should not require changing `cog.rs`, `session.rs`, `GdalRasterSource`, or raster cache materialization.

## Temporary Code

I temporarily added an ignored diagnostic test to `crates/gdal/tests/raster_decode_parity.rs` and removed it after capture.

Left in tree: no throwaway diagnostic code.
