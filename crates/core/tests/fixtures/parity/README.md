# Parity Golden Artifact Contract

Milestone 1 parity goldens are loader-independent JSON records. Geometry truth is
the `canonical_wkb_hex` field: little-endian 2D WKB emitted by
`shed-core::algo::canonical_wkb_multi_polygon`.

## Canonicalizer

- `canonicalizer_version`: `shed-canonical-wkb-v1`
- Coordinate precision: 6 decimal places (`CANONICAL_WKB_DECIMAL_PRECISION = 6`)
- Coordinate absolute epsilon: `0.000001`
- Ring closure: explicit first vertex repeated as last
- Ring orientation: exterior rings are CCW; interior rings are CW
- Ring start vertex: lexicographically smallest rounded `(x, y)`; duplicate
  rounded coordinates are tied by the full adjacent cyclic vertex sequence
- Hole order: normalized ring bbox, signed area, full rounded vertex sequence
- Polygon/component order: normalized exterior bbox, polygon area, hole count,
  full rounded exterior sequence, then full rounded hole sequences
- Antimeridian-crossing geometries are out of scope for M1 because the selected
  A/B/C outlets are far from +/-180 degrees.

The 6-decimal precision is intentionally coarser than normal f64 operation
noise. M1 goldens require pre-rounding coordinate divergence to remain below
`1e-9` degrees, giving at least a 500x margin below the `5e-7` degree half-step
where a rounded coordinate could flip. Changing this precision changes the
canonicalizer version and invalidates captured goldens.

## Golden Record Fields

- `canonical_wkb_hex`: hex-encoded canonical final geometry WKB
- `area_km2`: scalar area compared with epsilon policy, not byte-exact equality
- `input_outlet`: original outlet coordinate
- `resolved_outlet`: resolved outlet coordinate
- `refined_outlet`: refined outlet coordinate, present only when refinement
  outcome is `Applied`
- `terminal_id`: version-neutral terminal identifier as `i64`
- `upstream_ids`: sorted version-neutral upstream identifier set as `Vec<i64>`
- `resolution_method`: outlet resolution method label
- `resolver_config`: resolver settings, including `search_radius_m`
- `refinement_outcome`: refinement status and optional reason
- `canonicalizer_version`: canonicalizer contract version
- `comparison_policy`: coordinate absolute epsilon plus `area_km2`
  absolute/relative epsilon tied to canonical WKB precision

## Commands

Offline comparison gate:

```bash
cargo build -p shed-core
cargo test -p shed-core --test parity_v01_oracle_capture
cargo test -p shed-core --test parity_golden_artifacts
```

Network-gated capture and refresh:

```bash
SHED_PARITY_R2_CAPTURE=1 cargo test -p shed-core --test parity_v01_oracle_capture -- --ignored --nocapture
```

Golden refresh is intentionally explicit. Do not regenerate or re-bless M1
goldens during offline comparison work.

## Synthetic Refined Raster Fixture

`v01_synthetic_refined/` is oracle B's committed v0.1 input fixture. It mirrors
the existing `simple_convergent_5x5` refinement geometry with real TIFF bytes.

- Dimensions: 5 columns x 5 rows for both `flow_dir.tif` and `flow_acc.tif`
- CRS: EPSG:4326
- Transform: north-up GDAL transform `[0, 1, 0, 0, 0, -1]`
- Origin: upper-left PixelIsArea corner `(0, 0)`
- Pixel size: `1 x -1` degrees
- Extent: `x=[0, 5]`, `y=[-5, 0]`
- Pixel interpretation: GeoTIFF `GTRasterTypeGeoKey=PixelIsArea`; shed uses
  pixel centers for raster refinement, so cell `(row=2, col=2)` is
  `(lon=2.5, lat=-2.5)`
- Flow direction samples: one-band unsigned 8-bit, ESRI D8 encoding, nodata
  tag `255`
- Flow accumulation samples: one-band 32-bit float, nodata tag `-1`, decoded
  by readers as `NaN`
- Carve contract: terminal catchment ID `1` is the rectangle
  `(0, -5, 5, 0)`, outlet `(2.5, -2.5)`, snap threshold `500`, and center
  accumulation `800`

M2 must not mutate or move this M1 B fixture in place. The v0.2.1 work creates a
separate fixture copy and reuses the exact same `flow_dir.tif` and
`flow_acc.tif` bytes. The durable artifact test re-hashes only those two TIFFs
at this committed M1 path so accidental byte drift fails offline after M2.

The B TIFFs are the deterministic, byte-identical M1-to-M4 parity path. For M4
real-data D8 parity, use `merit/0.2.0`; `merit-basins/0.1.0` is the M1
real-data v0.1 oracle C input, not the M4 v0.2.1 target.

GDAL parity proof command:

```bash
cargo test -p shed-gdal --test raster_decode_parity synthetic_b_tiff_matches_gdal -- --ignored --nocapture
```

M1 already proved TIFF-vs-GDAL tile identity for B and for the accepted C
`rhine_basel` windows. M4 may reuse the B proof for the byte-identical raster
bytes, or re-run the proof if the reader implementation changes.
