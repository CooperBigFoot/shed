# Basin GeoParquet Export

## Status

This document defines the `shed` basin GeoParquet export format. It is a documented shed format, not a versioned spec. It is intended to be precise enough for code that reads shed outputs, while leaving external conformance rules, compatibility negotiation, and validator fixtures for a later versioned open spec if another producer appears.

## Purpose

The export supports the "delineate once, extract many" workflow: produce one dataset-level basin outlines file that downstream HDX-style pipelines can query by basin identity and source-fabric delineation.

Each row represents one `(basin_id, delineation)` pair. Reusing a `basin_id` with different `delineation` labels is valid and represents the same caller-owned basin catalog entry delineated from different fabric data or methods.

## Required Schema

| Column | Arrow type | Nullable | Description |
|---|---|---:|---|
| `basin_id` | `Utf8` | No | Caller-owned basin identity, unique per physical basin within a run and safe for HDX `basin=<id>` path segments. |
| `delineation` | `Utf8` | No | Source fabric data and method label, formatted by default as `{fabric_name}/{fabric_version}/{method}`. |
| `geometry` | `Binary` | No | OGC WKB `MultiPolygon`, 2D, EPSG:4326. |
| `outlet_lon` | `Float64` | No | Resolved outlet longitude in EPSG:4326. |
| `outlet_lat` | `Float64` | No | Resolved outlet latitude in EPSG:4326. |
| `area_km2` | `Float64` | No | Final geodesic drainage area from the dissolved watershed. |
| `bbox_minx` | `Float32` | No | Geometry bounding box west, rounded outward. |
| `bbox_miny` | `Float32` | No | Geometry bounding box south, rounded outward. |
| `bbox_maxx` | `Float32` | No | Geometry bounding box east, rounded outward. |
| `bbox_maxy` | `Float32` | No | Geometry bounding box north, rounded outward. |

The file must not store `hilbert_index`; Hilbert ordering is only an export sort key.

## Optional Provenance

The default documented profile includes these nullable provenance columns:

| Column | Arrow type | Nullable | Description |
|---|---|---:|---|
| `resolution_method` | `Utf8` | Yes | Exported display/debug form of outlet resolution. |
| `refinement_status` | `Utf8` | Yes | One of `applied`, `best_effort_skipped`, or `disabled`. |
| `upstream_unit_ids` | `List<Int64>` | Yes | Dataset-local HFX unit IDs included in traversal, including the terminal. |
| `adapter_version` | `Utf8` | Yes | Adapter/tooling version from the HFX manifest. This is provenance only. |

A minimal profile may omit all optional provenance columns.

## Geometry And CRS

`geometry` is 2D OGC WKB `MultiPolygon` in EPSG:4326. The Parquet file must carry GeoParquet `geo` footer metadata in file-level key-value metadata, not only Arrow schema metadata.

The `geo` JSON declares:

- `version`: `1.1.0`
- `primary_column`: `geometry`
- `columns.geometry.encoding`: `WKB`
- `columns.geometry.geometry_types`: `["MultiPolygon"]`
- `columns.geometry.crs`: EPSG:4326 PROJJSON with `id.authority = "EPSG"` and `id.code = 4326`
- `columns.geometry.bbox`: dataset-level `[minx, miny, maxx, maxy]` as f64 values covering all row geometries

The export omits `orientation`. It also does not declare GeoParquet `covering.bbox`; the flat bbox columns are ordinary attributes with Parquet statistics.

## Basin Identity

`basin_id` is parsed at the boundary with an allowlist:

- bytes length: 1 through 128
- regex: `^[A-Za-z0-9._-]+$`
- reject `.` and `..`
- reject Windows device names case-insensitively: `CON`, `PRN`, `AUX`, `NUL`, `COM1` through `COM9`, and `LPT1` through `LPT9`
- reject trailing `.` or trailing space
- reject `=` by the regex, because HDX uses `basin=<id>` path segments

The caller owns `basin_id` assignment. A narrow single-fabric default may use `DelineationResult::terminal_unit_id()` formatted as decimal text, but negative terminal unit IDs are rejected instead of emitting a leading `-`.

Duplicate `(basin_id, delineation)` rows are an error. If default terminal-unit IDs collide, the writer must report a specific default-ID collision that names both originating outlets and asks the caller to supply explicit `basin_id` values.

## Delineation Label

`delineation` identifies source fabric data and export method. The default label is:

```text
{fabric_name}/{fabric_version}/{method}
```

The `fabric_version` component comes from `Manifest::fabric_version()`, not `adapter_version`. If `fabric_version()` is absent, the default label constructor errors and the caller must provide an explicit `DelineationLabel` or explicit fabric data version.

`adapter_version` may appear in the optional provenance column, but it never drives `delineation`. The label is a column value, not a filesystem path segment, so it is not validated with the `basin_id` path allowlist.

## Bbox Columns

Per-row bbox columns are computed from the dissolved `MultiPolygon<f64>` and converted to `Float32` with outward rounding:

- `bbox_minx` and `bbox_miny` round toward negative infinity at f32 precision
- `bbox_maxx` and `bbox_maxy` round toward positive infinity at f32 precision

The dataset-level GeoParquet bbox remains f64 and must cover the true row bboxes.

## Hilbert Ordering

HFX does not define Hilbert curve parameters for this export. Shed therefore owns these deterministic parameters:

- fixed global EPSG:4326 extent: `[-180.0, -90.0, 180.0, 90.0]`
- input point: centroid of the dissolved `MultiPolygon`
- bit depth: 16 bits per axis
- axis mapping: longitude maps to x, latitude maps to y
- normalization: clamp finite lon/lat to the global extent before quantization
- quantization: map minimum extent to `0`, maximum extent to `(2^16 - 1)`
- sort key: `HilbertIndex(u32)`
- tie-breaks: `(hilbert_index ASC, basin_id ASC, delineation ASC)`

Because ordering uses a fixed global extent, adding unrelated rows does not renormalize existing row keys.

## Row Groups

For fewer than 4,096 rows, write exactly one row group. For 4,096 or more rows:

- compute `group_count = ceil(row_count / 8192)`
- distribute rows evenly across groups
- give the first `remainder` groups one extra row
- ensure every group, including the final group, has 4,096 through 8,192 rows

Writers must enable bbox column statistics so row-group pruning can use the outward-rounded bbox attributes.

## Minimal Example

| basin_id | delineation | geometry | outlet_lon | outlet_lat | area_km2 | bbox_minx | bbox_miny | bbox_maxx | bbox_maxy | adapter_version |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `rhine-basel` | `grit/2.0.0/d8-best-effort` | WKB MultiPolygon | 7.589 | 47.5596 | 365000.0 | 5.80 | 45.80 | 10.60 | 48.90 | `hfx-grit-adapter/1.4.0` |
| `rhine-basel` | `merit/2024.1/d8-carved` | WKB MultiPolygon | 7.589 | 47.5596 | 364200.0 | 5.81 | 45.81 | 10.58 | 48.88 | `hfx-merit-adapter/0.9.0` |

The repeated `basin_id` is valid because the `delineation` values are distinct.

## Reader Smoke

The committed fixture at `crates/core/tests/fixtures/export/basin-geoparquet-golden.parquet` was smoke-loaded with GeoPandas and PyOGRIO available in the local Python environment:

```bash
python3 - <<'PY'
import geopandas as gpd
frame = gpd.read_parquet("crates/core/tests/fixtures/export/basin-geoparquet-golden.parquet")
assert len(frame) == 3
assert frame.crs.to_epsg() == 4326
assert frame.geometry.geom_type.tolist() == ["MultiPolygon"] * 3
assert frame["basin_id"].tolist() == ["basin-center", "basin-west", "basin-east"]
PY
```

## CLI Emission

M5 does not add a `shed` CLI GeoParquet emit command. The current CLI has a `delineate` command with single-outlet flags and an `--outlets` CSV for GeoJSON output, but it does not define a basin export catalog shape. A CLI wrapper should be added only after that catalog contract is settled, so it can parse all `BasinId` values before delineation and call the core writer without inventing another input format.

## Elevation Path

If another producer or external conformer needs to target this format, elevate this document into a versioned open spec with compatibility rules, fixtures, and a conformance test suite.
