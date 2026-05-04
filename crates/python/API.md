# pyshed API Reference

Developer-oriented reference for the supported public `pyshed` Python API.
This file mirrors the runtime surface re-exported from
[`python/pyshed/__init__.py`](python/pyshed/__init__.py) and the shipped
PEP 561 stub in [`python/pyshed/__init__.pyi`](python/pyshed/__init__.pyi).

## Public Exports

The `pyshed` package exports these names:

- `Engine`
- `DelineationResult`
- `AreaOnlyResult`
- `ShedError`
- `DatasetError`
- `ResolutionError`
- `AssemblyError`
- `set_log_level`
- `__version__`

`_pyshed` exists as a compiled implementation detail, but its helper functions
are not part of the supported public API.

## set_log_level

```python
set_log_level(level: str) -> None
```

Sets the active log level for both the Rust tracing bridge and the Python
`logging` tree.

| Parameter | Type | Meaning |
|---|---|---|
| `level` | `str` | Case-insensitive level name: `"trace"`, `"debug"`, `"info"`, `"warn"`/`"warning"`, or `"error"`/`"critical"` |

Records from Rust code route through pyo3-log under loggers named after their
crate (`_pyshed.*`, `shed_core.*`, `hfx_core.*`). If any relevant logger has no
handler, a `StreamHandler` is added to that logger automatically, so first-time
users see output without calling `logging.basicConfig`.

Set `PYSHED_LOG` to one of the same level names to opt in at import time.

## Engine

### Constructor

```python
Engine(
    dataset_path: str,
    *,
    snap_radius: float | None = None,
    snap_strategy: Literal["distance-first", "weight-first"] | None = None,
    snap_threshold: int | None = None,
    clean_epsilon: float | None = None,
    refine: bool = True,
    parquet_cache: bool = False,
    parquet_cache_max_mb: int = 2048,
) -> None
```

Opens an HFX dataset and constructs a delineation engine.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `dataset_path` | `str` | — | Path or URL to the HFX dataset root directory |
| `snap_radius` | `float \| None` | `None` | Snap-path search radius in metres; must be finite and positive when provided |
| `snap_strategy` | `"distance-first" \| "weight-first" \| None` | `None` | Snap ranking strategy. Defaults to `"weight-first"` (HFX v0.2 contract). |
| `snap_threshold` | `int \| None` | `None` | Minimum upstream-pixel count for stream-network snapping |
| `clean_epsilon` | `float \| None` | `None` | Topology-cleaning epsilon in degrees |
| `refine` | `bool` | `True` | Whether raster-based terminal refinement is enabled |
| `parquet_cache` | `bool` | `False` | Enable in-memory Parquet column-chunk cache for repeated delineations on the same dataset |
| `parquet_cache_max_mb` | `int` | `2048` | Maximum cache size in MiB; must be > 0 when `parquet_cache=True` |

### Exceptions

- `DatasetError` when the dataset cannot be opened or read.
- `ValueError` when a configuration argument is invalid, such as an unknown
  `snap_strategy`, a non-positive `snap_radius`, or `parquet_cache_max_mb=0`
  when `parquet_cache=True`.

### Methods

```python
delineate(
    *,
    lat: float,
    lon: float,
    geometry: bool = True,
) -> DelineationResult | AreaOnlyResult
```

Delineates the watershed upstream of a single outlet.

| Parameter | Type | Meaning |
|---|---|---|
| `lat` | `float` | Outlet latitude in decimal degrees (EPSG:4326) |
| `lon` | `float` | Outlet longitude in decimal degrees (EPSG:4326) |
| `geometry` | `bool` | When `True`, return a full `DelineationResult`; when `False`, return `AreaOnlyResult` scalar metadata without geometry accessors |

Type checkers see precise overloads:

```python
Engine.delineate(*, lat: float, lon: float) -> DelineationResult
Engine.delineate(*, lat: float, lon: float, geometry=True) -> DelineationResult
Engine.delineate(*, lat: float, lon: float, geometry=False) -> AreaOnlyResult
```

#### Exceptions

- `ValueError` when `lat` or `lon` is outside the valid geographic range.
- `ResolutionError` when the outlet cannot be resolved to a terminal catchment.
- `DatasetError` when underlying dataset reads fail during delineation.
- `AssemblyError` when watershed geometry assembly fails.
- `ShedError` for other engine failures such as traversal or refinement errors.

```python
delineate_batch(
    outlets: list[dict[str, float]],
    *,
    progress: Callable[[dict], None] | None = None,
) -> list[DelineationResult]
```

Delineates watersheds for a batch of outlets that share the same engine
configuration.

Each outlet must be a dict with exactly these keys:

```python
{"lat": 47.3769, "lon": 8.5417}
```

Results are returned in input order. The call raises on the first failure in
that order rather than returning per-outlet error objects.

When `progress` is supplied, the batch runs sequentially and the callback is
invoked once per outlet (after it completes) with an event dict:

| Key | Type | Present |
|---|---|---|
| `index` | `int` | always |
| `total` | `int` | always |
| `lat` | `float` | always |
| `lon` | `float` | always |
| `duration_ms` | `int` | always |
| `status` | `str` (`"ok"` or `"error"`) | always |
| `n_catchments` | `int` | success only |
| `error` | `str` | failure only |

Exceptions raised by the callback are swallowed and logged via `warn!`; they do
not interrupt the batch.

Without `progress`, the batch runs in parallel via Rayon.

#### Exceptions

- `KeyError` when an outlet dict is missing `"lat"` or `"lon"`.
- `ValueError` when any outlet contains invalid coordinates.
- The same typed `pyshed` exceptions as `delineate()` for engine failures.

## DelineationResult

Returned by `Engine.delineate()` and `Engine.delineate_batch()`.

### Properties

| Property | Type | Meaning |
|---|---|---|
| `terminal_atom_id` | `int` | Terminal HFX atom ID that the outlet resolved to |
| `input_outlet` | `tuple[float, float]` | Original outlet as `(lon, lat)` |
| `resolved_outlet` | `tuple[float, float]` | Outlet used for resolution as `(lon, lat)` |
| `refined_outlet` | `tuple[float, float] \| None` | Raster-refined outlet as `(lon, lat)`, or `None` if refinement was not applied |
| `resolution_method` | `str` | Debug/provenance string describing how outlet resolution happened |
| `upstream_atom_ids` | `list[int]` | Upstream atom IDs including the terminal atom |
| `area_km2` | `float` | Geodesic watershed area in square kilometres |
| `geometry_bbox` | `tuple[float, float, float, float] \| None` | Geometry bounds as `(minx, miny, maxx, maxy)`, or `None` for empty geometry |
| `geometry_wkb` | `bytes` | Watershed geometry encoded as OGC WKB bytes |

### Methods

```python
to_geojson() -> str
```

Serializes the result as a GeoJSON `Feature` string.

```python
__repr__() -> str
```

Returns a concise debug representation including the terminal atom ID, area,
and upstream atom count.

## AreaOnlyResult

Returned by `Engine.delineate(..., geometry=False)`.

This result exposes scalar metadata and area only. It intentionally does not
provide `geometry_wkb`, `geometry_bbox`, or `to_geojson()`.

### Properties

| Property | Type | Meaning |
|---|---|---|
| `terminal_atom_id` | `int` | Terminal HFX atom ID that the outlet resolved to |
| `input_outlet` | `tuple[float, float]` | Original outlet as `(lon, lat)` |
| `resolved_outlet` | `tuple[float, float]` | Outlet used for resolution as `(lon, lat)` |
| `refined_outlet` | `tuple[float, float] \| None` | Raster-refined outlet as `(lon, lat)`, or `None` if refinement was not applied |
| `resolution_method` | `str` | Debug/provenance string describing how outlet resolution happened |
| `upstream_atom_ids` | `list[int]` | Upstream atom IDs including the terminal atom |
| `area_km2` | `float` | Geodesic watershed area in square kilometres |

## Exceptions

```python
class ShedError(Exception): ...
class DatasetError(ShedError): ...
class ResolutionError(ShedError): ...
class AssemblyError(ShedError): ...
```

These typed exceptions are raised by the engine so callers can distinguish
dataset-open failures, outlet-resolution failures, and geometry-assembly
failures from broader engine errors.

## Module Metadata

```python
__version__: str
```

Installed package version reported by `importlib.metadata.version("pyshed")`.
