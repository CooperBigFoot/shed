# pyshed API Reference

Developer-oriented reference for the supported public `pyshed` Python API.
This file mirrors the runtime surface re-exported from
[`python/pyshed/__init__.py`](python/pyshed/__init__.py) and the shipped
PEP 561 stub in [`python/pyshed/__init__.pyi`](python/pyshed/__init__.pyi).

## Public Exports

The `pyshed` package exports these names:

- `Engine`
- `DelineationResult`
- `ShedError`
- `DatasetError`
- `ResolutionError`
- `AssemblyError`
- `__version__`

`_pyshed` exists as a compiled implementation detail, but its helper functions
are not part of the supported public API.

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
) -> None
```

Opens an HFX dataset and constructs a delineation engine.

| Parameter | Type | Meaning |
|---|---|---|
| `dataset_path` | `str` | Path to the HFX dataset root directory |
| `snap_radius` | `float \| None` | Optional snap-path search radius in metres; must be finite and positive when provided |
| `snap_strategy` | `"distance-first" \| "weight-first" \| None` | Snap ranking strategy. Defaults to `"weight-first"` (HFX v0.2 contract: higher weight = more hydrologically significant). Pass `"distance-first"` to opt back into proximity-first ranking for datasets whose `weight` column is not rank-meaningful. |
| `snap_threshold` | `int \| None` | Minimum upstream-pixel count for stream-network snapping |
| `clean_epsilon` | `float \| None` | Topology-cleaning epsilon in degrees |
| `refine` | `bool` | Whether raster-based terminal refinement is enabled |

### Exceptions

- `DatasetError` when the dataset cannot be opened or read.
- `ValueError` when a configuration argument is invalid, such as an unknown
  `snap_strategy` or a non-positive `snap_radius`.

### Methods

```python
delineate(*, lat: float, lon: float) -> DelineationResult
```

Delineates the watershed upstream of a single outlet.

| Parameter | Type | Meaning |
|---|---|---|
| `lat` | `float` | Outlet latitude in decimal degrees (EPSG:4326) |
| `lon` | `float` | Outlet longitude in decimal degrees (EPSG:4326) |

#### Exceptions

- `ValueError` when `lat` or `lon` is outside the valid geographic range.
- `ResolutionError` when the outlet cannot be resolved to a terminal catchment.
- `DatasetError` when underlying dataset reads fail during delineation.
- `AssemblyError` when watershed geometry assembly fails.
- `ShedError` for other engine failures such as traversal or refinement errors.

```python
delineate_batch(outlets: list[dict[str, float]]) -> list[DelineationResult]
```

Delineates watersheds for a batch of outlets that share the same engine
configuration.

Each outlet must be a dict with exactly these keys:

```python
{"lat": 47.3769, "lon": 8.5417}
```

Results are returned in input order. The call raises on the first failure in
that order rather than returning per-outlet error objects.

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
