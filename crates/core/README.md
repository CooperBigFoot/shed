# shed-core

Pure-Rust core library for the shed watershed extraction engine. It handles two responsibilities: loading HFX datasets from disk (`session` + `reader`), and providing all watershed-delineation algorithms (`algo`). External capabilities — GDAL raster I/O, GEOS geometry repair — are kept behind traits defined here and implemented in `shed-gdal`, so the hot path has no native dependencies.

## Architecture

```mermaid
graph TD
    session[DatasetSession\nsession.rs] --> reader[reader/]
    reader --> manifest[manifest.rs\nparse manifest.json]
    reader --> graph[graph.rs\ndecode graph.arrow]
    reader --> catchment[catchment_store.rs\nlazy Parquet reader]
    reader --> snap[snap_store.rs\nlazy Parquet reader]

    algo[algo/] --> foundation[Foundation Types\ncoord · area · distance\ngeo_transform · flow_dir\nsnap_threshold · clean_epsilon\ntile_state]
    algo --> raster_infra[Raster Infrastructure\nraster_tile · flow_direction_tile\naccumulation_tile · catchment_mask]
    algo --> algorithms[Raster Algorithms\ntrace · snap · rasterize · polygonize]
    algo --> graph_traversal[Graph Traversal\ncollect_upstream]
    algo --> geometry[Geometry Processing\ndissolve · clean_topology · hole_fill\nlargest_polygon · watershed_area\nself_intersection]
    algo --> pipeline[Pipeline + Traits\nwatershed_geometry · traits]

    traits[traits.rs] -.->|implemented by| shed_gdal[shed-gdal]
```

**Data flow for a delineation:**

```mermaid
sequenceDiagram
    participant S as DatasetSession
    participant T as RasterSource (shed-gdal)
    participant A as algo/

    S->>S: open() — validate layout, load graph, prepare Parquet readers
    S->>T: load_flow_direction(bbox)
    S->>T: load_accumulation(bbox)
    T-->>A: FlowDirectionTile<Raw>
    T-->>A: AccumulationTile<Raw>
    A->>A: rasterize_polygon → CatchmentMask
    A->>A: AccumulationTile.apply_mask → AccumulationTile<Masked>
    A->>A: snap_pour_point → SnappedPoint
    A->>A: trace_upstream → CatchmentMask
    A->>A: polygonize → Vec<Polygon>
    A->>A: dissolve → MultiPolygon
    A->>A: WatershedGeometry pipeline → Polygon
```

## Glossary

| Term | Meaning |
|---|---|
| Atom | Fundamental spatial unit in HFX — one catchment polygon with an ID, area, and WKB geometry |
| AtomId | Unique positive `i64` identifier for an atom (newtype in `hfx_core`) |
| D8 | Eight-direction flow model where each raster cell drains to exactly one of its 8 neighbours |
| ESRI D8 | D8 encoding using powers of two: E=1, SE=2, S=4, SW=8, W=16, NW=32, N=64, NE=128 |
| TauDEM D8 | D8 encoding counter-clockwise from east: E=1, NE=2, N=3, NW=4, W=5, SW=6, S=7, SE=8 |
| Upstream set | All atoms reachable via upstream adjacency from a terminal atom, inclusive of the terminal itself |
| Pour point | The outlet cell of a watershed — the single cell where flow exits the catchment |
| Snap | Moving a pour point to the nearest high-accumulation cell within a catchment mask |
| SnapThreshold | Minimum flow-accumulation pixel count a cell must exceed to be a snap candidate |
| Dissolve | Boolean union of all catchment polygons in the upstream set into one multi-polygon |
| CleanEpsilon | Tiny buffer distance (degrees) used in buffer-unbuffer topology cleaning |
| HoleFillMode | Policy for interior holes: remove all, or keep holes above an area threshold |
| Typestate | Compile-time state tracking via zero-size type parameters (`Raw`/`Masked`, `Dissolved`/`TopologyCleaned`/`HolesFilled`) |
| GeoTransform | GDAL-style affine transform storing origin + pixel dimensions (no rotation/shear) |
| Row-group pruning | Skipping Parquet row groups whose bbox statistics don't intersect the query bbox |

## Key Types

### Session / Reader layer

| Type | File | Role |
|---|---|---|
| `DatasetSession` | `session.rs` | Entry point — open an HFX dataset, validate layout, expose readers |
| `RasterPaths` | `session.rs` | Validated paths to `flow_dir.tif` + `flow_acc.tif` (no GDAL handles) |
| `CatchmentStore` | `reader/catchment_store.rs` | Lazy Parquet reader for `catchments.parquet` with bbox pruning |
| `SnapStore` | `reader/snap_store.rs` | Lazy Parquet reader for `snap.parquet` with bbox pruning |
| `SessionError` | `error.rs` | All dataset-open and read errors |

### Algorithm layer

| Type / Function | File | Role |
|---|---|---|
| `WatershedGeometry<S>` | `algo/watershed_geometry.rs` | Typestate pipeline: `Dissolved` → `TopologyCleaned` → `HolesFilled` → `Polygon` |
| `FlowDirectionTile<S>` | `algo/flow_direction_tile.rs` | Typed D8 raster tile; `S` is `Raw` or `Masked` |
| `AccumulationTile<S>` | `algo/accumulation_tile.rs` | Typed flow-accumulation tile; `apply_mask` transitions `Raw` → `Masked` |
| `CatchmentMask` | `algo/catchment_mask.rs` | Boolean visited-cell set; output of `trace_upstream` and `rasterize_polygon` |
| `RasterTile<T>` | `algo/raster_tile.rs` | Generic row-major tile with OOB-safe `(isize,isize)` indexing |
| `GeoTransform` | `algo/geo_transform.rs` | Pixel ↔ geographic coordinate conversion |
| `FlowDir` | `algo/flow_dir.rs` | D8 direction enum with ESRI and TauDEM decoding |
| `SnappedPoint` | `algo/snap.rs` | Result of a successful pour-point snap (grid cell + geo coord + accumulation) |
| `snap_pour_point` | `algo/snap.rs` | Snap outlet to nearest masked cell above `SnapThreshold` |
| `trace_upstream` | `algo/trace.rs` | DFS upstream traversal returning a `CatchmentMask` |
| `collect_upstream` | `algo/upstream.rs` | BFS upstream traversal over `DrainageGraph` — returns `UpstreamAtoms` |
| `UpstreamAtoms` | `algo/upstream.rs` | Terminal atom + full upstream set with O(1) membership check |
| `dissolve` | `algo/dissolve.rs` | Parallel boolean union of polygon slices |
| `RasterSource` | `algo/traits.rs` | Trait for windowed GeoTIFF reads; implemented by `shed-gdal::GdalRasterSource` |
| `GeometryRepair` | `algo/traits.rs` | Trait for geometry repair; implemented by `shed-gdal::GdalGeometryRepair` |
