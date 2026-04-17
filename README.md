# shed

## Component Tracker

| # | Component | Status | Version | Tests |
|---|---|---|---|---|
| 1 | Dataset Session | **Done** | v0.1.10 | 68 |
| 2 | Generic Algorithm Core | **Done** | v0.1.19 | 296 |
| 3 | Outlet Resolution | **Done** | v0.1.27 | 38 |
| 4 | Inclusive Upstream Traversal | **Done** | v0.1.29 | 25 |
| 5 | Terminal Refinement | **Done** | v0.1.31 | 20 |
| 6 | Geometry Fetch and Final Assembly | **Done** | v0.1.33 | 13 |
| 7 | Engine Surface, CLI, and PyO3 Bindings | **Done** | v0.1.37 | 31 |
| 8 | Wire pyshed into watershed-retrieve | Not started | — | — |

## Port Plan

### Summary

- Scope is engine-first.
- Build the HFX loader, query engine, and minimal invocation path first.
- Port only the reusable hydra-shed algorithmic core: snap, D8 trace, polygonize, dissolve, topology cleanup, and geometry repair.
- Do not port MERIT/HydroSHEDS operational systems into v1: downloads, cache management, shapefile/sqlite readers, basin boundary selection, reverse geocoding, checkpointing, or batch output UX.

### Public Interfaces

- `DatasetSession`
  Load one HFX dataset root and expose typed access to manifest, graph, catchments, snap targets, and optional rasters.
- `Engine`
  Accept a loaded session plus runtime options and execute one delineation query for one outlet.
- `RuntimeOptions`
  Own engine behavior such as snap radius, distance tolerance, accumulation threshold, and raster-refinement toggles.
- `DelineationResult`
  Keep the v1 result minimal: terminal atom id, snapped outlet, final polygon, area in km², and whether snap or raster refinement ran.

### Component 1: Dataset Session

#### Goal

- Load an HFX dataset once and hold the engine-facing state needed for repeated queries.

#### Build

- Parse `manifest.json` into `hfx-core` types.
- Load `graph.arrow` and convert it into a traversal-friendly in-memory shape.
- Add readers for `catchments.parquet` and `snap.parquet` that preserve bbox-based pruning.
- Add optional raster handles for `flow_dir.tif` and `flow_acc.tif`.
- Fail fast if required artifacts are missing or if manifest-declared optional artifacts are inconsistent with the files on disk.

#### Carry Over From `hydra-shed`

- Reuse the idea that expensive dataset state is loaded once and queried many times.
- Do not carry over `BasinCode`, `Comid`, shapefile/sqlite layouts, or MERIT path logic.

#### Output

- A session object that can answer:
  - what artifacts exist
  - what topology class the dataset declares
  - how to fetch graph neighbors
  - how to fetch catchments or snap targets by bbox or id
  - how to open raster windows for one terminal atom

#### Done When

- A query can open one HFX dataset root without touching any fabric-specific code.
- Graph traversal can run entirely from session state.
- Catchment and snap reads are not implemented as full-file scans by default.

### Component 2: Generic Algorithm Core

#### Goal

- Extract the reusable hydra-shed algorithm pieces into HFX-neutral modules.

#### Build

- Port `snap_pour_point`.
- Port D8 reverse trace.
- Port raster-mask polygonization.
- Port dissolve, topology cleanup, largest-polygon selection, and GDAL/GEOS repair.
- Replace MERIT-only wrappers with generic raster/grid/geometry types owned by `shed`.

#### Carry Over From `hydra-shed`

- Carry over algorithm logic and tests where the behavior is generic.
- Do not carry over MERIT threshold ladders, MERIT pixel-area assumptions, fixed upstream slot counts, or low-res reach-endpoint fallback behavior.

#### Output

- A small set of algorithm modules with no dependency on MERIT naming or layout assumptions.

#### Done When

- The ported modules can be tested in isolation with synthetic raster and geometry fixtures.
- No generic algorithm module depends on `BasinCode`, `Comid`, `RiverReach`, or MERIT cache/path types.

### Component 3: Outlet Resolution

#### Goal

- Resolve an outlet coordinate to one terminal HFX atom.

#### Build

- If `snap.parquet` exists:
  - query candidates within runtime search radius
  - rank by distance
  - break ties by weight
  - break remaining ties by mainstem preference
- If `snap.parquet` does not exist:
  - prune `catchments.parquet` by bbox stats
  - run point-in-polygon on remaining candidates
- Return the chosen terminal atom id and enough detail to explain the resolution path.

#### Carry Over From `hydra-shed`

- Carry over the idea of a cheap bbox prefilter before exact geometry checks.
- Do not carry over basin boundary preselection or buffered boundary fallback.

#### Output

- A terminal atom resolver that is pure HFX behavior.

#### Done When

- The same engine surface works for datasets with and without `snap.parquet`.
- Resolution logic has no dependency on fabric-specific boundary artifacts.

### Component 4: Inclusive Upstream Traversal

#### Goal

- Starting from a terminal atom, collect the full upstream atom set required by HFX v0.1.

#### Build

- Traverse `graph.arrow` from the terminal atom.
- Maintain a visited set.
- Support both `tree` and `dag` topologies.
- Return the ordered atom set used by later geometry fetch and dissolve steps.

#### Carry Over From `hydra-shed`

- Carry over the traversal shape from `collect_upstream`.
- Replace reach-based traversal with direct HFX atom traversal.

#### Output

- A graph traversal module over HFX atom ids.

#### Done When

- Shared upstream nodes in DAG datasets are visited exactly once.
- Traversal does not require reach geometry or MERIT reach records.

### Component 5: Terminal Refinement

#### Goal

- Refine only the terminal atom when rasters are available.

#### Build

- Read the terminal atom polygon.
- Window `flow_dir.tif` and `flow_acc.tif` to the terminal atom bbox.
- Normalize flow-direction encoding from the manifest.
- Rasterize the terminal polygon into a mask.
- Apply snap-to-accumulation within the mask.
- Run reverse D8 trace from the snapped cell.
- Polygonize the traced mask.
- Replace the coarse terminal atom polygon with the refined sub-polygon.

#### Carry Over From `hydra-shed`

- Carry over the high-resolution refinement sequence almost 1:1.
- Drop the MERIT low-res fallback that snaps to a reach endpoint.

#### Output

- A refinement step that returns:
  - snapped outlet coordinate
  - refined terminal polygon when refinement succeeds
  - coarse terminal polygon when rasters are absent or refinement is disabled

#### Done When

- Raster refinement is entirely driven by HFX manifest and raster artifacts.
- Only the terminal atom geometry changes; upstream atoms remain coarse polygons.

### Component 6: Geometry Fetch and Final Assembly

#### Goal

- Build the final watershed geometry from the terminal atom plus all upstream atoms.

#### Build

- Fetch catchment geometries for the traversed atom set.
- Substitute the refined terminal polygon when available.
- Dissolve the geometry set.
- Run cleanup and repair.
- Keep `MultiPolygon<f64>` as the canonical internal geometry output for v1.
- Compute final geodesic area.

#### Carry Over From `hydra-shed`

- Carry over the dissolve and cleanup sequence.
- Do not carry over river clipping or stream-order output.

#### Output

- The final watershed boundary as a canonical `MultiPolygon<f64>` plus geodesic area.

#### Done When

- Final geometry assembly is independent of fabric-specific reach data.
- The terminal atom substitution path is explicit and tested.

### Component 7: Engine Surface, CLI, and PyO3 Bindings

#### Goal

- Compose the pipeline into a public `Engine` API, a CLI binary, and Python bindings (`pyshed`).

#### Built

- **Engine** (`crates/core/src/engine.rs`): `Engine` struct with builder pattern, `DelineationResult` with typed accessors and WKB export, `DelineationOptions` with per-call knobs, `RefinementOutcome` enum, batch delineation via rayon.
- **CLI** (`src/main.rs`): `shed delineate --dataset <path> --lat/--lon` or `--outlets <csv>`. GeoJSON FeatureCollection output (stdout or `--output`), `--json` machine-readable envelope, verbosity flags, `--no-refine`.
- **PyO3** (`crates/python/`): `pyshed.Engine(path)` with `delineate(lat, lon)` and `delineate_batch(outlets)`. Returns `DelineationResult` with `geometry_wkb`, `to_geojson()`, typed getters. GIL released during Rust computation. Custom exception hierarchy. Published as `pyshed` via maturin.
- **Supporting**: WKB encoding in `algo/wkb.rs`, `test-fixtures` feature flag on shed-core.

#### Divergence From Original Plan

- Original plan scoped Component 7 as "minimal engine surface, thin CLI if added, exclude batch." Actual scope expanded to include batch delineation (rayon), a full clap CLI with CSV batch input and `--json` envelope, and PyO3 bindings with maturin packaging. This was driven by the downstream goal of wiring into `watershed-retrieve`.
- `assembly.rs` stayed `pub(crate)` — the Engine consumes it internally. `EngineError::Assembly` stores an opaque `Box<dyn Error>` to preserve the error chain without leaking the internal type.
- GeoJSON serialization lives in CLI and PyO3, not in shed-core (presentation concern).

#### Done When

- `shed delineate --dataset <path> --lat <f> --lon <f>` works end-to-end. ✓
- `pyshed.Engine(path).delineate(lat, lon)` works from Python. ✓
- Batch mode processes multiple outlets in parallel. ✓
- `uv run pytest tests/ -q` passes from `crates/python/`. ✓

### Component 8: Wire pyshed into watershed-retrieve

#### Goal

- Add on-the-fly delineation to `watershed-retrieve` so users can pass `(lat, lon)` and get a watershed polygon delineated from any HFX-compliant dataset, not just pre-computed lookups.

#### Build

- Add `pyshed` as a dependency of `watershed-retrieve`.
- Add a `delineate(lat, lon, dataset=...)` function (or similar) that wraps `pyshed.Engine`.
- Return a `GeoDataFrame` matching the existing `get_watershed()` schema — the caller should not need to know whether the result was pre-computed or delineated on the fly.
- Handle the dataset path: accept an explicit path, or resolve from a configured default.
- Map `pyshed` exceptions to `watershed-retrieve`'s error hierarchy (`WatershedRetrieveError` and subclasses).

#### Exclude For Now

- Automatic HFX dataset downloading (user provides the dataset path).
- Caching of delineation results.
- River network output from the delineation path.
- Batch delineation at the `watershed-retrieve` API level (can be added later; `pyshed` already supports it).

#### Output

- A new public function in `watershed-retrieve` that delineates on the fly.
- The function returns the same `GeoDataFrame` shape as existing lookup functions.

#### Done When

- `watershed_retrieve.delineate(lat=47.3, lon=8.5, dataset="/path/to/hfx")` returns a single-row `GeoDataFrame` with a polygon geometry and area.
- The result is indistinguishable in schema from `get_watershed()` output.
- Errors from the engine surface as the appropriate `watershed-retrieve` exception types.

### Test Plan

- Unit tests
  - snap tie-breaking
  - D8 trace connectivity
  - polygonize edge cases
  - dissolve and cleanup
  - geometry repair
  - DAG traversal with shared upstream nodes
- Fixture tests
  - catchment-only HFX dataset
  - snap-enabled HFX dataset
  - raster-refined HFX dataset
  - dataset with `has_up_area = false`
- Acceptance checks
  - outlet resolves to the expected terminal atom
  - traversal returns the expected upstream set
  - raster refinement changes only the terminal atom contribution
  - final area and boundary are stable for representative fixtures

### Session Guidance

- Each component is a separate work packet for another LLM session.
- Every session should re-explore the repo and the relevant `hydra-shed` code before editing.
- Prefer direct ports where the algorithm is already generic.
- Prefer replacement, not adaptation, where the old code exists only to support MERIT/HydroSHEDS layout or UX.

## Python bindings

`pyshed` is published on PyPI (Apple Silicon only in v0.1):

```bash
pip install pyshed
```

See `crates/python/README.md` for usage and `CONTRIBUTING.md` for the open call for
cross-platform wheel contributions.
