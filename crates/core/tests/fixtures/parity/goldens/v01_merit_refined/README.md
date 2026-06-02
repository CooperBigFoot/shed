# Oracle C - merit-basins/0.1.0 Refined Goldens

These goldens are captured from:

```text
https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/
```

The capture path opens the remote dataset, attaches `LocalTiffRasterSource`, and
then runs the unmodified `Engine::delineate()`. During refinement, the engine
calls `DatasetSession::localize_raster_window()` for the terminal bbox; the COG
reader range-reads only intersecting remote `flow_dir.tif` and `flow_acc.tif`
tiles into `HFX_CACHE_DIR`, then `LocalTiffRasterSource` reads those local
windows for the actual carve.

Live Step 4 accepted oracle:

- `rhine_basel` was evaluated at `GeoCoord::new(7.5890, 47.5596)` with a
  `5000 m` search radius.

Real-data D8 parity is achieved through `rhine_basel`. The
`mekong_phnom_penh` candidate at `GeoCoord::new(104.9300, 11.5700)` with a
`5000 m` search radius remains deferred and is not a required or gating golden.
After the deterministic dissolve fix, it still showed residual run-to-run
canonical-WKB drift at continental scale. The suspected source is downstream of
shed's dissolve path, likely floating-point nondeterminism in
`geo::BooleanOps::union`; this is tracked as follow-up work rather than part of
the Step 4b gate.

The accepted C record asserts the public-result invariants available from
`DelineationResult`: `RefinementOutcome::Applied`, finite `refined_outlet`
inside the terminal-unit bbox, non-empty final watershed geometry, positive
finite geodesic `area_km2`, three-run canonical WKB/scalar stability, and
window byte counts under the 500 MB per-outlet ceiling. The golden records do
not store a refined-terminal sub-polygon metric because `DelineationResult`
exposes only the final assembled watershed geometry. Terminal-carve containment
was independently verified during the Step 4 investigation; see
`docs/hfx-v02-redesign/m1-step4-c-investigation.md`. Terminal carve behavior is
also covered by the `refine.rs` unit tests.

MERIT raster contract recorded in the JSON:

- Remote COG source, localized to plain north-up EPSG:4326 GeoTIFF windows
- PixelIsArea raster interpretation; refinement uses pixel centers
- ESRI D8 flow-direction encoding
- `uint8` flow direction with `255` nodata
- `float32` accumulation with source nodata decoded as `NaN`

`localize_raster_window()` is `pub(crate)`, so the GDAL proof cannot call it
directly from `shed-gdal`. The proof first materializes windows by running the
core capture delineations, then reads the cached `.tif` files through both
`LocalTiffRasterSource` and `GdalRasterSource`. For the blessed `rhine_basel`
window, it verifies matching tile geotransforms, sample values, nodata handling,
and direct terminal-carve output. The C oracle is therefore scoped as: core TIFF
reader carve proven tile-identical to the GDAL production decode for the
localized C window.

Refresh command:

```bash
SHED_PARITY_R2_CAPTURE=1 cargo test -p shed-core --test parity_v01_oracle_capture -- --ignored --nocapture
```

Decode proof command, after capture has populated `HFX_CACHE_DIR`:

```bash
SHED_PARITY_R2_CAPTURE=1 cargo test -p shed-gdal --test raster_decode_parity merit_c_windows_tiff_match_gdal -- --ignored --nocapture
```
