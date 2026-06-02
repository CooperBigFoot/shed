# Oracle A - grit/1.0.0 Non-Refined Goldens

These goldens are captured from:

```text
https://basin-delineations-public.upstream.tech/grit/1.0.0/
```

The capture path opens the remote dataset and builds `Engine` with no raster
source. `grit/1.0.0` declares `has_rasters=false`, so each accepted case records
`RefinementOutcome::NoRastersAvailable`. This is the expected engine behavior;
the fixture does not attach a raster source to force refinement.

Captured outlets:

- `zurich`: `GeoCoord::new(8.5417, 47.3769)`, default `1000 m` search radius
- `repparfjord`: `GeoCoord::new(23.04, 69.97)`, explicit `50000 m` search radius

`repparfjord` is captured with `50000 m` because the pinned `grit/1.0.0` snap
table has no candidate within either the default `1000 m` radius or a `5000 m`
radius for that coordinate.

`hammerfest` is excluded. The required coordinate
`GeoCoord::new(23.6821, 70.6634)` has no snap candidate within the recommended
explicit `5000 m` radius in pinned `grit/1.0.0`, so Step 4 records the exclusion
instead of silently increasing that case beyond the plan recommendation.

The JSON records include pinned URL identity for `manifest.json`,
`catchments.parquet`, and `graph.arrow` from remote HEAD metadata. The offline
artifact test validates only the recorded strings and never re-fetches remote
artifacts.

Offline comparison command:

```bash
cargo test -p shed-core --test parity_golden_artifacts
```

Refresh command:

```bash
SHED_PARITY_R2_CAPTURE=1 cargo test -p shed-core --test parity_v01_oracle_capture -- --ignored --nocapture
```
