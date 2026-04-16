# Small Outlet Area Mismatch Against Reference Baselines

- Surface: `shed` CLI
- Severity: high
- Dataset: `/Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu`
- Benchmark coordinate: `lat=42.1524185132387`, `lon=24.8674213350357`

## Repro

1. `shed` single-outlet benchmark result:
   `/tmp/shed-bench-20260416/single/small_bulgaria/run-01/stdout.json`
2. `hydra merit` reference result:
   `/tmp/hydra-bench-20260416/single/merit/small_bulgaria/run-01/stdout.json`
3. `hydra hydrosheds-v1` reference result:
   `/tmp/hydra-bench-20260416/single/hydrosheds-v1/small_bulgaria/run-01/stdout.json`

## Expected

- Different hydrofabrics will not match exactly.
- Even so, the extracted drainage area should stay in the same broad magnitude for the same outlet.

## Observed

- `shed` reported `0.07784999909090887 km2`.
- `hydra merit` reported `8982.749185064913 km2`.
- `hydra hydrosheds-v1` reported `10326.655841779788 km2`.
- So `shed` is smaller by roughly:
  - `115,385x` versus `hydra merit`
  - `132,648x` versus `hydra hydrosheds-v1`
- The same benchmark set does not show that pattern for larger outlets:
  - `medium_rhine`: `shed=159783.3539 km2`, `hydra merit=159608.6173 km2`, `hydra hydrosheds-v1=159775.9937 km2`
  - `large_danube`: `shed=787505.1069 km2`, `hydra merit=786303.5348 km2`, `hydra hydrosheds-v1=786104.3574 km2`
- That makes the issue look specific to small-outlet handling rather than a general area-computation bias.

## Likely Layer

- Outlet resolution / snapping for small basins.
- `shed` may be resolving this coordinate to a tiny local terminal atom instead of the expected downstream basin outlet.
- The returned `terminal_atom_id` for this case is `160006876`, which is worth tracing through the outlet-resolution path.

## Artifacts

- `/tmp/shed-bench-20260416/single/small_bulgaria/run-01/stdout.json`
- `/tmp/hydra-bench-20260416/single/merit/small_bulgaria/run-01/stdout.json`
- `/tmp/hydra-bench-20260416/single/hydrosheds-v1/small_bulgaria/run-01/stdout.json`

## Next Fix Hint

- Reproduce this outlet with verbose logging around outlet resolution and terminal refinement.
- Compare the resolved catchment / terminal atom against the nearest downstream candidates.
- Check whether the current path is missing a snap step that hydra applies for near-channel points.
