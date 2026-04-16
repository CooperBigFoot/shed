# 2026-04-16 Hydra CLI Baseline vs Shed

- Date: `2026-04-16`
- `hydra` binary: `~/.cargo/bin/hydra` (`0.1.76`)
- `shed` binary: `/tmp/shed-bench-20260416/install/bin/shed` (`0.1.39`)
- `hydra` caches:
  - MERIT: `~/.hydra-shed/data -> /Users/nicolaslazaro/data/merit-hydro-processed`
  - HydroSHEDS v1: `~/.hydra-shed/data-hydrosheds-v1`
- `shed` dataset: `/Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu`
- Raw artifacts:
  - `/tmp/hydra-bench-20260416`
  - `/tmp/shed-bench-20260416`
- Rebuilt summary JSON:
  - `/tmp/hydra-bench-20260416/summary.json`
  - `/tmp/shed-bench-20260416/summary.json`

## Method

- Offline-only benchmark. No downloads were allowed.
- This was a rough directional baseline, not an apples-to-apples comparison:
  - `hydra` and `shed` were run against different hydrofabrics
  - the goal was to get an order-of-magnitude feel for current behavior, not declare a definitive winner
- `hydra` coordinates were prevalidated with `--dry-run` on both `merit` and `hydrosheds-v1`.
- Single-outlet runs:
  - 3 coordinates
  - 5 repeats each
  - `hydra`: 2 fabrics
  - `shed`: 1 GRIT HFX dataset
- Batch runs:
  - 300 outlets total
  - 100 copies each of the small, medium, and large coordinate classes
  - 3 repeats
- Timings use `/usr/bin/time -lp` wall clock.
- `hydra` also reports internal `elapsed_secs` in JSON.
- Memory could not be collected reliably in this sandbox because `/usr/bin/time -lp` emitted `sysctl kern.clockrate: Operation not permitted` and did not include the usual peak-memory fields.

## Coordinates

| Case | Lat | Lon | Notes |
|---|---:|---:|---|
| `small_bulgaria` | 42.1524185132387 | 24.8674213350357 | Small outlet, valid on both hydra fabrics |
| `medium_rhine` | 51.8548282167870 | 6.08365368932144 | Medium outlet, valid on both hydra fabrics |
| `large_danube` | 45.2260726879592 | 28.6965807743404 | Large outlet, valid on both hydra fabrics |

## Results

### Single-outlet medians

| Tool | Fabric / Dataset | Small wall s | Medium wall s | Large wall s |
|---|---|---:|---:|---:|
| `hydra` | `merit` | 0.51 | 0.82 | 1.64 |
| `hydra` | `hydrosheds-v1` | 0.19 | 0.23 | 0.33 |
| `shed` | `grit-hfx-eu` | 0.50 | 2.88 | 29.03 |

### Batch medians

| Tool | Fabric / Dataset | Batch wall s | Sec / outlet |
|---|---|---:|---:|
| `hydra` | `merit` | 362.76 | 1.209 |
| `hydra` | `hydrosheds-v1` | 13.48 | 0.045 |
| `shed` | `grit-hfx-eu` | 921.62 | 3.072 |

### Hydra internal elapsed medians

| Tool | Fabric | Small cli s | Medium cli s | Large cli s | Batch cli s | Batch cli s / outlet |
|---|---|---:|---:|---:|---:|---:|
| `hydra` | `merit` | 0.341 | 0.638 | 1.458 | 362.151 | 1.207 |
| `hydra` | `hydrosheds-v1` | 0.040 | 0.081 | 0.177 | 13.191 | 0.044 |

## Interpretation

- The runs give a rough lower bound on the gap worth caring about:
  - `shed` is in the same ballpark as `hydra merit` on the smallest case
  - `shed` falls behind more clearly on the medium and large cases
- On this 300-outlet benchmark, `shed` throughput was about:
  - `2.5x` slower than `hydra merit`
  - `~68x` slower than `hydra hydrosheds-v1`
- Those ratios should be treated as directional only because the hydrofabrics differ.
- `hydra hydrosheds-v1` is itself dramatically faster than `hydra merit` on the same coordinates, which shows that fabric choice alone can move the result by a large amount.

## Caveats

- This is a same-coordinate comparison, not a same-hydrofabric comparison:
  - `hydra` uses MERIT or HydroSHEDS v1
  - `shed` uses GRIT HFX Europe
- Because the hydrofabrics differ, these results are useful for rough calibration only.
- `hydra` always writes vector output to an output directory; `shed` singles were benchmarked JSON-only, while `shed` batch was benchmarked end-to-end with GeoJSON output.
- The previously used tiny GRIT outlet at `70.45°N` was excluded because HydroSHEDS v1 coverage stops at `60°N`.
