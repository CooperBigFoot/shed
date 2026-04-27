# Phase 4 R2 Smoke — Cloud-Native Delineation, Cold/Warm Cache

- Status: closed
- Date: 2026-04-27
- Dataset: MERIT-Basins global v0.1.0 from `basin-delineations-public.upstream.tech`
- Outlet: lat=70.4521297483898 lon=28.4906601273434

## Repro

Cold cache:

```bash
HFX_SMOKE_CACHE_MODE=cold bash scripts/phase4-smoke-r2.sh \
  > scratchpad/benchmarks/20260427-phase4-r2-smoke-cold.json
```

Warm cache:

```bash
HFX_SMOKE_CACHE_MODE=warm bash scripts/phase4-smoke-r2.sh \
  > scratchpad/benchmarks/20260427-phase4-r2-smoke-warm.json
```

## Results

| Cache | Wall | Peak RSS | RSS vs local-disk | COG header bytes | COG tile bytes | COG tile count | COG window pixels | All gates passed |
|---|---|---|---|---|---|---|---|---|
| Cold | 37 s | 626 MB | 1.05× | 32 MB | 280 KB | 2 | 3420 | true |
| Warm | 32 s | 568 MB | 1.00× | 0 (cached) | 0 (cached) | 0 | 0 | true |

## Cloud-native invariants

- Cold-cache remote delineation transfers ~90 MB for this outlet: graph.arrow is ~57 MB once per dataset version, COG headers are ~32 MB once per dataset version, selected COG tiles are ~280 KB per outlet, and Parquet row-groups add ~1-5 MB transiently.
- Warm-cache remote delineation avoids the graph and COG cache misses, leaving ~5 MB of transient Parquet reads for this outlet.
- Compared with the 65 GB published dataset, that is ~720× cold-cache reduction and ~13,000× warm-cache reduction.

## What landed across Phase 4

- R2 upload plus custom domain for the global MERIT-Basins HFX dataset, served through HTTP/2 and the reverse proxy.
- R3 COG window reads in `crates/core/src/cog.rs` and `RemoteRasterCache::get_or_fetch_window`.
- Parquet remote-id indexing fix in commit `7c8e72d`.

## Artifacts

- `scratchpad/benchmarks/20260427-phase4-r2-smoke-cold.json`
- `scratchpad/benchmarks/20260427-phase4-r2-smoke-warm.json`

## Caveats

- The smoke script's `non_cog_bytes_on_wire` field reports `0` because the byte counter only taps the COG path.
- The about-90 MB cold-cache figure is derived from per-component sizing, not from a single trace measurement.
