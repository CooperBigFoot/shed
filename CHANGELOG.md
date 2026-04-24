# Changelog

All notable changes to `shed` (the CLI binary) and `shed-core` (the engine crate) are documented here.

## Unreleased

### Added

- Documented remote HFX dataset locations backed by the object-store
  integration, including local paths, `file://`, `s3://`, Cloudflare R2 HTTPS
  URLs, manifest/graph cache behavior, `HFX_CACHE_DIR`, and parquet range
  reads.

## 0.1.56 — 2026-04-20

### Changed

- Default snap strategy flipped from `SnapStrategy::DistanceFirst` to `SnapStrategy::WeightFirst` to align with HFX v0.2. This fixes a small-basin correctness bug where outlets coincident with a tiny tributary stub's first vertex resolved to a ~0.08 km² headwater instead of the ~9000 km² mainstem.
- Bumped `hfx-core` pin from `=0.1.26` to `=0.2.0`.

### Opt-out

- Legacy distance-first behavior remains available via `--snap-strategy distance-first` (CLI) or `snap_strategy="distance-first"` (Python). Use for datasets whose `weight` column is not hydrologically rank-meaningful.
