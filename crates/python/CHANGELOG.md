# Changelog

All notable changes to `pyshed` are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[PEP 440](https://peps.python.org/pep-0440/) versioning (decoupled from the workspace's
per-commit Rust crate versioning).

## [Unreleased]

## [0.1.10] - 2026-05-04

### Changed

- Updated the root README examples to use the canonical public GRIT HFX
  v1.0.0 dataset at
  `https://basin-delineations-public.upstream.tech/grit/1.0.0/`.

### Added

- Added `AreaOnlyResult` via `Engine.delineate(..., geometry=False)` for callers
  that only need scalar delineation metadata and area.
- Added `DelineationResult.geometry_bbox` and cached repeated
  `DelineationResult.geometry_wkb` property access.
- Documented `pyshed.Engine(...)` dataset strings for local paths, `file://`,
  `s3://`, and Cloudflare R2 HTTPS URLs, plus remote manifest/graph caching via
  `HFX_CACHE_DIR` and parquet range-read behavior.

## [0.1.7] - 2026-04-21

### Changed

- Reverted the experimental Linux manylinux wheel setup. `pyshed` is again
  published as an Apple Silicon macOS-only wheel while Linux support is left
  open for future community contribution.

## [0.1.6] - 2026-04-21

### Fixed

- Quoted the Linux `LDFLAGS` assignment in the cibuildwheel environment so the
  manylinux job parses correctly. This fixes the immediate `Malformed
  environment option` failure seen in `0.1.5` before the Linux build even
  started.

## [0.1.5] - 2026-04-21

### Fixed

- Corrected the Linux manylinux wheel stack builder to handle `lib64` installs
  from CMake projects like PROJ while still preferring `lib` where explicitly
  requested. This fixes the failed `0.1.4` Linux wheel build before wheel
  repair.

## [0.1.4] - 2026-04-21

### Added

- Added Linux x86_64 wheel builds via cibuildwheel's `manylinux2014` image,
  alongside the existing Apple Silicon macOS wheel.
- Added Linux wheel verification with `auditwheel show`, an `ldd` dependency
  check against the repaired wheel, and a clean-container import smoke test.

### Changed

- Documented Linux x86_64 as a supported wheel platform in the package README
  and metadata.

## [0.1.3] - 2026-04-20

### Changed

- Default `snap_strategy` is now `"weight-first"` (was `"distance-first"`). Fixes small-basin correctness where an outlet coincident with a tiny tributary stub's first vertex resolved to a ~0.08 km² headwater instead of the ~9000 km² mainstem. Aligns pyshed with the HFX v0.2 weight contract.

### Opt-out

- Pass `snap_strategy="distance-first"` to `Engine(...)` or `Engine.delineate(...)` to keep the v0.1.2 behavior.

## [0.1.2] - 2026-04-18

### Added
- Shipped PEP 561 typing metadata in the wheel via `pyshed/__init__.pyi` and
  `pyshed/py.typed`, so IDE hover, autocomplete, and static type checking now
  work against the public Python API.
- Added a developer-oriented API reference in `crates/python/API.md`
  documenting the exported classes, return types, properties, and exceptions.

### Changed
- Corrected the batch-delineation README example to match the real API shape:
  `Engine.delineate_batch()` accepts outlet dicts with `"lat"` and `"lon"`
  keys.

## [0.1.1] - 2026-04-17

### Changed
- Locked GDAL's cmake dependency discovery to the wheel build prefix and passed
  explicit PROJ, TIFF, SQLite, GEOS, and curl hints to reduce accidental
  linkage against runner-local libraries.
- Added a delocate preflight step that inspects install names with `otool`
  before repair, plus an unrepaired-wheel `delocate-listdeps` dump ahead of
  `delocate-wheel`.
- Seeded bundled `GDAL_DATA` and `PROJ_DATA` in `pyshed.__init__` before
  importing `_pyshed`, while keeping the existing PyO3 runtime injection as a
  belt-and-suspenders fallback. `_set_proj_data()` now also sets the `PROJ_DATA`
  GDAL config option before calling `OSRSetPROJSearchPaths`.

## [0.1.0] - 2026-04-17

First public release on PyPI. Apple Silicon macOS only (`macosx_11_0_arm64`);
community contributions for Linux / Intel / Windows are welcome — see
[CONTRIBUTING.md](https://github.com/CooperBigFoot/shed/blob/main/CONTRIBUTING.md).

### Added
- `pyshed.Engine(path).delineate(lat, lon)` and `.delineate_batch(outlets)`.
- `DelineationResult` with `geometry_wkb`, `to_geojson()`, area, and snap info.
- Typed exception hierarchy rooted at `ShedError` (`DatasetError`,
  `ResolutionError`, `AssemblyError`).
- Bundled native stack inside the wheel: GDAL 3.12.1, PROJ 9.7.1, GEOS 3.14.1,
  libtiff 4.7.1, SQLite, zlib, libcurl, nghttp2, OpenSSL, libpng, jpeg-turbo,
  zstd, libdeflate, xz. All 14 licenses shipped under
  `pyshed-0.1.0.dist-info/licenses/`.
- Runtime injection of bundled `GDAL_DATA` and `proj.db` via `CPLSetConfigOption`
  and `OSRSetPROJSearchPaths` at module import time.

## Pre-release history

### [0.1.0rc4] - 2026-04-17
Dropped `PROJ_RENAME_SYMBOLS` — PROJ's cmake renames its own symbols but not
libgeod's, so GDAL's preprocessor rewrote `geod_init` → `internal_geod_init`
against a PROJ that didn't export the renamed names.

### [0.1.0rc3] - 2026-04-17
Fixed build order: `build_tiff` must run before `build_proj`; PROJ 9.7's cmake
requires TIFF.

### [0.1.0rc2] - 2026-04-17
Removed a top-level `permissions: actions: read` block that was stripping
`contents: read` and causing `actions/checkout` to fail on the private repo.

### [0.1.0rc1] - 2026-04-17
Initial TestPyPI dry run.
