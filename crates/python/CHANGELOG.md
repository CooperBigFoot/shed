# Changelog

All notable changes to `pyshed` are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[PEP 440](https://peps.python.org/pep-0440/) versioning (decoupled from the workspace's
per-commit Rust crate versioning).

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
