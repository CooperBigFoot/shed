# Changelog

All notable changes to `pyshed` are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[PEP 440](https://peps.python.org/pep-0440/) versioning (decoupled from the workspace's
per-commit Rust crate versioning).

## [0.1.0rc3] - 2026-04-17

Third release candidate. Fixes a build-order bug in ci/config.sh: PROJ's
cmake requires TIFF, so build_tiff must run before build_proj. rc2 built
PROJ before TIFF and failed with "Could NOT find TIFF (missing:
TIFF_LIBRARY)".

## [0.1.0rc2] - 2026-04-17

Second release candidate. Fixes CI checkout permissions — the top-level
`permissions: actions: read` block in build-wheels.yaml was overriding
the default and removing `contents: read`, causing `actions/checkout`
to fail on the private repository.

## [0.1.0rc1] - 2026-04-17

First release candidate — exercises the TestPyPI publication pipeline. No
functional difference from the planned 0.1.0 final.

## [0.1.0] - 2026-04-17

### Added
- First public release on PyPI.
- `pyshed.Engine(path).delineate(lat, lon)` and `.delineate_batch(outlets)`.
- `DelineationResult` with `geometry_wkb`, `to_geojson()`, area, snap info.
- Apple Silicon macOS wheel (cp39-abi3, `macosx_11_0_arm64`).
- Bundled GDAL 3.12.1, PROJ 9.7.1, GEOS 3.14.1, libtiff, SQLite, etc. (see `LICENSES/`).
