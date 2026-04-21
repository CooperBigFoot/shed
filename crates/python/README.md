# pyshed

Python bindings for the `shed` watershed delineation engine. `pyshed` loads
[HFX-format](https://github.com/CooperBigFoot/hfx) datasets and returns
watershed polygons from a `(lat, lon)` outlet. The full native stack (GDAL,
PROJ, GEOS, libtiff, SQLite, and more) is bundled inside the wheel — no system
install required.

## Install

```bash
pip install pyshed
```

**Platform support (v0.1):** Apple Silicon macOS only (`macosx_11_0_arm64`).
Linux, Intel macOS, and Windows wheels are not yet built — see
[CONTRIBUTING.md](https://github.com/CooperBigFoot/shed/blob/main/CONTRIBUTING.md)
if you want to help port the build.

## Quickstart

```python
import pyshed

engine = pyshed.Engine("/path/to/hfx/dataset")
result = engine.delineate(lat=47.3769, lon=8.5417)
print(result.area_km2)
```

For batch delineation:

```python
outlets = [
    {"lat": 47.3769, "lon": 8.5417},
    {"lat": 46.9480, "lon": 7.4474},
]
results = engine.delineate_batch(outlets)
```

## API Reference

For the full developer-oriented API surface, including argument types, return
types, and the exception hierarchy, see [API.md](https://github.com/CooperBigFoot/shed/blob/main/crates/python/API.md).

## What it does

- Resolves the outlet coordinate to a terminal HFX atom (via `snap.parquet`
  or point-in-polygon on `catchments.parquet`).
- Walks the upstream graph in `graph.arrow` collecting all contributing atoms.
- Optionally refines the terminal atom geometry using `flow_dir.tif` /
  `flow_acc.tif` rasters when present.
- Returns a dissolved `MultiPolygon` + geodesic area in km².
- Bundles GDAL / PROJ / GEOS / libtiff / SQLite — no system GDAL install
  needed.

## Links

- **Source & issues:** https://github.com/CooperBigFoot/shed
- **HFX dataset spec:** https://github.com/CooperBigFoot/hfx
- **License:** MIT for `pyshed`; bundled native libraries retain their own
  licenses — see
  [`LICENSES/`](https://github.com/CooperBigFoot/shed/tree/main/LICENSES).
