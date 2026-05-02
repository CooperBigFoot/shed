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
Linux, Intel macOS, and Windows wheels are not yet built — community
contributions are welcome. See
[CONTRIBUTING.md](https://github.com/CooperBigFoot/shed/blob/main/CONTRIBUTING.md)
if you want to help port the build.

## Quickstart

```python
import pyshed

engine = pyshed.Engine("/path/to/hfx/dataset")
result = engine.delineate(lat=47.3769, lon=8.5417)
print(result.area_km2)
```

Snapping options belong on the **constructor**, not on `delineate`:

```python
# Correct — snap_radius is an Engine constructor kwarg
engine = pyshed.Engine("/path/to/hfx/dataset", snap_radius=5000)
result = engine.delineate(lat=47.3769, lon=8.5417)
```

`Engine` also accepts dataset root URLs backed by the object-store integration:

```python
local_engine = pyshed.Engine("/data/hfx/rhine")
file_url_engine = pyshed.Engine("file:///data/hfx/rhine")
s3_engine = pyshed.Engine("s3://bucket/path/to/hfx/rhine")
r2_engine = pyshed.Engine(
    "https://<account>.r2.cloudflarestorage.com/<bucket>/path/to/hfx/rhine"
)
public_r2_engine = pyshed.Engine(
    "https://basin-delineations-public.upstream.tech/global/hfx"
)
```

Remote dataset sessions cache `manifest.json` and `graph.arrow` under
`~/.cache/hfx/<fabric_name>/<adapter_version>/` by default. Set
`HFX_CACHE_DIR=/path/to/cache` before constructing `pyshed.Engine(...)` to use a
different cache root. Parquet artifacts are read with object-store range reads;
they are not copied into the cache wholesale.

GDAL raster URI and configuration plumbing is wired through the Python engine,
but public Cloudflare R2 raster access still depends on the target bucket,
credentials, and GDAL driver behavior. Verify the specific remote raster dataset
you plan to use.

### Verbose mode

Enable structured log output from both the Python and Rust layers:

```python
import pyshed

pyshed.set_log_level("info")
engine = pyshed.Engine("https://basin-delineations-public.upstream.tech/global/hfx")
# INFO lines stream during manifest/graph/catchment loading
result = engine.delineate(lat=47.3769, lon=8.5417)
```

Valid levels: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`. In a Jupyter
notebook `"info"` is enabled automatically on import.

### Speeding up repeated delineations

Enable the in-memory Parquet column-chunk cache to avoid redundant range reads
across overlapping watersheds:

```python
engine = pyshed.Engine(
    "https://basin-delineations-public.upstream.tech/global/hfx",
    parquet_cache=True,
    parquet_cache_max_mb=512,
)
```

The cache is off by default. `parquet_cache_max_mb` defaults to `2048` when
`parquet_cache=True`. Cache state is per-`Engine` instance and is not persisted
to disk.

### Batch delineation with progress

```python
import pyshed

# tqdm is a user dependency — not bundled with pyshed
from tqdm.auto import tqdm

url = "https://basin-delineations-public.upstream.tech/global/hfx"
engine = pyshed.Engine(url)

outlets = [
    {"lat": 47.3769, "lon": 8.5417},
    {"lat": 46.9480, "lon": 7.4474},
    {"lat": 48.1351, "lon": 11.5820},
]

bar = tqdm(total=len(outlets), unit="outlet")

def on_progress(event):
    bar.update(1)
    bar.set_postfix(status=event.get("status"), ms=event.get("duration_ms"))

results = engine.delineate_batch(outlets, progress=on_progress)
bar.close()
```

The `progress` callback receives a dict with keys `index`, `total`, `lat`,
`lon`, `duration_ms`, `status` (`"ok"` or `"error"`), plus `n_catchments` on
success and `error` on failure. Exceptions raised inside the callback are
swallowed and logged; they do not interrupt the batch.

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
