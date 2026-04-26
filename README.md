# shed

The watershed extraction engine that consumes compiled
[HFX](https://github.com/CooperBigFoot/hfx) datasets and returns watershed
polygons for any `(lat, lon)` outlet.

`shed` is fabric-agnostic by design: it reads the open HydroFabric Exchange
contract (`manifest.json`, `catchments.parquet`, `graph.arrow`, plus optional
`snap.parquet`, `flow_dir.tif`, and `flow_acc.tif`) and runs outlet
resolution, upstream traversal, optional terminal raster refinement, and
final geometry assembly without any source-fabric-specific logic in the hot
path. The same engine works for any HFX-compliant dataset.

## Use it from Python

The Python wrapper [`pyshed`](https://pypi.org/project/pyshed/) is published
on PyPI as a self-contained wheel with GDAL, PROJ, GEOS, and friends bundled
inside — no system dependencies required.

```bash
pip install pyshed   # macOS arm64 only in v0.1 — see CONTRIBUTING.md
```

```python
import pyshed

engine = pyshed.Engine("/path/to/hfx/dataset")
result = engine.delineate(lat=47.3769, lon=8.5417)

print(result.area_km2)        # geodesic area in km²
print(result.terminal_atom_id)
geojson = result.to_geojson()
```

See [`crates/python/README.md`](crates/python/README.md) for the Python
quickstart and [`crates/python/API.md`](crates/python/API.md) for the full
developer API reference.

## Dataset locations

`shed` accepts local HFX dataset directories and remote object-store URLs for
the dataset root. The root must contain the HFX artifacts described by the
manifest: `manifest.json`, `catchments.parquet`, `graph.arrow`, and optional
`snap.parquet`, `flow_dir.tif`, and `flow_acc.tif`.

Supported dataset path forms:

| Form | Example |
|---|---|
| Local directory | `/data/hfx/rhine` |
| Local file URL | `file:///data/hfx/rhine` |
| Amazon S3 URL | `s3://bucket/path/to/hfx/rhine` |
| Cloudflare R2 HTTPS URL | `https://<account>.r2.cloudflarestorage.com/<bucket>/path/to/hfx/rhine` |
| Public R2 custom-domain URL | `https://basin-delineations-public.upstream.tech/global/hfx` |

For remote datasets, `manifest.json` and `graph.arrow` are cached locally under
`~/.cache/hfx/<fabric_name>/<adapter_version>/` by default. Set
`HFX_CACHE_DIR=/path/to/cache` to override the cache root. Parquet artifacts are
read through object-store range reads instead of being downloaded wholesale.

Raster URI and GDAL configuration plumbing is present for remote dataset
sessions. Treat public R2 raster access as environment-dependent until a
dataset-specific smoke test has verified the target bucket, credentials, and
GDAL driver behavior.

### Canonical hosted dataset

The canonical public dataset for smoke tests and examples is MERIT-Basins
global v0.1.0:

```text
https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/
```

CLI example:

```bash
./target/release/shed delineate \
    --dataset https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/ \
    --lat 47.3769 --lon 8.5417
```

Python example:

```python
import pyshed

engine = pyshed.Engine(
    "https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/"
)
result = engine.delineate(lat=47.3769, lon=8.5417)
print(result.area_km2)
```

Cold-cache smoke results, to be filled in Phase 5:

```text
<bytes-on-wire> | <peak-RSS> | <wall-time>
```

## Use it from the CLI

```bash
git clone https://github.com/CooperBigFoot/shed
cd shed
cargo build --release

# Single outlet
./target/release/shed delineate --dataset /path/to/hfx \
    --lat 47.3769 --lon 8.5417

# Batch via CSV
./target/release/shed delineate --dataset /path/to/hfx \
    --outlets outlets.csv --output watersheds.geojson
```

`shed delineate --help` for all flags (snap radius, accumulation threshold,
`--no-refine`, `--json` envelope, etc.).

## Repository layout

| Path | Purpose |
|---|---|
| `crates/core` | Pure-Rust algorithm core (HFX I/O, traversal, dissolve, repair) |
| `crates/gdal` | GDAL bridge for windowed raster reads + GEOS geometry repair |
| `crates/python` | PyO3 bindings, published on PyPI as `pyshed` |
| `src/main.rs` | The `shed` CLI binary |
| `ci/`, `.github/` | macOS arm64 wheel build pipeline (cibuildwheel + bespoke native stack) |
| `scripts/` | Version-bump helpers — see `CLAUDE.md` for the workflow |

## Contributing

Build instructions, coding conventions, and the open call for community
wheel contributions (Linux / Intel macOS / Windows) live in
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

`shed` and `pyshed` are MIT-licensed (see [`LICENSE`](LICENSE)). Bundled
native libraries in the published wheel retain their own licenses; see
[`THIRD_PARTY_LICENSES.md`](THIRD_PARTY_LICENSES.md) and the per-library
texts in [`LICENSES/`](LICENSES/).
