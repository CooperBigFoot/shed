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

See [`crates/python/README.md`](crates/python/README.md) for the full Python
API.

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
