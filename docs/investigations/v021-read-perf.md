# HFX v0.2.1 Read Performance Diagnosis

Date: 2026-06-05

## Executive Verdict

The dominant cause is open-time full-dataset work, not ineffective bbox pruning and not the
`graph.arrow` to `graph.parquet` change.

The expensive path is `DatasetSession::open()` doing whole-dataset referential validation before
any outlet-specific windowing can help. In current source, local open loads `graph.parquet`, opens
`catchments.parquet`, then `validate_graph_catchments()` calls:

- `catchments.read_all_ids()`
- `catchments.query_by_ids(&catchment_ids)`

Because `catchment_ids` is every unit, this selects every catchment row group. `query_by_ids()`
uses the full catchment projection, including `geometry`, so local open reads and decodes the
whole catchments file. See `crates/core/src/session.rs:247`, `crates/core/src/session.rs:249`,
`crates/core/src/session.rs:953`, `crates/core/src/session.rs:968`,
`crates/core/src/session.rs:972`, `crates/core/src/reader/catchment_store.rs:522`,
`crates/core/src/reader/catchment_store.rs:549`, and
`crates/core/src/reader/catchment_store.rs:1536`.

This defeats the HFX premise that a single small-basin delineation should pay only windowed reads
after bbox row-group pruning.

## Measurements

Datasets:

| Dataset | Path | Rows | `catchments.parquet` | `graph.parquet` |
|---|---:|---:|---:|---:|
| MERIT global local | `/Users/nicolaslazaro/Desktop/merit-hfx-v2/planetary/merit-hfx-global` | 2,876,771 | 6.1 GiB | 80 MiB |
| MERIT pfaf62 local | `/Users/nicolaslazaro/Desktop/merit-hfx-v2/tier2_5_pfaf62/merit-hfx-pfaf62` | 153,093 | 266 MiB | 4.3 MiB |
| MERIT 3-basin local | `/Users/nicolaslazaro/Desktop/merit-hfx-v2/tier2/merit-hfx-3basin` | 112,255 | 413 MiB | 3.2 MiB |

### Installed Python 0.2.0rc3

Measured with `crates/python/.venv/bin/python`, `pyshed.__version__ == 0.2.0rc3`,
extension at `crates/python/python/pyshed/_pyshed.abi3.so`.

Open-only, `pyshed.Engine(path, refine=False)`:

| Dataset | Iteration 0 | Iteration 1 | Result |
|---|---:|---:|---|
| MERIT global | 319,291 ms | 315,024 ms | no warm-cache improvement |
| MERIT pfaf62 | 13,639 ms | 13,192 ms | scales with dataset size |
| MERIT 3-basin | 17,950 ms | 17,886 ms | scales with dataset size/file weight |

These timings reproduce the observed local-global pain: open itself is dataset-size-bound.

### Current Rust Source Harness

`scripts/bench-delineate.sh --release`, current checkout `v0.1.163-1-gc00ed26`.

MERIT pfaf62 complete cold run, outlet `lat=-14.074999999999978 lon=-74.72`:

| Stage | Time |
|---|---:|
| catchment_id_index | 5.8 ms |
| catchment_store_open | 7.9 ms |
| validate_graph_catchments | 747.5 ms |
| snap_id_index / snap_store_open | 358.3 ms |
| validate_snap_refs | 2.0 ms |
| outlet_resolve | 9.5 ms |
| upstream_traversal | 0.004 ms |
| watershed_assembly | 0.6 ms |
| result_compose | 0.001 ms |
| total wall | 1,198 ms |

Warm fresh-engine pfaf62 runs were still 1,157 ms and 1,169 ms. Hot reuse of one engine was
53-54 ms per delineation. So per-delineation work is already small once the engine is open.

MERIT global current-source default run against Ticino failed later on the known
`AmbiguousD8Coverage`, but the open-side trace completed:

| Stage | Time |
|---|---:|
| catchment_id_index | 114.8 ms |
| catchment_store_open | 117.5 ms |
| validate_graph_catchments | 17,274 ms |
| snap_id_index / snap_store_open | 8,199 ms |
| validate_snap_refs | 95 ms |
| outlet_resolve | 31 ms |
| upstream_traversal | 0.01 ms |

The current source is far faster than the installed Python extension, but it still spends tens of
seconds in open-time global validation before the outlet-specific stages.

## H1 Verdict: Eager Open Is the Bottleneck

H1 is confirmed in spirit, but the most expensive current-source operation is not the id-column
scan alone.

The code does eagerly build a `UnitId -> row_group` index at open:
`crates/core/src/reader/catchment_store.rs:120` and `crates/core/src/reader/catchment_store.rs:153`.
It reads all row groups' `id` projection in `read_all_ids_with_row_groups_async()`
(`crates/core/src/reader/catchment_store.rs:1168` through
`crates/core/src/reader/catchment_store.rs:1221`). However measured current-source global
`catchment_id_index` was only 115 ms, and PyArrow read the full global `id` column in 30-64 ms.

The larger eager cost is `validate_graph_catchments()`: it calls `query_by_ids()` for all ids,
which selects all row groups and projects the full catchment schema, including `geometry`. That
is O(all units) and O(catchments file geometry weight) on every local open.

Local id-index caching is not reused. The local open path calls `CatchmentStore::open()` with
`id_index_path = None` (`crates/core/src/reader/catchment_store.rs:164` through
`crates/core/src/reader/catchment_store.rs:175`). Persistent id-index read/write only occurs
when both `id_index_path` and an ETag are present (`crates/core/src/reader/catchment_store.rs:1355`
and `crates/core/src/reader/catchment_store.rs:1396`). Tests explicitly assert remote cache files
exist and local opens do not allocate caches (`crates/core/src/session.rs:1874`,
`crates/core/src/session.rs:1888`, `crates/core/src/session.rs:2106`).

That matches the filesystem evidence:

- R2 cache has `/Users/nicolaslazaro/Library/Caches/hfx/grit/grit-global-2.0.0/catchments.idindex.arrow`
  at 261 MiB.
- Local `/Users/nicolaslazaro/Desktop/merit-hfx-v2/planetary/merit-hfx-global/` has no
  `catchments.idindex.arrow`.

## H2 Verdict: Bbox Pruning Is Effective for This Outlet

H2 is rejected for the Ticino outlet.

The HFX spec requires Hilbert sorting, 4,096-8,192 row groups, and bbox statistics
(`../hfx/spec/HFX_SPEC.md:87` through `../hfx/spec/HFX_SPEC.md:99`). MERIT global complies on
the inspected dimensions:

- 2,876,771 rows
- 702 row groups
- 4,097.96 average rows per group
- 0 row groups missing bbox stats

For outlet `lat=46.15299718384468 lon=8.800465726838459`, a point query and a small
0.01-degree bbox both matched only 2 of 702 row groups: 0.285% of row groups. The matching groups
were row groups 389 and 390.

So the outlet-specific catchments window should be tiny. The problem is that open-time validation
runs before the outlet-specific window.

## The `.arrow` Question

Keeping v0.1 `graph.arrow` would not have fixed this symptom.

For local MERIT global, `graph.parquet` is 84,338,714 bytes. PyArrow read all graph rows in
163 ms on the first measured read and 46-49 ms on subsequent reads. That is immaterial beside
315-319 s Python open time and still small beside current-source global validation/snap open.

The graph cutover happened with the HFX v0.2.1 loader at commit `5f2142e`
(`feat(core)!: cut over to HFX v0.2.1 contract and loader`). The relevant regression was not
the file extension; it was the v0.2.1 open path validating graph/catchment levels by querying all
catchment ids and reading full catchment rows.

## History

Key points from git and `clog`:

- `v0.1.34` loaded `graph.arrow`, opened catchments, and did referential integrity with
  `catchments.read_all_ids()` only. It did not call `query_by_ids(&all_ids)` at open.
- `v0.1.80` still used `graph.arrow` and the same id-only referential integrity shape.
- `5f2142e` (`feat(core)!: cut over to HFX v0.2.1 contract and loader`) introduced
  `graph.parquet` and the current `validate_graph_catchments()` shape that calls
  `query_by_ids(&catchment_ids)` for every catchment.
- `9f271cd` (`perf: optimize remote delineation hot path`) added the persistent ID index format
  and remote cache improvements. `clog` entries on 2026-05-06 record "Implement internal ID index
  cache format", "Wire remote ID indexes and validation sidecar", and "Verify Wave 2 persistent ID
  index sidecar".
- The D8 ambiguity is separate. `clog search "AmbiguousD8Coverage"` shows M4 entries on
  2026-06-04 documenting overlapping-Pfaf `AmbiguousD8Coverage` as a consumer-side limitation.

Conclusion: this is partly "always slow on large" because the old fast runs were small regional
datasets and the engine has had some O(N) open work for a long time. But the v0.2.1 cutover
introduced a worse O(N) full-catchments validation path. The old v0.1 read path was lazier in the
important sense that it did not read and decode every catchment row's full projection during open.

## Small-Dataset Options

R2 prefixes checked:

- `s3://basin-delineations-public/grit/1.0.0/` contains old v0.1 artifacts including
  `graph.arrow`.
- `s3://basin-delineations-public/grit/2.0.0/` is global GRIT v0.2.x:
  `catchments.parquet` is 32,508,030,585 bytes and `graph.parquet` is 699,720,490 bytes.
- `s3://basin-delineations-public/merit/0.2.0/` is global MERIT:
  `catchments.parquet` is 6,593,009,135 bytes and `graph.parquet` is 84,338,714 bytes.
- `s3://basin-delineations-public/merit-basins/0.1.0/` is not v0.2.1.

I did not find a small regional v0.2.1 R2 prefix.

Small local v0.2.1 development options:

- `/Users/nicolaslazaro/Desktop/merit-hfx-v2/tier2_5_pfaf62/merit-hfx-pfaf62`
  - 153,093 units
  - complete current-source run resolves at `--outlet -14.074999999999978,-74.72`
  - current-source hot delineation: 53-54 ms
  - current-source cold/warm fresh-engine: about 1.16-1.20 s
- `/Users/nicolaslazaro/Desktop/merit-hfx-v2/tier2/merit-hfx-3basin`
  - 112,255 units
  - larger total directory because of auxiliary rasters

## Recommended Fix

Owner: shed reader/session.

Smallest high-impact fix:

1. Remove full-row `query_by_ids(&all_ids)` from `validate_graph_catchments()`.
2. Validate graph membership with the already-built id set and validate level consistency from a
   narrow projection (`id`, `level`) or from graph-side data, not full catchment rows.
3. Make local open use a persistent validation sidecar/id-level index keyed by file size and
   mtime/hash. Do not require object ETag for local files.
4. Keep outlet-specific `query_by_bbox()` and `query_geometries_by_ids()` as the only paths that
   read geometry for a single delineation.

Secondary fix:

- Persist/reuse local `catchments.idindex.arrow` and `snap.idindex.arrow`. This helps, but by
  itself it does not solve the full-geometry validation read.

Writer-side HFX fix is not indicated for MERIT global row groups. The row groups are correctly
sized and bbox stats prune the Ticino outlet to 2/702 row groups.

Rough effort: one PCE milestone in shed, 1-2 focused days for a narrow `id+level` validation path
and local cache sidecar, plus regression benchmarks for global local open, regional cold/warm/hot,
and R2 cache behavior.
