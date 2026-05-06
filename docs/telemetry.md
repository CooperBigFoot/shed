# Telemetry

Phase A/B telemetry is intended for benchmark harnesses that need stable stage
timings and coarse remote-I/O counters without depending on internal Rust APIs.
The delineation harness is documented in
[`docs/benchmarks/delineate-harness.md`](benchmarks/delineate-harness.md).

## Enablement

| Variable | Effect |
|---|---|
| `PYSHED_BENCH_TRACE` | Emits JSONL stage records for benchmark consumers. |
| `PYSHED_BENCH_NET=1` | Enables aggregate object-store network counters. |

Network counters are aggregate process counters. They are useful for comparing
whole benchmark runs, but they are not per-span attribution and should not be
joined to individual stages as exact network cost.

## JSONL Records

Each JSONL line is an event object. Benchmark harnesses should parse these
fields:

| Field | Required | Meaning |
|---|---:|---|
| `kind` | yes | Record kind. Stage records use `stage`. |
| `stage` | yes | Stable lower_snake_case stage name from the table below. |
| `duration_ms` | yes | Stage wall time in milliseconds. |
| `bytes` | optional | Bytes recorded on the stage span. |
| `requests` | optional | Request count recorded on the stage span. |
| `cache_status` | optional | Cache outcome, for example `hit`, `miss`, or `wait`. |
| `path` | optional | File, object, or cache path associated with the stage. |
| `timestamp` | yes | Unix timestamp in milliseconds when the stage record was emitted. |
| `thread` | optional | Thread metadata if the tracing layer includes it. |

Consumers should ignore unknown fields.

Benchmark harnesses may augment base stage records. `bench_delineate` adds
`iteration` and `iteration_wall_time_ms` to copied stage records. The base
telemetry layer does not emit iteration fields unless a harness adds them.

## Stages

| Rust stage | Stable name |
|---|---|
| `RemoteOpen` | `remote_open` |
| `ManifestFetch` | `manifest_fetch` |
| `GraphFetch` | `graph_fetch` |
| `CatchmentStoreOpen` | `catchment_store_open` |
| `CatchmentIdIndex` | `catchment_id_index` |
| `SnapStoreOpen` | `snap_store_open` |
| `SnapIdIndex` | `snap_id_index` |
| `ValidateGraphCatchments` | `validate_graph_catchments` |
| `ValidateSnapRefs` | `validate_snap_refs` |
| `OutletResolve` | `outlet_resolve` |
| `UpstreamTraversal` | `upstream_traversal` |
| `TerminalRefine` | `terminal_refine` |
| `TerminalCatchmentFetch` | `terminal_catchment_fetch` |
| `RasterLocalizeFlowDir` | `raster_localize_flow_dir` |
| `RasterLocalizeFlowAcc` | `raster_localize_flow_acc` |
| `CogPrepareWindow` | `cog_prepare_window` |
| `CogFetchTiles` | `cog_fetch_tiles` |
| `RasterCacheLookup` | `raster_cache_lookup` |
| `WatershedAssembly` | `watershed_assembly` |
| `ResultCompose` | `result_compose` |

Stage taxonomy invariant: validation stages must not include ID-index scans.
`CatchmentIdIndex` and `SnapIdIndex` cover index open/build/load work.
`ValidateGraphCatchments` and `ValidateSnapRefs` cover only membership checks, or
are skipped entirely when the validated sidecar matches current artifact
metadata.

## Stability

The stable stage names and core field names above are part of the benchmark
contract. Additive fields may appear over time; benchmark harnesses should keep
parsers permissive and key off `kind`, `stage`, and `duration_ms` for stage
timing summaries.
