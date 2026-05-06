# Delineation Benchmark Harness

`bench_delineate` is diagnostic-only infrastructure for measuring existing
delineation behavior. It does not change engine algorithms, cache policy,
parallelism, or public API behavior.

## Modes

| Mode | Meaning |
|---|---|
| `cold` | Uses a unique empty `HFX_CACHE_DIR` for every measured iteration. |
| `warm` | Populates the run cache once, then measures fresh `Engine` instances against that cache. This v1 harness is same-process warm, not true process isolation. |
| `hot` | Opens one `DatasetSession` and reuses one `Engine` for repeated delineations in the same process. |

`--cache-dir` defaults to a temp/scratch parent under `std::env::temp_dir()`.
The harness always sets `HFX_CACHE_DIR` to a run-specific child directory and
never falls back to user `~/.cache/hfx`.

The harness does not enable `parquet_cache`; it opens datasets through
`DatasetSession::open` so timings reflect the engine's existing default
behavior.

## Examples

Canonical remote runs:

```bash
scripts/bench-delineate.sh --release --measure-rss --mode cold --dataset r2 --outlet zurich --iterations 3 --out scratchpad/benchmarks/cold-r2-zurich.jsonl
scripts/bench-delineate.sh --release --measure-rss --mode cold --dataset r2 --outlet hammerfest --iterations 3 --out scratchpad/benchmarks/cold-r2-hammerfest.jsonl
scripts/bench-delineate.sh --release --measure-rss --mode warm --dataset r2 --outlet zurich --iterations 5 --out scratchpad/benchmarks/warm-r2-zurich.jsonl
scripts/bench-delineate.sh --release --measure-rss --mode hot --dataset r2 --outlet zurich --iterations 10 --out scratchpad/benchmarks/hot-r2-zurich.jsonl
```

Local fixture smoke:

```bash
cargo run -p shed-core --features test-fixtures --bin bench_delineate -- \
  --mode hot --dataset local --outlet 0,0 --iterations 1 --out /tmp/shed-local-bench.jsonl
```

`--dataset r2` expands to
`https://basin-delineations-public.upstream.tech/grit/1.0.0/`. Any other
dataset value is passed directly to `DatasetSession::open`.

## JSONL Output

The output file contains:

| Record | Contents |
|---|---|
| `header` | Dataset, mode, outlet, iteration count, cache directory, and harness version. |
| `stage` | Stage records copied from `PYSHED_BENCH_TRACE`, augmented with `iteration` and `iteration_wall_time_ms`. |
| `iteration` | Per-iteration wall time and HTTP counters when `DatasetSession::http_stats()` is available. |
| `summary` | Min, median, p95, and max wall time across measured iterations. |

During measured iterations the harness sets `PYSHED_BENCH_TRACE` to a temporary
per-iteration trace file and `PYSHED_BENCH_NET=1` so stage spans and remote
object-store counters are captured.

Current limitation: `DatasetSession::http_stats()` is accessible before the
session is moved into `Engine`, but not after delineation through the public
`Engine` API. The v1 harness therefore relies on stage records for network
context and leaves per-iteration HTTP summaries absent.

## Comparison

Use the stdlib-only comparison script:

```bash
python3 scripts/compare-bench.py baseline.jsonl candidate.jsonl
python3 scripts/compare-bench.py baseline.jsonl candidate.jsonl --gates gates.json
```

Gate files use a simple JSON shape:

```json
{
  "max_wall_pct_regression": 10.0,
  "max_pct_regression_by_stage": {
    "watershed_assembly": 15.0,
    "*": 25.0
  }
}
```
