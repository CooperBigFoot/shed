# Large-Basin Delineation Is Too Slow

- Surface: `shed` CLI, `pyshed`, core engine
- Severity: high
- Dataset: `/Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu`

## Repro

1. Small outlet baseline:
   `time /tmp/shed-e2e-20260416/install/bin/shed --json delineate --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --lat 70.4521297483898 --lon 28.4906601273434`
2. Small batch baseline:
   `time /tmp/shed-e2e-20260416/install/bin/shed --json delineate --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --outlets /tmp/shed-e2e-20260416/outlets_valid_small.csv`
3. Large-basin stress case:
   `time /tmp/shed-e2e-20260416/install/bin/shed --json delineate --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --lat 48.7451234848118 --lon 44.5733577785127`
4. Large-basin with full GeoJSON write:
   `target/debug/shed --json delineate --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --lat 48.7451234848118 --lon 44.5733577785127 --output /tmp/shed-e2e-20260416/cli-single-volga.geojson`

## Expected

- Measured `hydra` baselines are now available in `scratchpad/benchmarks/20260416-hydra-cli-baseline.md`.
- This engine should not jump from sub-second small outlets to minute-scale large outlets without an obvious warning or progress signal.

## Observed

- Small outlet CLI run completed in `0.56 s`.
- Small batch of 4 outlets completed in `2.23 s` total, so Rayon is active and working.
- Large-basin Volga run stayed at `100%` CPU for more than `60 s` even with `--json` only and no GeoJSON file output, then was manually aborted.
- Large-basin debug run with `--output` did succeed, but it took well over a minute and produced a `32 MB` GeoJSON artifact.
- The large-basin pain is therefore not just file writing; the delineation path itself is too slow.
- Follow-up benchmark against `hydra`:
  - `shed` single-outlet medians on the benchmark set were `0.50 s`, `2.88 s`, and `29.03 s`
  - `shed` 300-outlet batch median was `921.62 s` (`3.072 s/outlet`)
  - `hydra merit` batch median on the same coordinates was `362.76 s` (`1.209 s/outlet`)
  - `hydra hydrosheds-v1` batch median was `13.48 s` (`0.045 s/outlet`)
  - Because the hydrofabrics differ, treat those numbers as rough calibration only, not a strict apples-to-apples performance ranking.
  - Directionally, the current `shed` batch path is about `2.5x` slower than `hydra merit` and about `68x` slower than `hydra hydrosheds-v1` on this benchmark.

## Artifacts

- `/tmp/shed-e2e-20260416/cli-single-small.json`
- `/tmp/shed-e2e-20260416/cli-single-small.stderr`
- `/tmp/shed-e2e-20260416/cli-batch-valid-small.json`
- `/tmp/shed-e2e-20260416/cli-batch-valid-small.stderr`
- `/tmp/shed-e2e-20260416/cli-single-volga.json`
- `/tmp/shed-e2e-20260416/cli-single-volga.geojson`
- `/tmp/shed-e2e-20260416/cli-single-volga-nooutput.json` and `.stderr` are empty because the run was aborted after staying slow for over a minute.

## Likely Layer

- Large-upstream geometry assembly, not session startup.
- `pyshed.Engine(dataset)` alone opens in about `0.048 s`, so `DatasetSession::open()` is not the main bottleneck here.
- More likely hot spots:
  - `CatchmentStore::query_by_ids()` scans the full catchments parquet for ID lookups: `crates/core/src/reader/catchment_store.rs`
  - `assemble_watershed()` decodes every upstream WKB geometry and dissolves them: `crates/core/src/assembly.rs`
  - topology repair / dissolve cost grows sharply for large upstream sets

## Next Fix Hint

- Profile a large-basin run around:
  - `query_by_ids`
  - `decode_wkb_multi_polygon`
  - `dissolve`
  - geometry repair
- If `query_by_ids` is a major share, add an ID index or row-group shortcut instead of full-file scans.
- If dissolve dominates, test a cheaper assembly path for already clean catchments or a staged dissolve strategy.
- Add progress or timing logs for long runs so the CLI is not silent for minute-scale delineations.
