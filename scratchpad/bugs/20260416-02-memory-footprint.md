# Memory Footprint Is High Even For Small Outlets

- Status: closed / resolved for Phase 3b.7 close-out
- Closed: 2026-04-24
- Final baseline: [`ci/rss-baseline.json`](../../ci/rss-baseline.json)

- Surface: `shed` CLI, `pyshed`
- Severity: medium
- Dataset: `/Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu`

## Repro

1. CLI single small outlet:
   `time /tmp/shed-e2e-20260416/install/bin/shed --json delineate --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --lat 70.4521297483898 --lon 28.4906601273434`
2. CLI small batch:
   `time /tmp/shed-e2e-20260416/install/bin/shed --json delineate --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --outlets /tmp/shed-e2e-20260416/outlets_valid_small.csv`
3. Python single small outlet:
   `uv run python /tmp/shed-e2e-20260416/pyshed_probe.py --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --mode single --payload '{"lat": 70.4521297483898, "lon": 28.4906601273434}'`
4. Python small batch:
   `uv run python /tmp/shed-e2e-20260416/pyshed_probe.py --dataset /Users/nicolaslazaro/Desktop/grit-hfx/grit-hfx-eu --mode batch --payload '[{"lat": 70.4521297483898, "lon": 28.4906601273434}, {"lat": 70.4479351627286, "lon": 24.3580998300224}, {"lat": 70.4400138994510, "lon": 24.4139580487914}]'`

## Expected

- Small outlets should not need several hundred MB of peak RSS just to return one polygon and a few metadata fields.

## Observed

- CLI single small outlet peak memory footprint: about `271 MB`
- CLI small batch of 4 peak memory footprint: about `530 MB`
- CLI mixed small batch with partial GeoJSON output: about `576 MB`
- Python single small outlet peak memory footprint: about `419 MB`
- Python batch of 3 small outlets peak memory footprint: about `820 MB`

## Artifacts

- `/tmp/shed-e2e-20260416/cli-single-small.stderr`
- `/tmp/shed-e2e-20260416/cli-batch-valid-small.stderr`
- `/tmp/shed-e2e-20260416/cli-batch-mixed-small.stderr`
- `/tmp/shed-e2e-20260416/py-single-small.stderr`
- `/tmp/shed-e2e-20260416/py-batch-valid-small.stderr`

## Likely Layer

- Multiple in-memory geometry copies during assembly:
  - parquet rows
  - `HashMap<AtomId, CatchmentAtom>`
  - decoded `MultiPolygon`s
  - dissolve / repair intermediates
- Python results also inflate memory further when callers request GeoJSON strings.

## Next Fix Hint

- Measure memory around:
  - `query_by_ids`
  - `index_catchments_by_id`
  - `decode_wkb_multi_polygon`
  - `assemble_from_geometries`
- Reduce intermediate duplication where possible.
- Consider a leaner result path for Python callers that only need WKB or area metadata.

## Phase 3b.7 Close-Out

### Before Context

- The original repros above showed small real-outlet runs peaking around:
  - CLI single small outlet: `271 MB`
  - CLI small batch of 4: `530 MB`
  - CLI mixed small batch with partial GeoJSON output: `576 MB`
  - Python single small outlet: `419 MB`
  - Python batch of 3: `820 MB`
- The likely source was duplicate geometry materialization across parquet rows,
  catchment indexing, decoded multipolygons, dissolve/repair intermediates, and
  Python GeoJSON result strings.

### Implemented Changes

- Geometry rows are decoded while indexing catchments instead of retaining extra
  row-level copies.
- Geometry decoding now reads directly from Arrow buffers in the hot path.
- Terminal refinement decode behavior is covered by a regression test that
  guards against repeated full decode passes.

### Final Measurement

Command:

```bash
scripts/measure-rss.sh --bin target/debug/examples/bench_delineation
```

Output:

```text
--- command stdout ---
{"atoms":2500,"coords_per_ring":1500,"elapsed_ms":36872,"area_km2":1107499.9686472395,"polygon_count":2500,"wkb_bytes":60032509}
--- command stderr and time output ---

       37.48 real       187.73 user         1.92 sys
time: sysctl kern.clockrate: Operation not permitted
{"command":"target/debug/examples/bench_delineation","exit_status":0,"max_rss_bytes":null}
```

The benchmark completed successfully on `v0.1.86`
(`6bd30f524f012c0b64133762ad9aa95f9f618029`) with the default synthetic
configuration of `2500` atoms and `1500` coordinates per ring. The final RSS
baseline records `max_rss_bytes: null` because this macOS sandbox reports:

```text
time: sysctl kern.clockrate: Operation not permitted
```

### Remaining Caveats

- This close-out does not claim a numeric final RSS on this machine. The harness
  ran successfully, but `/usr/bin/time -l` could not expose max RSS in the
  sandboxed Darwin environment.
- CI is unchanged. An RSS regression job would be useful once a stable Linux
  runner baseline and threshold are established; adding one from this macOS
  `null` measurement would not be a robust gate.
