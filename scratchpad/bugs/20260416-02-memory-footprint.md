# Memory Footprint Is High Even For Small Outlets

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
