# Parity Golden Artifact Contract

Milestone 1 parity goldens are loader-independent JSON records. Geometry truth is
the `canonical_wkb_hex` field: little-endian 2D WKB emitted by
`shed-core::algo::canonical_wkb_multi_polygon`.

## Canonicalizer

- `canonicalizer_version`: `shed-canonical-wkb-v1`
- Coordinate precision: 6 decimal places (`CANONICAL_WKB_DECIMAL_PRECISION = 6`)
- Coordinate absolute epsilon: `0.000001`
- Ring closure: explicit first vertex repeated as last
- Ring orientation: exterior rings are CCW; interior rings are CW
- Ring start vertex: lexicographically smallest rounded `(x, y)`; duplicate
  rounded coordinates are tied by the full adjacent cyclic vertex sequence
- Hole order: normalized ring bbox, signed area, full rounded vertex sequence
- Polygon/component order: normalized exterior bbox, polygon area, hole count,
  full rounded exterior sequence, then full rounded hole sequences
- Antimeridian-crossing geometries are out of scope for M1 because the selected
  A/B/C outlets are far from +/-180 degrees.

The 6-decimal precision is intentionally coarser than normal f64 operation
noise. M1 goldens require pre-rounding coordinate divergence to remain below
`1e-9` degrees, giving at least a 500x margin below the `5e-7` degree half-step
where a rounded coordinate could flip. Changing this precision changes the
canonicalizer version and invalidates captured goldens.

## Golden Record Fields

- `canonical_wkb_hex`: hex-encoded canonical final geometry WKB
- `area_km2`: scalar area compared with epsilon policy, not byte-exact equality
- `input_outlet`: original outlet coordinate
- `resolved_outlet`: resolved outlet coordinate
- `refined_outlet`: refined outlet coordinate, present only when refinement
  outcome is `Applied`
- `terminal_id`: version-neutral terminal identifier as `i64`
- `upstream_ids`: sorted version-neutral upstream identifier set as `Vec<i64>`
- `resolution_method`: outlet resolution method label
- `resolver_config`: resolver settings, including `search_radius_m`
- `refinement_outcome`: refinement status and optional reason
- `canonicalizer_version`: canonicalizer contract version
- `comparison_policy`: coordinate absolute epsilon plus `area_km2`
  absolute/relative epsilon tied to canonical WKB precision
