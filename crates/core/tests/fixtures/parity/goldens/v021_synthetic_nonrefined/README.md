# v0.2.1 Synthetic Non-Refined Golden

This golden captures the offline refine-off watershed for
`crates/core/tests/fixtures/parity/v021_synthetic_refined/`.

It was captured before staged geometry movement in M3 Step 7, using the pre-M3
M2 engine with refinement disabled through `with_refine(false)` /
`RefinementMode::Disabled`. The capture used the same outlet and resolver
settings as the committed B oracle:

- input outlet: lon `2.5`, lat `-2.5`
- resolver search radius: `1000.0` metres
- resolution method: point-in-polygon
- refinement outcome: `Disabled`

Provenance:

- baseline: commit `dc65924`, tag/version `v0.1.122`
- current capture: commit `5f3d3cf`, version `0.1.127`
- canonicalizer: `shed-canonical-wkb-v1` at 6 decimal places
- comparison policy: coordinate epsilon `1e-6`, area absolute epsilon `1e-6`,
  area relative epsilon `1e-6`

The canonical WKB, terminal ID, sorted upstream IDs, resolved outlet, disabled
refinement outcome, and area were byte/scalar verified equal between the
baseline worktree at `dc65924` and the current Step 6 starting HEAD `5f3d3cf`
before this golden was committed.
