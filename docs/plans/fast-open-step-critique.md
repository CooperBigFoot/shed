# Adversarial Critique — Fast-Open Step Plan

Reviewer: adversarial critic. Verified against current `main` (`c00ed26`).

This document has three parts: the **Final verdict** (round 3), the **Re-review**
(round 2), and the **Round-1 critique** (the original SEND BACK, retained for
provenance).

---

# FINAL VERDICT (round 3)

Both round-2 minors were folded into the plan and verified in source:

- **MINOR-A:** Step 2 now guards the test-only `path_display` parameter with
  `#[cfg_attr(not(test), allow(unused_variables))]` on `extract_units_from_batch()`
  (`fast-open-step-plan.md:81`), keeping `cargo clippy --workspace -- -D warnings` green
  without cfg-splitting the `:495`/`:587` call sites. Exactly the recommended fix.
- **MINOR-B:** Step 5 now requires confirming the harness accepts an absolute
  `--dataset` path via a one-iteration interface check before quoting medians
  (`fast-open-step-plan.md:189`).

No new defects introduced; no production code or goldens touched. Every axis is PASS and
no open items remain.

## VERDICT: **DISPATCH AS-IS.** Executors may proceed.

---

# RE-REVIEW (round 2)

The planner addressed the round-1 SEND BACK. I re-opened the source to verify each
revision independently rather than trust the changelog.

## The blocking finding is resolved

Round 1 killed the plan because the only CI-durable gate was **vacuous**: the
`geometry_decode_count_for_test` counter is incremented solely in the
`query_geometries_by_ids` path (`catchment_store.rs:1117`), never in the
`query_by_ids` → `extract_units_from_batch` path that validation actually uses, so a
"zero decodes after open" assertion read 0 on broken and fixed code alike.

The revised plan fixes this correctly (new Step 2 + Step 3):

- **Instrumentation is on the real validation path.** Step 2 adds
  `#[cfg(test)] record_geometry_decode_for_test(...)` at the geometry materialization
  in `extract_units_from_batch` (`catchment_store.rs:916` `geom_bytes = …to_vec()`,
  `:928` `WkbGeometry::new`). Verified that is exactly where per-row geometry bytes are
  materialized, and that this function is the one `query_by_ids` calls (`:587`). So on
  HEAD, `open_path` → `validate_graph_catchments` → `query_by_ids(&all_ids)` will trip
  the counter for every unit.
- **Red-on-bug is now mandatory and ordered.** Step 2 (instrument + prove the counter
  fires on the broken route) lands *before* Step 3 (reroute validation to
  `read_id_levels`). The plan explicitly requires demonstrating the final zero
  assertion is red on the pre-routing state, then green after. That makes the gate
  genuinely gate the regression — the round-1 defect.
- **Post-fix zero is real, not coincidental.** I confirmed the only two call sites of
  `extract_units_from_batch` are `query_by_bbox` (`:495`) and `query_by_ids` (`:587`).
  Neither runs during `open_path` once validation switches to `read_id_levels`, so
  "zero full-row geometry materializations during open" is a true post-fix invariant,
  not a fixture artifact.
- **Crate-internal placement is now correct (round-1 G1).** Step 2 explicitly puts the
  test in a crate-internal `#[cfg(test)]` module because the hooks are
  `#[cfg(test)] pub(crate)` (`catchment_store.rs:702`, `:1137`) and invisible to
  `tests/`. Matches the visibility reality I flagged.

## Round-1 secondary gaps — all addressed

- **G2 (sidecar-hit untested):** New Step 4 drives a second remote open through the
  sidecar-hit branch and asserts zero full-row geometry materializations. Verified the
  test it builds on is real: `second_remote_open_uses_persistent_indexes_and_validation_sidecar`
  (`session.rs:2078`) already does two `open_remote` calls, asserts `validated.json`
  exists, and uses a `CountingStore` to compare ranged gets — a sound base to extend
  with the geometry-read counter. Good.
- **M1 (don't thread id list):** Step 1 now scans all row groups with the `[id, level]`
  projection directly. Correct — `selected_row_groups_for_ids(all)` returns every group
  anyway (`catchment_store.rs:717`).
- **M2 (keep ordered Vec + HashSet):** Step 3 explicitly preserves both the ordered
  `catchment_ids: Vec` for the reverse check (`session.rs:1040-1041`) and the `HashSet`
  for membership (`session.rs:988`, `:1012`). Correct.
- **M3 (perf targets non-binding):** §5 now states the MERIT thresholds are manual
  acceptance targets and CI is protected by the crate-internal geometry-materialization
  tests. Correct framing.

## Remaining items — one new minor, none blocking

**MINOR-A (new, concrete): the instrumentation will trip the `-D warnings` gate unless
guarded.** Step 2 adds a `path_display: &str` parameter to `extract_units_from_batch`
whose only consumer is the `#[cfg(test)]` counter call. In the default
`cargo clippy --workspace -- -D warnings` build (Step 5 gate), `#[cfg(test)]` code is
stripped, leaving `path_display` unused — and Rust's `unused_variables` lint fires on
unused **function parameters**, so `-D warnings` fails. The existing counter call site
doesn't hit this because it reads `context.path_display`, a struct field used elsewhere;
the new parameter has no non-test use. Minimum fix, pick one:
  - `#[cfg_attr(not(test), allow(unused_variables))]` on `extract_units_from_batch`
    (simplest, local), or
  - cfg the parameter itself (`#[cfg(test)] path_display: &str`) and cfg the argument at
    both call sites (`:495`, `:587`) — more invasive.
  Flagging so the implementer pre-empts it rather than discovering it at the Step 5 gate.

**MINOR-B (carryover): confirm the bench dataset-path form.** Step 5's commands pass
`--dataset <abs-path>`; `scripts/bench-delineate.sh:8` documents named/`r2` datasets.
Confirm the local-path form is accepted before quoting before/after medians. Trivial.

**Note (not a defect): signature touch on the hot path is non-behavioral.** Adding
`path_display` to `extract_units_from_batch` changes the `query_by_bbox` call site
(`:495`) signature, but production behavior is byte-identical (the only new consumer is
cfg(test)). Hot-path parity and goldens are preserved; Step 5 still gates
`parity_golden_artifacts`, `d8_refinement_parity`, `staged_delineation`. Acceptable.

## Axis-by-axis (round 2)

| Axis | Result |
|---|---|
| 1. Detection genuinely preserved | **PASS** — same checks; level sourced from catchments; existing `open_path` tests guard it. |
| 2. All three call sites fixed | **PASS** — Step 3 covers validate + sidecar-hit `463-465`. |
| 3. Perf gate real and non-vacuous | **PASS** — counter now on the validation path; red-on-bug ordered before the fix; CI-durable, dataset-independent. |
| 4. Hot path / parity untouched | **PASS** — instrumentation is cfg(test); no projection/golden change. |
| 5. Correctness of level source | **PASS** — catchment `level`, spec-mandated non-null. |
| 6. Scope / doctrine / version | **PASS** — five steps, each bump+tag; pyshed untouched; surgical. |

## VERDICT (round 2)

**DISPATCH WITH MINORS.**

The plan is sound and the round-1 blocker is fully resolved: the regression gate now
instruments the path validation actually executes (`extract_units_from_batch:916-928`),
is required to be demonstrated red on HEAD before validation is rerouted, and is a
crate-internal `#[cfg(test)]` test that needs no large dataset. All three call sites
(including the remote sidecar-hit at `session.rs:463-465`) are fixed and the sidecar-hit
path now has its own coverage. Executors may proceed; fold in the two minors:

1. **MINOR-A:** guard the new `path_display` param against `unused_variables` (e.g.
   `#[cfg_attr(not(test), allow(unused_variables))]`) so Step 5's `-D warnings` gate
   stays green.
2. **MINOR-B:** confirm `bench-delineate.sh` accepts the absolute `--dataset` path form
   before recording perf numbers.

---

# ROUND-1 CRITIQUE (superseded — retained for provenance)

Original verdict: **SEND BACK**, on a single blocking defect plus two secondary gaps.

**Blocking (resolved in round 2):** the CI-durable zero-geometry-decode gate was
vacuous. `record_geometry_decode_for_test` is incremented only at
`catchment_store.rs:1117` inside the `query_geometries_by_ids` path; the validation
path (`query_by_ids` → `extract_units_from_batch`, `:747-949`, geometry materialized at
`:916-929`) never touched the counter. So "zero decodes after open" passed on HEAD (the
6.1 GiB read) and post-fix alike — gating nothing. With the Step-4/5 benchmark explicitly
non-reproducible in CI, that left the fix with no durable regression guard.

**Round-1 G1 (resolved):** Step 3's preferred integration-test placement was infeasible —
`geometry_decode_count_for_test`/`reset_*` are `#[cfg(test)] pub(crate)` and invisible to
`crates/core/tests/`; the test had to be crate-internal.

**Round-1 G2 (resolved):** the remote sidecar-hit fix (`session.rs:463-465`) had no
regression coverage; the local `open_path` test and the stage-timed benchmark both miss
that branch.

**What round 1 already confirmed sound (unchanged):** the diagnosis; all three call sites
correctly identified incl. sidecar-hit; catchment-`level` source backed by
`HFX_SPEC.md:62-63,171-173`; detection logic survives the projection narrowing; the five
detection tests genuinely exercise `open_path`; `write_catchments` fixtures already carry
a non-null `level` so the strict reader won't mis-fire; hot path/goldens untouched;
doctrine and per-step version discipline respected.
