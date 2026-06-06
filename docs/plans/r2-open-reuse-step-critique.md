# Adversarial Critique — R2 Open Reuse Step Plan (Re-review)

Reviewer role: adversarial critic (read-only on source/tests/config).
Date: 2026-06-06 (revision 2)
Target: `docs/plans/r2-open-reuse-step-plan.md`
Verified against: `crates/core/src/{session.rs, cache.rs}`,
`crates/core/src/reader/{catchment_store.rs, snap_store.rs, graph.rs, id_index.rs}`,
and tests in `crates/core/tests/{graph_parquet_reader.rs, session_open.rs, snap_aux_reader.rs}`.

## Verdict

**DISPATCH AS-IS** (revision 3) — both prior blocking defects are resolved and all minors,
including the rev-2 sequencing nit, are folded into the plan. No outstanding issues.

Rev-3 confirmation (verified against the plan + code):

- M-A closed: the snap warm-skip proof now lives in Step 2 (plan line 133), tied to the real
  path at `session.rs:479-481`, with the gate covering both counters (line 148). Step 5
  explicitly disclaims a warm-skip proof and defers to Step 2 (line 233); its gates no longer
  assert a fail-before/pass-after test (lines 245-247). The proof now genuinely
  fails-before/passes-after the gate removal that changes behavior.
- M-B closed: Step 5 keeps its patch bump + commit + tag (plan lines 251-254).
- HEAD-not-GET folded in for manifest/graph token metadata (plan lines 66, 141), so warm open
  pays at most two cheap metadata round-trips, not extra full downloads.

The detail below is retained as the rev-2 record.

---

### (rev 2) DISPATCH WITH MINORS — both prior blocking defects resolved; one sequencing nit + two cheap fold-ins (now all closed in rev 3).

---

## Blocking items from revision 1 — both CLOSED

### BLOCKING 1 (graph not in token) — CLOSED

The token now attests the full validated relationship:

- Token fields add `manifest` and `graph` path+ETag+size (plan lines 56-57).
- Match rule fails closed if manifest/graph ETags are missing (plan line 72).
- Invalidation lists manifest and graph (plan lines 76-77).
- The exact graph-only-swap-after-eviction scenario I raised is now the named proof
  obligation (plan line 86) and a dedicated Step 3 regression test (plan line 169): evict the
  cached graph, leave `validated.json`, mutate only `graph.parquet`, assert
  `validate_graph_catchments` re-runs.
- Step 2 calls out the required plumbing — capture ETag+size for manifest and graph even when
  the parsed artifacts come from the local cache path at `session.rs:345-354`, and refuse to
  match without them (plan lines 66, 141).

This is sound. I confirmed the warm-skip of `validate_snap_refs` is also covered: snap
validation reads `decl.references_levels` (from the manifest), the catchment id-set/levels,
and the snap artifact itself — all three are now token inputs, so skipping snap validation on
a token match no longer trusts anything unattested.

One feasibility note (not blocking): on the cache-hit open path the graph/manifest arrive
from local disk with no remote metadata, so the implementation must issue HEADs on
`manifest.json` and `graph.parquet` to obtain ETag+size for matching — two added round-trips
on warm open (~tens of ms, negligible vs. the win). The plan's "must capture … if it cannot
obtain … the token must not match" language already mandates the fail-closed behavior; just
make sure the executor uses HEAD (mirroring the catchments store's `head_meta` at
`catchment_store.rs:414`), not a full GET, to keep warm open cheap.

### BLOCKING 2 (Step 5 dropped snap geometry/stem_role detection) — CLOSED

Step 5 is reframed from "drop geometry" to "guard and measure without weakening detection":

- Cold/invalidated opens keep the `geometry` projection, `geometry_from_array` +
  `validate_snap_geometry`, and `stem_role` parsing (plan line 238, citing
  `snap_store.rs:799-832, 820-829` — verified accurate).
- `snap_aux_invalid_stem_role_is_typed` (`snap_aux_reader.rs:248`) and
  `snap_aux_rejects_non_point_or_linestring_wkb` (`snap_aux_reader.rs:332`) are named as
  non-negotiable regression proofs (plan lines 230-231) — both verified to assert errors from
  `DatasetSession::open_path`.
- The perf win is now warm-skip of the whole snap scan behind the token (plan line 239), not
  cold-path deletion. The flowchart (plan line 41) and Non-Scope (plan line 329) match.
- Any future move of geometry validation to the HFX validator is correctly fenced as an
  ESCALATE requiring a spec citation (plan lines 241, 336).

Resolved correctly.

### Minors from revision 1 — all folded in

1. Two-snap fixture infra now explicit (plan line 134). ✓
2. Warm `<9s` graph-parse floor reported separately (plan lines 276, 288). ✓
3. Cold `<60s` snap-scan contribution measured, not assumed (plan lines 277, 289). ✓
4. `validation_logic_version` discipline stated honestly — no test can catch a forgotten
   bump; doc-comment the constant and both validators (plan line 87). ✓
5. Step 1 single-snap-only scope note added (plan line 95). ✓
6. Lock discipline + 5× non-flake loop retained (plan lines 101, 116, 200, 214). ✓

---

## New minor — fold in before/while executing

**M-A (regression-proof sequencing for Step 5's snap warm-skip).** Step 5's gate says the
"warm-skip snap-scan test fails before the token/skip fix and passes after" (plan line 245),
but the behavior change that makes the snap scan skip on warm opens lands in **Step 2**, not
Step 5. Verified: today `validate_snap_refs` runs only in the cold/else branch
(`session.rs:479-481`); on a `validation_hit` the code falls through to the log-only block at
`session.rs:501-506` and never calls it. So the moment Step 2 turns GRIT into a token-HIT,
the snap scan is already skipped — and Step 5 changes no production behavior (it only adds the
counter, keeps cold detection, and measures). Authored at Step 5, the snap-warm-skip proof is
therefore after-the-fact: by then the fix already shipped two commits earlier.

Fix (cheap): move the snap-warm-skip counter + "second open does 0 snap scans" proof into
**Step 2**, where it genuinely fails-before / passes-after the `snap_stores.len() <= 1` gate
removal (the same place the two-snap read-id-level proof already lives). Leave Step 5 as what
it actually is: regression guards that the cold-path geometry/`stem_role` detection stays
green, plus cold-scan measurement. Optionally relabel Step 5's commit accordingly
(`test(core): guard snap validation reuse` is already honest about it being test/guard work).

**M-B (Step 5 is a test/guard-only commit — keep the patch bump anyway).** Because Step 5
changes no source behavior, confirm the executor still runs `./scripts/bump-version.sh patch`
+ tag for it per CLAUDE.md's per-commit rule (the plan already lists this at lines 251-254).
No change needed; just don't let "test-only" tempt anyone into skipping the bump.

---

## Soundness checks that PASSED (do not re-litigate)

- Token cannot collide cross-dataset (`validated.json` namespaced under fabric/adapter,
  `cache.rs:101-108`); any content replacement changes the R2 ETag.
- Cold path still reads catchment `(id, level)` and compares to the graph
  (`session.rs:967-1011`); M2 level-equality and cross-level-edge detection survive every
  cold/invalidated open. Option A holds.
- `read_id_levels` stays live on the cold path (`session.rs:967`) and in
  `test_read_id_levels_returns_expected_pairs` (`catchment_store.rs:2130`); no dead code from
  Steps 1/3.
- Skipping `validate_snap_refs` on a token match is sound now that manifest + graph +
  catchments + all snaps are token inputs.
- Parallelization template for Step 4 is real: `read_all_ids_with_row_groups_async` uses
  `.buffered(ID_INDEX_ROW_GROUP_CONCURRENCY)` (`catchment_store.rs:1377`); the serial
  `read_id_levels_async` (`catchment_store.rs:760-803`) is the correct refactor target.

---

## Bottom line

The plan is sound and executable. Ship it with M-A folded in (relocate the snap-warm-skip
proof to Step 2 so every step's regression test genuinely fails-before/passes-after), keep
M-B's per-commit bump in mind, and have the executor use HEAD (not GET) for the new
manifest/graph metadata so warm open stays cheap.
