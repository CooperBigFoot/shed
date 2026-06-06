# Adversarial Critique — R2 Snap Open Reuse Step Plan (revised, trust-HFX)

Reviewer role: adversarial critic (read-only on source/tests/config).
Date: 2026-06-06 (revision 2 — re-review after planner edits)
Target: `docs/plans/r2-snap-open-reuse-step-plan.md`
Verified against shed `crates/core/src/{session.rs, cache.rs, reader/snap_store.rs,
reader/id_index.rs}`, tests in `crates/core/tests/{snap_aux_reader.rs, session_open.rs}`,
and the hfx validator `../hfx/crates/hfx-validator/src/{check/auxiliary.rs, check/geometry.rs,
reader/snap.rs}`. Code claims below were checked, not taken on faith.

## Verdict (revision 3)

**DISPATCH AS-IS** — sound and executable; no blocking issues, no remaining minors. The counter
naming is now consistent across every step and gate (`snap_geometry_decode_rows_for_test()` and
`snap_membership_rows_for_test()`; the `snap_validation_scan_count_for_test()` reference at plan
line 167 is the legitimately-reused existing counter from the prior milestone, `session.rs:142`).
Both revision-1 blockers and all four minors remain closed (detail below). Ship it.

## Verdict (revision 2)

**DISPATCH WITH MINORS** — both revision-1 blocking defects are resolved and all four minors are
folded in. One trivial cosmetic nit remains (counter naming is inconsistent across steps); the
executor folds it in. The plan is sound and executable.

### Revision-1 blocking items — both CLOSED

- **BLOCKING 1 (dead-reader teardown) — CLOSED.** Step 3 now explicitly deletes the orphaned island
  in the same commit that stops calling it: `read_or_build_id_index`,
  `read_all_snap_refs_from_store`, `read_all_snap_refs_from_store_async`,
  `read_snap_refs_row_group_async`, `UnitIdRowGroupReadContext`, the unused `SnapValidationReadStats`
  geometry/stem fields, and the snap `id_index_path` plumbing through `open_remote_with_caches` /
  `open_object` / the `session.rs` call sites (plan lines 182-187). It explicitly retains
  `geometry_from_array` (`snap_store.rs:1138`) and `validate_snap_geometry` (`:1162`) as query-live
  (line 187), and adds `cargo clippy -p shed-core -- -D warnings` as a Step 3 gate that proves the
  removal (line 195). I confirmed the deleted island is a closed call graph whose only entry was
  `open_object:363`, so deletion is safe and the Step 3 commit is now clippy-clean.

- **BLOCKING 2 (counter anchored to deleted reader) — CLOSED.** Step 1 now anchors the geometry
  counter at the durable shared decode site (`geometry_from_array` / `validate_snap_geometry`) and
  explicitly forbids `read_snap_refs_row_group_async` / `read_all_snap_refs_from_store_async` (plan
  lines 93-94, reinforced at 103). The non-vacuousness argument is now stated: the current full scan
  reaches the same helper (fail-before `> 0`), and post-fix `query_by_bbox` can still drive the
  counter non-zero, so the `== 0`-at-open guard remains regressible (line 94). Verified: both helpers
  stay live via `extract_snap_targets_from_batch` (`snap_store.rs:989-990`), so the increment site is
  not orphaned.

### Revision-1 minors — all folded in

- **M1 (NotLoaded accessors).** `read_all_snap_refs()` / `read_all_unit_ids()` must return a typed
  `SessionError` on `SnapRefsState::NotLoaded`, never panic (plan line 137). ✓
- **M2 (`InvalidStemRole` not orphaned).** The "if unused, remove" hedge is gone; both
  `InvalidStemRole` and `SnapGeometryInvalid` are kept as query-time-live with re-scoped doc
  comments (plan line 229), and the stale ESCALATE flag for `InvalidStemRole` removal is dropped. ✓
- **M3 (single HEAD).** Threading the single token HEAD into lazy (and preferably cold) open is now
  the default; double-HEAD is the documented, fail-closed fallback only (plan lines 180, 337). ✓
- **M4 (measurement literal).** Step 5 logs *measured* membership rows and flags deviation, treating
  `22,337,300` as the GRIT v2.0.0 reference, not a hard-asserted literal (plan line 255). ✓

### Remaining minor (executor folds in; not blocking)

- **Counter-name consistency.** The same geometry counter is referred to by three names across
  steps: `snap_geometry_decode_rows_for_test()` (line 108), `geometry_rows_for_test()` (line 125),
  and `snap_geometry_rows_for_test()` (lines 167-168). Pick one accessor name (e.g.
  `snap_geometry_decode_rows_for_test`) and use it in every step/gate. Likewise reconcile
  `membership_rows_for_test()` (line 125) with `snap_membership_rows_for_test()` (lines 167-168).
  Purely cosmetic — the intent is unambiguous.

---

## Original (revision-1) record retained below

**SEND BACK** — two blocking executability / regression-proof defects, both in the snap-reader
teardown. The plan is sound on every hard soundness question (token coverage, fail-closed,
trust-HFX scope, membership preservation, version bump), but as written **Step 3 cannot pass its
own clippy-clean / independently-committable gate**, and the **geometry regression counter is
placed at a site the same milestone deletes**, leaving the post-fix guard non-regressible. Both
fixes are surgical; the planner must edit the Step 1 and Step 3 step bodies, so it goes back to the
planner rather than being folded silently by the executor.

---

## BLOCKING 1 — Step 3 orphans the entire snap geometry-decode reader chain (dead code → clippy gate fails)

The milestone's whole point is that, after Step 3, `SnapStore::open_object` stops calling the
geometry-decoding cold reader on both warm (lazy) and cold (lean) paths. But that reader chain has
**no other caller**, so Step 3 leaves it dead in the production lib build. Verified call graph:

- `open_object` → `read_or_build_id_index` (`snap_store.rs:363`)
- `read_or_build_id_index` (`snap_store.rs:568`) → `read_all_snap_refs_from_store` (`:613`)
- `read_all_snap_refs_from_store` (`snap_store.rs:549`) → `read_all_snap_refs_from_store_async` (`:557`)
- `read_all_snap_refs_from_store_async` (`snap_store.rs:641`) → `read_snap_refs_row_group_async` (`:720`)
- `read_snap_refs_row_group_async` (`snap_store.rs:747`) is called nowhere else.

`grep` confirms these four functions form a closed island whose only entry is `open_object:363`
(the snap copy of `read_or_build_id_index` is distinct from the still-live catchment copy at
`catchment_store.rs:1626` — removing the snap one is safe). Once Step 3 routes
`ColdMembershipValidation` to the **new** lean reader and `LazyMetadata` to nothing, the entire
chain — plus `UnitIdRowGroupReadContext` (`snap_store.rs:99`), the geometry/stem fields of
`SnapValidationReadStats` (`:128-144`), and the snap `id_index_path` plumbing
(`session.rs:457-458,469`; `snap_store.rs:248,258,271,371`) — becomes unreferenced.

These are module-private items; `dead_code` is warn-by-default and `cargo clippy -p shed-core --
-D warnings` (Step 2 gate, line 150) and `cargo clippy --workspace -- -D warnings` (DURABILITY,
line 271) promote it to an error. **Step 3 as written produces a tree that does not compile under
the milestone's own clippy gate**, violating DURABILITY line 311 ("Every step is independently
committable") and the per-step clippy requirement.

The plan body never instructs this teardown. It carefully covers the *new*-side dead code (the
temporary `#[cfg_attr(not(test), allow(dead_code))]` on `LazyMetadata`/`NotLoaded`, line 138) but
is silent on the *old*-side island that Step 3 orphans. Step 3's only related lines (179, 182) say
"remove any Step 2 temporary allow" and "do not call `read_all_snap_refs_from_store` on the hit
branch" — neither removes the now-dead functions.

Required change (fold into Step 3's "Change" list): explicitly delete `read_or_build_id_index`,
`read_all_snap_refs_from_store`, `read_all_snap_refs_from_store_async`,
`read_snap_refs_row_group_async`, `UnitIdRowGroupReadContext`, and the unused
`SnapValidationReadStats` geometry/stem fields, **and** drop the snap `id_index_path` parameter
threading through `open_remote_with_caches` / `open_object` (or document why it is retained). State
that `geometry_from_array` (`snap_store.rs:1138`) and `validate_snap_geometry` (`:1162`) must stay,
because they are still used by the query-time `extract_snap_targets_from_batch` (`:989-990`).

## BLOCKING 2 — geometry regression counter is anchored to the deleted reader (orphaned + vacuous post-fix guard)

Step 1 (lines 93-94) instructs placing `SNAP_VALIDATION_GEOMETRY_ROWS_FOR_TEST` "immediately after
`stats.geometry_rows_validated += 1` at `snap_store.rs:866-868`, or aggregate at
`read_all_snap_refs_from_store_async` after `snap_store.rs:727-732`." **Both sites are inside the
reader chain that BLOCKING 1 deletes.** Consequences:

1. After Step 3, the only code that increments the geometry counter is gone. The counter's
   increment function becomes itself dead (`dead_code` even under `--all-targets`, since only the
   deleted reader called it), compounding BLOCKING 1.
2. The flipped post-fix assertions — Step 3 line 165 (`snap_geometry_rows_for_test() == 0` on warm)
   and the cold assertion (`geometry_rows == 0` while membership `> 0`) — become **non-regressible**:
   no surviving production path can ever make the counter non-zero, so the guard can never catch a
   future re-introduction of open-time geometry decode. This is precisely the "after-the-fact passing
   test" the project's regression rule forbids ("the test must hit the real failing path").

The fail-before/pass-after *sequence across steps* is technically intact (Step 1 proves `> 0` on
current code; within Step 3 the flip fails-before / passes-after the reorder while the old reader
still exists at the start of Step 3). But the resulting guard is dead weight afterward, and the
counter symbol is orphaned.

Required change: place the geometry counter at the **durable shared decode site** that survives the
milestone — `validate_snap_geometry` (`snap_store.rs:1162`) or `geometry_from_array`
(`snap_store.rs:1138`), both reachable from query-time `extract_snap_targets_from_batch`. Then:

- Step 1 (current code): a warm 2nd open still flows `open_object → … → read_snap_refs_row_group_async
  → validate_snap_geometry`, so the counter reads `> 0`. Proof still fails-before correctly.
- Step 3 (post-fix): open decodes nothing, so the counter is `0` *at open time* — but `query_by_bbox`
  would still increment it, so the guard is **non-vacuous and regressible**.
- No dead code: `validate_snap_geometry` stays live via the query path.

The membership counter (`SNAP_MEMBERSHIP_ROWS_FOR_TEST`) does not have this problem — it belongs in
the new lean reader, which stays live on the cold/local path. Keep it there.

---

## Minors (fold in while revising; not blocking on their own)

- **M1 — `read_all_snap_refs` on `NotLoaded`.** Step 2 replaces `all_snap_refs: Vec<SnapUnitRef>`
  (`snap_store.rs:159`) with `SnapRefsState::{NotLoaded, Loaded}`. `read_all_snap_refs()`
  (`snap_store.rs:520`, sole production caller `validate_snap_refs` at `session.rs:1115`) must return
  a typed `SessionError` (not `unwrap`/`panic`) when called on a `NotLoaded` (lazy/hit) store, per
  the library no-unwrap rule. The plan does not specify the `NotLoaded` behavior of the accessors.
  State it: validation only ever calls this on the cold branch where the state is `Loaded`; defend
  the invariant with an explicit error, not a panic.

- **M2 — `InvalidStemRole` is NOT orphaned.** Step 4 line 220 hedges "if `InvalidStemRole` becomes
  unused in production, remove…". It does not become unused: `SessionError::InvalidStemRole` is still
  raised by the query-time path at `snap_store.rs:1032` (`extract_snap_targets_from_batch`). Only the
  open-reader raise site (`:860`) disappears with BLOCKING 1. Replace the hedge with the fact:
  `InvalidStemRole` and `SnapGeometryInvalid` both remain live via the query path
  (`:1032`; `:1166/1172/1188`); keep them with doc comments that now describe query-time, not
  open-time, semantics. No API change needed; the ESCALATE flag at line 330 is moot.

- **M3 — prefer one HEAD, not two, for snaps (TOCTOU narrowing).** Today the single
  `head_object_meta` inside `open_object` (`snap_store.rs:275`) is the *same* metadata used both to
  open the store and to build `artifact_meta()` for the token — token and store-open are guaranteed
  to describe the same object. Step 3's reorder (HEAD for token inputs *before* opening) introduces a
  second HEAD per snap on the lazy path, opening a token-vs-open mismatch window that does not exist
  today. The plan acknowledges this (line 178, ESCALATE line 328) but the *preferred* outcome should
  be the default: thread the single token HEAD (`ObjectMeta`) into lazy open so the lazy store reuses
  it instead of re-HEADing, preserving the current single-source-of-truth. Fall back to documented
  double-HEAD (both fail-closed) only if threading is non-surgical.

- **M4 — Step 5 cold measurement number.** Line 246 asks the cold measurement to log "membership
  rows `22,337,300`". That is the GRIT total snap-row count (investigation table), so it is the
  correct membership-row target — just have the executor log *measured* rows and flag deviation
  rather than asserting the literal, since a fabric/version change would shift it.

---

## Soundness checks that PASSED (do not re-litigate)

- **Trust-HFX scope (a) is real.** `check_snap_v1` unconditionally runs `check_snap_stem_roles` and
  `geometry::check_snap_geometries` after reading each `hfx.aux.snap.v1`
  (`../hfx/.../check/auxiliary.rs:159-161`). `check_snap_stem_roles` (`auxiliary.rs:207-224`) errors
  on any role outside `mainstem|tributary|distributary|unknown` over **all** rows.
  `check_snap_geometries` (`geometry.rs:58-71`) iterates **all** `geometry_wkb` (no sampling, unlike
  catchment geometry) and `check_single_snap_geometry` (`geometry.rs:149-164`) rejects non
  Point/LineString type codes and unparseable WKB. Null geometry is reported in the reader
  (`reader/snap.rs:299-317`). The validator's coverage matches the plan's claim; trusting it for
  geometry/`stem_role` well-formedness is the human's locked call and is genuinely backed.

- **Token attests every snap, fail-closed (b)/(d).** `ValidationSidecar.snaps: Vec<ArtifactMeta>`
  (`cache.rs:48`) holds *all* declared snaps; the matcher compares the full sorted vector
  (`cache.rs:300-309`). Inputs are collected as `Option<Vec<ArtifactMeta>>` via
  `.collect::<Option<Vec<_>>>()` (`session.rs:476-484`), and `ArtifactMeta::from_parts` returns
  `None` when ETag is absent (`cache.rs:78`). So any missing snap ETag ⇒ `snaps == None` ⇒
  `validation_sidecar_matches` returns `false` (`session.rs:1233-1240`); the write side also yields
  no token. Size-only trust is impossible. R2 multipart ETags (`"abf5…-773"`, investigation line 188)
  are content-derived and change on any content change, so a swapped/truncated snap cannot keep the
  same ETag — a same-size-same-ETag forgery would require a hash collision. Per-snap path+ETag+size
  coverage holds.

- **Membership preserved on cold/local (b)/(e).** `validate_snap_refs` (`session.rs:1107-1141`) is
  unchanged: it needs only `snap_id` (for the error message) + `unit_id` + the level map, all
  available from the lean `[id, unit_id]` projection. The kept tests exercise the *real* path via
  local `open_path` (always cold): `snap_aux_missing_unit_id_reports_real_snap_id`
  (`snap_aux_reader.rs:274`), `snap_aux_references_levels_mismatch_reports_real_snap_id` (`:302`), and
  `session_open.rs:970` (`unit_id 99` ∉ catchments). The lean reader retains null/invalid `id` and
  `unit_id` rejection (current `snap_store.rs:819-853`), so a null/invalid `unit_id` cannot slip
  through.

- **Right tests dropped (c).** Only `snap_aux_invalid_stem_role_is_typed` (`:248`) and
  `snap_aux_rejects_non_point_or_linestring_wkb` (`:332`) — both pure geometry/role well-formedness —
  are converted to assert open *acceptance* when membership is valid. No referential/membership check
  is smuggled out.

- **Query-time failure is real, not hand-waved (f).** `extract_snap_targets_from_batch` decodes
  candidate WKB (`snap_store.rs:989`) and calls `validate_snap_geometry` (`:990`), returning
  `SessionError::SnapGeometryInvalid` for non-Point/LineString. A contract-violating geometry that
  hfx should have caught surfaces here, on the small windowed candidate read, not via an open-time
  full scan. The proposed Step 4 residual test (query the bbox of the polygon fixture, expect
  `SnapGeometryInvalid`) genuinely exercises this. Open no longer scans all geometry.

- **Cost honesty.** Lean `[id, unit_id]` = 247,836,213 B across 5,453 row groups (investigation
  table) vs former `id+unit_id+geometry+stem_role` = 9,634,957,822 B — the 38.9× claim is accurate.
  Cold target `< 35 s` (escalate `> 45 s`) with RTT floor `ceil(5453/16) * 30-60 ms ≈ 10-21 s` plus
  transfer + decode is a defensible envelope. Warm `< 9 s` reports the cached-graph parse floor
  separately. `.buffered(16)` reuses `ID_INDEX_ROW_GROUP_CONCURRENCY`. Honest.

- **Version + lock discipline.** `VALIDATION_LOGIC_VERSION` lives at `cache.rs:62` (`r2-open-reuse-v1`),
  the matcher compares the **constant** (`cache.rs:304`), and no test hardcodes the literal — so the
  bump to `r2-snap-membership-v2` fails old tokens closed (`matches` returns false) without breaking
  existing `current()`/`matches()` tests. `GEOMETRY_DECODE_TEST_LOCK` is `pub(crate)`
  (`catchment_store.rs:62`), reachable from both `snap_store.rs` and `session.rs` tests, so the
  lock-first discipline for the new global counters is feasible; ensure **every** test touching
  either new counter acquires it first (Step 2's direct snap-store tests included), with the 5× loop
  as the non-flake gate. `Cargo.toml` is `0.1.173`; one patch bump + commit + `v<version>` tag per
  step, pyshed untouched.

- **No hot-path / blast-radius breakage.** `query_by_bbox` semantics are unchanged; the lazy store
  populates everything it needs (`row_groups`, `groups_without_stats`, schema, `file_size`, caches).
  Open-time schema/footer/bbox rejections (`require_column`, `optional_bbox_col_indices`, row-group
  bbox extraction at `snap_store.rs:325-361`) stay on both paths — the only checks that move out are
  the per-row geometry/`stem_role` validations, which is the intended trust-HFX change. M1/M3/M4/M5
  and the catchment fixes from the prior milestone are untouched.

---

## Bottom line

The trust-HFX decision is well-founded (the hfx validator genuinely enforces snap geometry and
`stem_role` over all rows), the token soundly attests every snap fail-closed, membership detection
is preserved on the real path, and the version bump is handled correctly. The plan is blocked only
by its teardown of the old snap reader: **Step 3 must explicitly delete the orphaned geometry-decode
reader chain + id_index plumbing (BLOCKING 1), and Step 1 must anchor the geometry counter at the
surviving query-time decode site rather than the deleted reader (BLOCKING 2).** Fold in M1–M4 while
revising. With those edits the milestone is sound and executable.
