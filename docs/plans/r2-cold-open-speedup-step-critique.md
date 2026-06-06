# Adversarial Critique — R2 Cold Open Speedup Step Plan

Reviewer role: adversarial critic (read-only on source/tests/config).
Date: 2026-06-06
Target: `docs/plans/r2-cold-open-speedup-step-plan.md`
Verified against shed `crates/core/src/{session.rs, cache.rs, telemetry/mod.rs,
reader/catchment_store.rs, reader/snap_store.rs, reader/id_index.rs}` and tests in
`crates/core/src/reader/catchment_store.rs` / `crates/core/src/session.rs`. Every file:line
anchor, cost claim, and concurrency claim below was checked against the dirty HEAD, not taken
on faith.

## Verdict (revision 2)

**DISPATCH AS-IS** — All six minors from revision 1 are folded in, verified against the updated
plan and re-checked against code:

1. **Mandatory const rename — CLOSED.** Plan line 201 now *renames* `ID_INDEX_ROW_GROUP_CONCURRENCY`
   to `LEAN_VALIDATION_ROW_GROUP_CONCURRENCY = 64` in both files (not a parallel add), with the
   orphan/clippy rationale stated. Re-verified: after the merge every remaining use of that const is
   a lean validation scan (catchment `read_id_levels` fallback + merged reader; snap membership), so
   the rename to 64 is semantically correct and leaves no use that should stay at 16. The Step 3
   `clippy -D warnings` gate now passes.
2. **`catchment_id_index_ms` + breakdown + lever separation — CLOSED.** Executive summary (line 12),
   expected-improvement math (line 37), Step 1 cold JSON (line 117), Step 4 reporting (line 246), and
   the PERF gate (line 282) now surface the merged-away `catchment_id_index` stage and attribute the
   merge vs concurrency wins separately.
3. **Id-index-build-path counter — CLOSED.** Plan line 140 names `read_all_ids_with_row_groups_async`
   and requires a counter on it so the 2→1 pass proof fires on the real path.
4. **In-flight flake/lock/5× — CLOSED.** Plan line 191 requires a latency-injecting/counting
   object-store wrapper (no reliance on fast in-memory fixtures); line 194 requires
   `GEOMETRY_DECODE_TEST_LOCK`-first and the 5× non-flake loop.
5. **`buffered(64)` ≠ HTTP concurrency — CLOSED.** Rationale (line 29), Step 3 change (line 211), and
   an ESCALATE flag (line 329) now state the gauge proves the scheduler bound, not network
   concurrency, and that the real-R2 re-measure is the acceptance proof.
6. **Warm target derivation — CLOSED.** Plan lines 43/255/259/285 frame 12 s as measured floor +
   variance margin with floor components reported separately.

No outstanding issues. The merge remains sound on every prime-directive point. Ship it.

---

## Verdict (revision 1)

**DISPATCH WITH MINORS** — The catchment-pass merge is **sound**: it preserves M2 per-row level
equality, the persisted id-index format, the warm-skip, and the geometry-decode-0 invariant, and
it correctly keeps a fallback `[id,level]` pass for the id-index-cache-hit/token-miss case. The
concurrency increase uses a separate constant and leaves the hot bbox paths untouched. The cold
target is honest about the non-optimizable graph download floor. No blocking soundness,
correctness, or regression-proof defect was found.

Six concrete minors the executor folds in before/while executing. The first is mandatory (the plan
as written fails its own per-step clippy gate); the rest are honesty/completeness/flake fixes.

---

## PRIME DIRECTIVE — catchment-pass merge soundness: PASSED

Each interrogation point from the dispatch, verified against code:

- **M2 level-mismatch detection survives.** `validate_graph_catchments` builds `catchment_levels`
  from **actual per-row** `(id, level)` values (`session.rs:1031-1034`), then enforces
  `row.level() != row_level` per graph row (`session.rs:1060`) and the same-level upstream-edge
  check (`session.rs:1085`). The plan's merged pass explicitly builds the `id -> level` map from
  `CatchmentIdLevelRow` values (plan lines 151-154), **not** row-group stats, and the investigation
  it derives from warns against trusting stats for per-row equality (`r2-open-reuse.md:144`). The
  merge feeds `validate_graph_catchments` the same per-row map it consumes today. The named M2
  regressions are preserved as gates: `graph_row_level_must_match_catchment_level`
  (`graph_parquet_reader.rs:150`, verified present) and the session-level swap/eviction
  revalidation tests `catchments_token_change_revalidates_graph_level_equality` (`session.rs:2982`)
  and `graph_token_change_after_cached_graph_eviction_revalidates_level_equality` (`session.rs:3023`,
  both verified present, both asserting `read_id_level_scan_count_for_test() > 0`). **Sound.**

- **id-index correctness preserved.** `IdIndex` stays `ids` + optional `id_row_groups`, ETag+size
  validated (`read_or_build_id_index`, `catchment_store.rs:1626-1667`). The merged worker builds
  `id -> row_group` exactly as `read_all_ids_with_row_groups_async` does today — per-row-group
  worker returns `(row_group, rows)` and the caller inserts `id -> row_group` with duplicate
  rejection (`catchment_store.rs:1510-1523`). Persisting only `ids` + `row_groups` (plan line 155)
  keeps the cache reusable by later warm opens. Duplicate-ID rejection already happens at store-open
  today (the id-index build runs inside `CatchmentStore::open*`, before validation), so moving it
  into the merged open pass changes nothing observable. **Sound.**

- **Warm-skip preserved.** On `validation_hit`, `validate_graph_catchments` is never called
  (guarded by `if !validation_hit`, `session.rs:516`), and on a persisted id-index cache hit the
  merged pass does not run (`read_or_build_id_index` returns early, `catchment_store.rs:1637-1647`).
  So a valid-token warm open still does zero catchment scalar reads. The warm proof
  `second_remote_open_with_two_snaps_uses_validation_sidecar` (`session.rs:2580`) stays valid.
  **Sound.**

- **geometry-decode-0 invariant preserved.** The merged pass projects `[id, level]` only (same
  `id_level_projection_indices` shape, `catchment_store.rs:778`); no geometry column is touched.
  **Sound.**

- **Ordering/equivalence.** `validate_graph_catchments` uses the id Vec only for a count check and
  reverse-membership iteration (`session.rs:1099`), and the level map only for lookups — no ordering
  dependency on the level map. The merged pass yields the id Vec in file order exactly as the
  id-index build does today. The plan calls for an explicit equivalence test (plan lines 190-193).
  **Sound.**

**No blocking merge defect.** The merge is the right shape and is internally consistent with the
real code.

---

## Concurrency increase — correctness/stability: PASSED with one realization caveat

- **Separate constant, hot path protected.** The plan adds `LEAN_VALIDATION_ROW_GROUP_CONCURRENCY
  = 64` and points it only at the merged catchment `[id,level]` pass, the fallback `read_id_levels`
  pass, and the snap `[id,unit_id]` membership pass (plan lines 196-200). It explicitly leaves
  `GEOMETRY_QUERY_ROW_GROUP_CONCURRENCY` (`catchment_store.rs:46`, value 16) and
  `SNAP_BBOX_ROW_GROUP_CONCURRENCY` (`snap_store.rs:87`, value **8**) untouched (plan lines 201-204).
  Verified: the catchment hot bbox read uses `GEOMETRY_QUERY_ROW_GROUP_CONCURRENCY`
  (`catchment_store.rs:707`) and the snap hot bbox read uses `SNAP_BBOX_ROW_GROUP_CONCURRENCY`
  (`snap_store.rs:559`). No hot-path bbox concurrency bleed. **Correct.**

- **Memory bounded.** Both target readers project scalars only (`[id,level]` ≈ 12 B/row,
  `[id,unit_id]` ≈ 16 B/row). At ~4,094 rows/RG, a decoded scalar row group is ~tens of KB; 64
  in-flight is single-digit MB plus compressed buffers. The plan's "scalar projection bounds memory"
  claim (plan line 28) is accurate. **Acceptable.**

- **Rate-limiting / failure modes.** ESCALATE flags exist for 429/throttling (plan lines 319, 338),
  worse tail latency (line 320), and memory (line 325), with a documented 48 fallback. Acceptable
  for a measured experiment.

- **Value justification.** 64 is a reasoned guess (4× of 16, "smallest aggressive step") backed by
  the 1.8 MB/s vs 10 MB/s latency-bound inference, manually re-measured in Step 4 with escalation
  gates. Acceptable engineering — but see Minor 5 for the realization caveat.

---

## Warm re-baseline — honesty: PASSED with one derivation fix

Warm is genuinely at-floor: validation is already skipped (all scan counters 0 on a token hit,
proven by `second_remote_open_with_two_snaps_uses_validation_sidecar`). The dominant residual cost
is the cached 700 MB graph disk read + ~2.4 s parse, which needs a format/loader change — correctly
fenced out of scope (plan lines 39, 308). Treating warm as "met at floor" and not optimizing it is
legitimate, not a dodge. The one fix is the 12 s value derivation (Minor 6).

---

## Cold realism — honesty: PASSED

The plan does **not** imply cold-first will be fast. It states the ~67-70 s graph download is a
bandwidth floor, reported separately, not optimized (plan lines 14, 34, 315), and that "cold-first
open over this slow link is inherently multi-minute; warm token reuse is the practical win"
(plan line 321). Cold target `<= 240 s` is explicitly inclusive of the graph floor. Honest. No
ESCALATE needed — the human trade-offs (rate-limiting risk, multi-minute cold) are already encoded
as in-execution escalation gates, which is the right place for them.

---

## Minors (fold in; Minor 1 is mandatory)

### Minor 1 (MANDATORY — plan fails its own clippy gate): rename, don't add, the concurrency const

After Step 3 redirects every lean-validation `buffered()` call to
`LEAN_VALIDATION_ROW_GROUP_CONCURRENCY = 64`, the existing `ID_INDEX_ROW_GROUP_CONCURRENCY = 16`
becomes **unused in both files**:

- `catchment_store.rs`: all uses are at `:762, :797, :809` (`read_id_levels_async`, the kept
  fallback) and `:1502` (`read_all_ids_with_row_groups_async`, replaced by the merged reader). Both
  switch to 64.
- `snap_store.rs`: all uses are at `:701, :713, :726`, all inside
  `read_all_snap_membership_refs_from_store_async`, which switches to 64.

The plan says **"Add `const LEAN_VALIDATION_ROW_GROUP_CONCURRENCY`"** (plan line 196). If the
executor adds a parallel const and leaves `ID_INDEX_ROW_GROUP_CONCURRENCY` in place, it is dead and
`cargo clippy -p shed-core -- -D warnings` (the Step 3 gate at plan line 213, and DURABILITY line
303) **fails** — the tree is not green at the end of Step 3, violating "each step independently
committable and green." This is the exact dead-code-under-own-clippy-gate failure the prior snap
milestone's critic flagged as blocking.

Fix (one line, executor-foldable): **rename** `ID_INDEX_ROW_GROUP_CONCURRENCY` to
`LEAN_VALIDATION_ROW_GROUP_CONCURRENCY` and set it to `64` in each file (after the merge, every one
of its uses is a lean full-file validation scan, so the rename is also semantically correct), or
delete the orphan. Verified: there are no other consumers of `ID_INDEX_ROW_GROUP_CONCURRENCY` in
either file.

### Minor 2: report the merged-away `catchment_id_index` stage; reconcile the cold breakdown

The merge's central win is eliminating the **separate catchment id-index build pass** — a real,
distinct telemetry stage `catchment_id_index` (`telemetry/mod.rs:78`), separate from
`validate_graph_catchments` (`:81`). But:

- The executive cold breakdown lists `validate_graph_catchments ~139 s` + `snap_membership ~141 s`
  + `graph ~70 s` ≈ **350 s**, which does not reconcile to the stated cold total **~537 s**. The
  ~130-187 s gap is the `catchment_id_index` build pass — precisely the thing the merge removes —
  and it is invisible in the breakdown.
- The current cold harness JSON reports `catchment_validate_ms`, `snap_membership_ms`,
  `graph_fetch_and_parse_ms`, `snap_store_open_ms`, `validate_snap_refs_ms` (`session.rs:2741-2763`)
  but **not** the `catchment_id_index` stage. Step 4's reporting list (plan lines 236-246) repeats
  this omission.
- The expected-improvement paragraph (plan line 35) says the merge collapses "the current ~280 s
  visible validation lean-scan cost (`validate_graph_catchments` plus snap membership)" — this is
  wrong about which lever does what. `read_id_levels` is already one `[id,level]` pass at
  `buffered(16)` (`catchment_store.rs:797` — the investigation's "serial" claim was pre-fix and no
  longer holds). The **merge** removes the id-index build pass (the unaccounted ~130-187 s); the
  **concurrency** bump is what speeds the 280 s of `read_id_levels` + snap membership. Conflating
  them makes the 240 s target's derivation unverifiable.

Fix: add `"catchment_id_index_ms": stage_ms(&stages, "catchment_id_index")` to the cold harness JSON
and Step 4's reporting list; reconcile the breakdown so the merged-away pass is quantified; and
rewrite the expected-improvement math to attribute (a) the id-index-build elimination to the merge
and (b) the 4× on `read_id_levels` + snap membership to concurrency. The end-to-end `<= 240 s` gate
is still measurable, but the attribution must be honest and diagnosable.

### Minor 3: anchor the 2→1 pass-count proof on the id-index build path too

Step 2 requires proving the OLD code does **two** distinct full catchment scalar passes
(`read_all_ids_with_row_groups_async` `[id]` + `read_id_levels_async` `[id,level]`) and the NEW code
does **one** (plan lines 139-141). Today only `read_id_levels` is counted
(`READ_ID_LEVEL_SCAN_COUNT_FOR_TEST`, `catchment_store.rs:53`, incremented at `:749`). The id-index
build pass (`read_all_ids_with_row_groups_async`, `catchment_store.rs:1442`) has **no** scan
counter. The plan's "extend instrumentation … to record … full-row-group pass count" (plan line 136)
gestures at this but should state explicitly: add a full-file-pass counter (with projection shape)
on the id-index build path so the "two passes → one" assertion fires on the real path, not by
inference. This keeps the merge proof non-vacuous per the CLAUDE.md regression rule.

### Minor 4: the `> 16` in-flight assertion can flake on fast in-memory fixtures

Step 3 asserts catchment/snap max-in-flight `> 16` and `<= 64` (plan lines 188-189). The existing
overlap test deliberately asserts only `> 1` (`catchment_store.rs:2295-2299`) because in-memory row
groups resolve fast — `buffered(64)` only reaches 17+ simultaneously pending if the futures stay
pending long enough, which tiny in-memory fixtures do not guarantee. A hard `> 16` on a fast fixture
risks intermittent failure (observed max could be 8-14). Fix: drive the proof with a
latency-injecting/counting object-store wrapper (or a fixture with enough row groups and per-RG
work) so >16 concurrent is deterministic, and pair it with the exact-output equivalence check.
Also: the plan omits the established non-flake discipline — restate that **every** test touching the
new snap in-flight gauge and the extended catchment gauge acquires `GEOMETRY_DECODE_TEST_LOCK` as
its first line (as `catchment_store.rs:2265` already does) and that the touched tests are looped ~5×
under the default parallel runner. The dispatch explicitly asks for this; the plan currently states
neither.

### Minor 5: `buffered(64)` ≠ 64 network requests — verify the HTTP client cap, and don't let the gauge over-claim

The 1.8 MB/s → "latency-bound at concurrency 16" inference is reasonable (1.8 MB/s ≪ the 10 MB/s
graph link, so it is not bandwidth). But the max-in-flight gauge measures **future-level**
concurrency at the `buffered()` layer, not concurrent HTTP connections. If the `object_store` HTTP
client's connection pool / HTTP-2 multiplexing caps effective concurrency below 64, `buffered(64)`
will queue at the transport and the 4× win will not materialize — while the gauge happily reports
64, giving false confidence. Fix: before attributing the slowdown solely to `buffered(16)`, check
(or note as a known unknown) the object-store client's connection configuration, and treat the
Step 4 manual re-measure (not the gauge) as the real proof the win landed.

### Minor 6: derive the 12 s warm target from the measured floor, don't hand-set it

`12,000 ms` (plan lines 41, 114, 277) is the measured warm (9.77 s) rounded up with margin, not a
derived floor. To avoid reading as "a number reverse-engineered to pass," tie it explicitly to the
summed measured floor components (graph disk read + ~2.4 s parse + 273 MB id-index read + token
HEADs) reported separately in the harness, and state `12 s = measured floor + variance margin`. The
plan already reports floor components separately (plan line 41) — just close the loop on the 12 s
value itself.

---

## Scope / blast-radius / version discipline: PASSED

- No touch to `query_by_bbox`, the hot delineation path, M1 goldens, or M3/M4/M5; the snap
  trust-boundary from v0.1.177 is unchanged (the plan keeps `validate_snap_refs` membership-only and
  does not re-introduce geometry scanning). Confirmed against Non-Scope (plan lines 305-315).
- Per-step patch bump + `Cargo.toml`/`Cargo.lock` staging + conventional commit + `v<version>` tag
  (plan lines 123-128, 172-177, 215-220, 263-268). pyshed untouched. Correct.
- Step 1 correctly commits the dirty `#[ignore]` env-gated harness
  (`measure_real_grit_{warm,cold}_*` at `session.rs:2639/2706`, plus
  `measure_local_merit_global_*` at `:2780`) and notes real-R2 numbers are gathered manually. Note
  in passing: the dirty harness currently asserts `elapsed_ms < 9000.0` (`session.rs:2701`), which
  would fail against the measured 9.77 s — Step 1's re-baseline to 12 s fixes that, so commit and
  re-baseline must land in the same step (they do).

---

## Bottom line

The merge is sound on every dispatch interrogation point (per-row M2 equality, id-index format,
warm-skip, geometry-decode-0, fallback pass, equivalence), the concurrency change is correctly
isolated from the hot paths, and the cold/warm targets are honest about the non-optimizable graph
floor. Ship it with the six minors folded in — **Minor 1 is mandatory** (otherwise Step 3 fails its
own `clippy -D warnings` gate via the orphaned `ID_INDEX_ROW_GROUP_CONCURRENCY` const), and Minors
2-3 are required for the merge's benefit to be measurable and the regression proof to be
non-vacuous.
