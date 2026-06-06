# Fast Open Step Plan

## 1. Chosen Fix Shape

Choose **(a) ID-only validation projection**, specifically `id + level`, and keep eager open-time referential validation.

Justification:

- The binding diagnosis is open-time full-dataset work, not bbox pruning or `graph.parquet`: `DatasetSession::open()` calls `validate_graph_catchments()`, which reads all ids and then calls `query_by_ids(&catchment_ids)` for every catchment row; `query_by_ids()` uses the full catchment projection including `geometry` (`docs/investigations/v021-read-perf.md:10`, `docs/investigations/v021-read-perf.md:17`, `docs/investigations/v021-read-perf.md:103`).
- Current source already shows the validation stage dominates cold local global open: `validate_graph_catchments` was about 17.274 s on MERIT global, while the id index was about 114.8 ms (`docs/investigations/v021-read-perf.md:78`).
- The validation helper consumes only `unit.id()` and `unit.level()` from the full catchment rows (`crates/core/src/session.rs:971`, `crates/core/src/session.rs:974`). No geometry, area, bbox, outlet, or parent check rides along in this path.
- HFX v0.2.1 requires `catchments.parquet.level` to be non-null `int16` and requires `graph.parquet.level` to match it (`../hfx/spec/HFX_SPEC.md:62`, `../hfx/spec/HFX_SPEC.md:63`, `../hfx/spec/HFX_SPEC.md:171`, `../hfx/spec/HFX_SPEC.md:173`).
- This is the smallest shed-owned reader/session change that removes geometry decoding from validation while preserving the M2 detection guarantee. It avoids changing HFX writer/format, lazy semantics, sidecar keying, M1 goldens, staged contracts, carve, export, or hot-path geometry reads.

Explicit deferrals:

- Defer lazy, skippable, or optional validation. Eager detection remains mandatory on non-sidecar paths.
- Defer local persistent sidecar/id-level-index reuse. Local `CatchmentStore::open()` currently passes `id_index_path = None` (`crates/core/src/reader/catchment_store.rs:164`, `crates/core/src/reader/catchment_store.rs:175`), but id-only validation should already remove the multi-GB geometry read. Sidecar reuse can be a later optimization if total open remains above target.
- Defer changing `query_by_ids()`, `query_by_bbox()`, and `query_geometries_by_ids()` behavior. The delineation hot path must stay unchanged.

## 2. Three Open Call Sites

All three open paths must stop using full `query_by_ids()` for all catchments:

1. Local open: `DatasetSession::open_path()` enters `Stage::ValidateGraphCatchments` and calls `validate_graph_catchments(&manifest, &graph, &catchments)` (`crates/core/src/session.rs:251`, `crates/core/src/session.rs:253`). Fix by changing `validate_graph_catchments()` to build `catchment_levels` from a new lean catchment-store reader, not from `query_by_ids()`.
2. Remote sidecar miss: `open_remote_with_stats()` enters `Stage::ValidateGraphCatchments` and calls the same `validate_graph_catchments()` (`crates/core/src/session.rs:470`, `crates/core/src/session.rs:473`). The same helper change fixes this path.
3. Remote sidecar hit: the sidecar-hit branch bypasses validation but still calls `catchments.read_all_ids()` followed by `catchments.query_by_ids(&catchment_ids)` just to build `HashMap<UnitId, Level>` (`crates/core/src/session.rs:455`, `crates/core/src/session.rs:463`, `crates/core/src/session.rs:465`, `crates/core/src/session.rs:467`). Fix this branch to call the same new lean `id + level` reader outside the validation stage.

## 3. Level Source Decision

Use the **catchment `level` column** as the authoritative source for `catchment_levels`.

Rationale:

- The guarantee being preserved is that graph levels match catchment levels. That cannot be proven by sourcing levels from graph rows alone.
- HFX v0.2.1 requires `catchments.parquet.level` as non-null `int16` (`../hfx/spec/HFX_SPEC.md:62`, `../hfx/spec/HFX_SPEC.md:63`), and `graph.parquet.level` must match it (`../hfx/spec/HFX_SPEC.md:171`, `../hfx/spec/HFX_SPEC.md:173`).
- Existing reader code currently treats catchment `level` as optional when extracting full `CatchmentUnit`s and defaults absent level to `0` (`crates/core/src/reader/catchment_store.rs:755`, `crates/core/src/reader/catchment_store.rs:791`, `crates/core/src/reader/catchment_store.rs:801`). The new validation projection must not silently default on v0.2.1. It should require `level` and reject missing or null levels with `SessionError::ParquetSchema` or `SessionError::InvalidRow`.

Absent-column handling:

- Add a validation-specific projection helper that requires `id` and `level`. If `level` is absent, session open must fail before graph/catchment level comparison, because there is no catchment-side level source to validate against.
- Do not change full hot-path extraction defaults in this fix unless tests show the validation helper requires shared code. Keeping the change scoped avoids altering legacy-ish query behavior outside open validation.

## 4. Independently Committable Steps

### Step 1: Add Lean Catchment ID-Level Reader

Files/functions touched:

- `crates/core/src/reader/catchment_store.rs`
- Add a typed row struct, for example `CatchmentIdLevelRow { id: UnitId, level: Level }`, with private fields and accessors.
- Add `CatchmentStore::read_id_levels(&self) -> Result<Vec<CatchmentIdLevelRow>, SessionError>`.
- Add async implementation that scans all row groups with only the `id` and `level` projection. Do not thread a caller-provided id list through this method; for validation, all rows are needed and `selected_row_groups_for_ids()` would select every row group anyway (`crates/core/src/reader/catchment_store.rs:717`).
- Add `id_level_projection_indices(parquet_schema) -> Result<[usize; 2], SessionError>` beside `geometry_projection_indices()` (`crates/core/src/reader/catchment_store.rs:1548`). It should use `id_column_index()` and `named_column_index("level")`, not `optional_named_column_index()`.
- Add a small batch extractor for id-level rows. It must reject null `id` and null `level`.

Verification commands and expected result:

- `cargo test -p shed-core catchment_store --lib` green.
- `cargo test -p shed-core graph_parquet_reader session_open` green.

Tests added/kept:

- Add a `catchment_store` unit test proving `read_id_levels()` returns expected `(id, level)` values from a fixture that includes a non-zero level.
- Add a `catchment_store` unit test proving missing `level` is rejected on `read_id_levels()`.
- Keep existing `query_by_ids()` and `query_geometries_by_ids()` tests green to prove hot-path projections are unchanged.

Version/commit:

- Run `./scripts/bump-version.sh patch`.
- Stage `Cargo.toml`, `Cargo.lock`, and the touched test/source file.
- Commit with `feat(core): add catchment id-level validation reader`.
- Tag with `git tag v$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')`.

### Step 2: Add Non-Vacuous Geometry-Read Regression Gate While HEAD Is Still Broken

Files/functions touched:

- `crates/core/src/reader/catchment_store.rs`
- Thread the store path display into full-row extraction, for example by adding a `path_display: &str` parameter to `extract_units_from_batch()` and updating its call sites. Then add `#[cfg(test)] record_geometry_decode_for_test(path_display, unit_id)` at the full-row geometry materialization point (`crates/core/src/reader/catchment_store.rs:916`, `crates/core/src/reader/catchment_store.rs:928`). This is the path current validation actually uses through `query_by_ids()`.
- Because `path_display` is consumed only by `#[cfg(test)]` instrumentation, guard the parameter against the normal non-test warning gate. Prefer `#[cfg_attr(not(test), allow(unused_variables))]` on `extract_units_from_batch()` so `cargo clippy --workspace -- -D warnings` stays green without cfg-splitting every call site.
- Keep the existing counter call in `read_geometry_row_group_async()` for `query_geometries_by_ids()` (`crates/core/src/reader/catchment_store.rs:1115`, `crates/core/src/reader/catchment_store.rs:1117`).
- Add a crate-internal `#[cfg(test)]` test, not an integration test. The existing hooks are `#[cfg(test)] pub(crate)` (`crates/core/src/reader/catchment_store.rs:702`, `crates/core/src/reader/catchment_store.rs:1137`), so `crates/core/tests/session_open.rs` cannot see them. Put the test in `crates/core/src/session.rs` or a crate-internal test module that can open a fixture and inspect the session's catchment store.

Test shape:

- Reset geometry decode counts with `reset_geometry_decode_counts_for_test()` (`crates/core/src/reader/catchment_store.rs:1137`).
- Open a fixture dataset with `DatasetSession::open_path()` while validation is still routed through `query_by_ids()`.
- Add a temporary crate-internal proof test asserting at least one fixture unit's geometry decode count is greater than zero immediately after open. This green-on-bug test proves the counter observes the broken validation path.
- Also run the final intended zero-decode assertion locally before Step 3 and capture that it fails on this state. Do not commit a failing test. The committed durable test after Step 3 must assert zero after open.

Verification commands and expected result:

- Before Step 3, run the focused final zero-decode assertion and capture that it fails, or commit the temporary nonzero proof test and state in the commit message that the final zero assertion was red on this pre-routing state.
- `cargo test -p shed-core catchment_store --lib` green except for any intentionally temporary red-on-bug assertion that is not committed.

Tests added/kept:

- Add the test-only instrumentation before rerouting validation so the gate is proven non-vacuous.
- Do not widen production API. Do not move the hooks into integration-test-visible public API.

Version/commit:

- Run `./scripts/bump-version.sh patch`.
- Stage `Cargo.toml`, `Cargo.lock`, and `crates/core/src/reader/catchment_store.rs` plus any crate-internal test file.
- Commit with `test(core): instrument full catchment geometry reads`.
- Tag with the current workspace version.

### Step 3: Route Validation and Sidecar-Hit Open Through ID-Level Reader

Files/functions touched:

- `crates/core/src/session.rs`
- Change `validate_graph_catchments()` so lines currently reading all ids and then full units (`crates/core/src/session.rs:968`, `crates/core/src/session.rs:971`) use `catchments.read_id_levels()` and build both `catchment_id_set` and `catchment_levels` from those rows.
- Preserve both derived structures: keep an ordered `catchment_ids: Vec<UnitId>` for the reverse catchment-without-graph check (`crates/core/src/session.rs:1040`, `crates/core/src/session.rs:1041`) and a `HashSet<UnitId>` for graph/upstream membership checks (`crates/core/src/session.rs:988`, `crates/core/src/session.rs:1012`).
- Change the remote sidecar-hit branch currently calling `query_by_ids(&catchment_ids)` (`crates/core/src/session.rs:463`, `crates/core/src/session.rs:465`) to call `catchments.read_id_levels()` and collect the same `HashMap<UnitId, Level>`.
- Do not alter `validate_snap_refs()`: it still consumes `catchment_levels` and preserves snap referential detection (`crates/core/src/session.rs:267`, `crates/core/src/session.rs:270`, `crates/core/src/session.rs:480`, `crates/core/src/session.rs:482`).

Verification commands and expected result:

- `cargo test -p shed-core --test session_open` green.
- `cargo test -p shed-core --test graph_parquet_reader` green.

Tests added/kept:

- Keep the three session-open detection tests green: graph row missing (`crates/core/tests/session_open.rs:857`), dangling upstream (`crates/core/tests/session_open.rs:879`), and dangling snap reference (`crates/core/tests/session_open.rs:929`).
- Keep the graph level-detection tests green: graph row level differs from catchment level (`crates/core/tests/graph_parquet_reader.rs:150`, assertion at `crates/core/tests/graph_parquet_reader.rs:163`) and graph edge crosses levels (`crates/core/tests/graph_parquet_reader.rs:167`, assertion at `crates/core/tests/graph_parquet_reader.rs:180`).
- Add a session-open-level mismatch test if any existing mismatch coverage is not exercising `DatasetSession::open_path()` after the refactor. The test should mutate catchment levels and assert `GraphReferentialIntegrity` still mentions `differs from catchment level`.
- Convert the crate-internal geometry-decode test from Step 2 into the committed durable assertion: `DatasetSession::open_path()` performs zero full-row geometry materializations during validation. This test must now pass because validation uses `read_id_levels()`.

Version/commit:

- Run `./scripts/bump-version.sh patch`.
- Stage `Cargo.toml`, `Cargo.lock`, and `crates/core/src/session.rs` plus tests.
- Commit with `fix(core): avoid full catchment reads during open validation`.
- Tag with the current workspace version.

### Step 4: Add Remote Sidecar-Hit Regression Coverage

Files/functions touched:

- `crates/core/src/session.rs`
- Extend or add a crate-internal remote-open test that drives the validation sidecar-hit branch (`crates/core/src/session.rs:455`, `crates/core/src/session.rs:463`). The existing remote sidecar test already performs two remote opens and asserts persistent index/sidecar files exist (`crates/core/src/session.rs:2079`, `crates/core/src/session.rs:2126`); use that shape but add geometry-read assertions.
- Reset the full-row geometry decode counter before the second open, where the sidecar hit is expected.

Test shape:

- First remote open builds the validation sidecar.
- Reset counts.
- Second remote open must hit the sidecar branch and still build `catchment_levels` without `query_by_ids()`.
- Assert zero full-row geometry materializations during that second open.
- Keep the existing sidecar assertions that the cache files exist and that the second open uses fewer ranged reads (`crates/core/src/session.rs:2101`, `crates/core/src/session.rs:2129`).

Verification commands and expected result:

- `cargo test -p shed-core sidecar` green for the focused remote-sidecar test.
- `cargo test -p shed-core --lib` green for crate-internal tests.

Version/commit:

- Run `./scripts/bump-version.sh patch`.
- Stage `Cargo.toml`, `Cargo.lock`, and `crates/core/src/session.rs`.
- Commit with `test(core): cover sidecar-hit id-level materialization`.
- Tag with the current workspace version.

### Step 5: Full Gates and Benchmark Capture

Files/functions touched:

- No production changes expected.
- Optionally update this plan or add benchmark notes only if the implementer is asked to record results in-repo.

Verification commands and expected result:

- `cargo test -p shed-core --test parity_golden_artifacts` green.
- `cargo test -p shed-core --test staged_delineation` green.
- `cargo test -p shed-core --test d8_refinement_parity` green.
- `cargo test -p shed-core --test graph_parquet_reader` green.
- `cargo test -p shed-core --test snap_aux_reader` green.
- `cargo test -p shed-core --test session_open` green.
- `cargo test -p shed-core --test hfx_v02_loader` green.
- The crate-internal zero-full-row-geometry-open tests for both local validation and remote sidecar hit are green.
- `cargo build --workspace --exclude pyshed` green.
- `cargo check -p pyshed` green.
- `cargo clippy --workspace -- -D warnings` green. Note that the pyshed size-threshold lint is linux/CI-only.

Perf commands and expected result:

- Before quoting before/after medians, confirm the benchmark harness accepts absolute local dataset paths in the `--dataset` argument. A one-iteration pfaf62 run is enough for this interface check.
- Fast iteration before and after:

```bash
scripts/bench-delineate.sh --release --mode cold \
  --dataset /Users/nicolaslazaro/Desktop/merit-hfx-v2/tier2_5_pfaf62/merit-hfx-pfaf62 \
  --outlet -14.075,-74.72 \
  --iterations 3 \
  --out scratchpad/benchmarks/fast-open-pfaf62-cold.jsonl
```

- Headline before and after:

```bash
scripts/bench-delineate.sh --release --mode cold \
  --dataset /Users/nicolaslazaro/Desktop/merit-hfx-v2/planetary/merit-hfx-global \
  --outlet -14.075,-74.72 \
  --iterations 3 \
  --out scratchpad/benchmarks/fast-open-global-cold.jsonl
```

- Parse the JSONL for `stage == "validate_graph_catchments"` and compare median duration before/after. Use the resolving South America outlet above or another confirmed resolving outlet; avoid the Ticino global outlet because the investigation records a later unrelated `AmbiguousD8Coverage` failure (`docs/investigations/v021-read-perf.md:75`).

Version/commit:

- If only benchmark artifacts under scratchpad are produced, do not commit.
- If repo docs are updated with results, run `./scripts/bump-version.sh patch`, commit with `docs(core): record fast-open benchmark results`, and tag.

## 5. Measured Perf Target

Target:

- MERIT global cold local `validate_graph_catchments` median **< 1.0 s**.
- MERIT global cold local total open-to-first-delineation wall time for the resolving outlet **< 2.0 s before outlet-specific raster/refinement work dominates**, or document the next measured non-validation stage if it becomes the new bottleneck.
- MERIT pfaf62 cold `validate_graph_catchments` should drop from about 747.5 ms (`docs/investigations/v021-read-perf.md:59`, `docs/investigations/v021-read-perf.md:63`) to a small id-level column scan, expected **< 150 ms**.

Why this is non-vacuous:

- The current expensive validation reads and decodes 6.1 GiB of catchments on global (`docs/investigations/v021-read-perf.md:34`, `docs/investigations/v021-read-perf.md:103`). The new projection should read only the two small scalar columns needed for detection.
- The CI-durable gate is not timing based: test-only instrumentation must count full-row geometry materialization in `extract_units_from_batch()` (`crates/core/src/reader/catchment_store.rs:916`, `crates/core/src/reader/catchment_store.rs:928`), the path current validation uses. The gate must be demonstrated red on the broken route before Step 3 and green after validation switches to `read_id_levels()`.
- Manual benchmark thresholds are acceptance targets, not CI requirements. If a local MERIT dataset is absent, CI still protects the regression with the crate-internal geometry-materialization tests.

## 6. Risks and Uncertainties

- **Level optionality drift:** `full_projection_indices()` currently treats `level` as optional (`crates/core/src/reader/catchment_store.rs:1531`), and `extract_units_from_batch()` defaults missing level to `0` (`crates/core/src/reader/catchment_store.rs:801`). The new validation reader must intentionally be stricter because HFX v0.2.1 requires `level`.
- **Sidecar-hit detection semantics:** A matched remote validation sidecar already skips membership validation, but it still needs `catchment_levels` for snap and query-time checks. This plan only changes how that map is materialized; it does not weaken the sidecar's existing trust boundary.
- **Sidecar-hit coverage:** The sidecar-hit branch is outside `Stage::ValidateGraphCatchments`, so stage-timing benchmarks do not cover it. Step 4 must drive a second remote open through the sidecar-hit branch and assert zero full-row geometry materializations there.
- **Hot-path untouched proof:** `query_by_bbox()` still builds full rows for spatial queries (`crates/core/src/reader/catchment_store.rs:414`), and `query_geometries_by_ids()` still uses the lean `[id, geometry]` projection (`crates/core/src/reader/catchment_store.rs:608`, `crates/core/src/reader/catchment_store.rs:642`, `crates/core/src/reader/catchment_store.rs:1548`). Do not route delineation geometry assembly through the new id-level reader.
- **Remote/local cache behavior:** Local id-index sidecar reuse remains deferred. If measured total open misses the target after geometry decoding is removed, the next smallest follow-up is local persistent id/index or validation sidecar keyed by file size and mtime/hash, but that is intentionally not part of this minimal fix.
- **Benchmark outlet:** The headline command must use a resolving outlet or otherwise avoid unrelated D8 ambiguity. The investigation's Ticino global run reached open telemetry but failed later on `AmbiguousD8Coverage` (`docs/investigations/v021-read-perf.md:75`).
