# Claude Agent Guidelines

## Project Overview

`shed` is intended to be the watershed extraction engine that consumes compiled HFX datasets from the sibling `../hfx` repository. `../hfx` defines the open HydroFabric Exchange contract, validator, and adapter-side normalization rules; this repository is the runtime engine that should load those HFX artifacts and perform outlet resolution, upstream traversal, terminal refinement, and final watershed geometry extraction.

Treat `../hfx/spec/HFX_SPEC.md` as the canonical contract for on-disk inputs. In practical terms, `shed` should read `manifest.json`, `catchments.parquet`, `graph.arrow`, and optionally `snap.parquet`, `flow_dir.tif`, and `flow_acc.tif`, while keeping all source-fabric-specific logic out of the hot path. If a question comes up about file layout, schema, topology semantics, snapping rules, or raster refinement behavior, read `../hfx` first and align this repo with the spec rather than inventing a parallel contract here.

## Behavioral Guidelines

These guidelines are guardrails against common LLM coding mistakes. They bias toward caution over speed, but use judgment for trivial tasks.

### Think Before Coding

- State assumptions explicitly.
- If a request has multiple plausible interpretations, surface them instead of silently picking one.
- Prefer the simpler approach when it solves the problem.
- If the task depends on HFX contract details, check `../hfx` first rather than guessing.

### Simplicity First

- Write the minimum code that solves the requested problem.
- Do not add speculative abstractions, configurability, or future-proofing.
- Do not add handling for scenarios that are impossible or outside the request.
- If the solution feels overbuilt, simplify it before shipping.

### Surgical Changes

- Touch only the lines required for the request.
- Do not refactor, reformat, or "clean up" adjacent code unless the change requires it.
- Match the existing local style and structure.
- Remove only unused code or imports created by your own change; mention unrelated cleanup separately.

### Goal-Driven Execution

- Translate requests into concrete checks or acceptance criteria before implementing.
- For multi-step tasks, keep a short plan with a verification step for each phase.
- Bug fixes should be tied to a reproduction or regression test when practical.
- Behavior changes should end with the smallest useful verification: tests, build, or a concrete manual check.

## Version Bumping (mandatory)

**Every commit MUST include a patch version bump.** No exceptions.

Before committing, follow this exact sequence:

1. `./scripts/bump-version.sh patch` — modifies `Cargo.toml` version field
2. Stage `Cargo.toml` alongside code changes
3. Commit with a conventional commit message
4. `git tag v$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')` — tag the commit

**Rules:**

- **Patch bumps**: Automatic with every commit. Claude MUST do this.
- **Minor/major bumps**: Only when the user explicitly requests. Use `./scripts/bump-version.sh minor` or `./scripts/bump-version.sh major`.
- **Never let tooling create its own commit or tag.** Fold version changes into the real commit.
- **Always tag** after every commit.

> **Note:** `cargo bump` does not support Cargo workspaces (it panics). Use `./scripts/bump-version.sh` instead — it edits `Cargo.toml` directly.

### Pyshed exemption

The `pyshed` crate (`crates/python/`) is **exempt** from the per-commit patch-bump rule. Its
version evolves only on intentional PyPI releases and uses a separate tag namespace:

- Bump with `./scripts/bump-pyshed-version.sh <patch|minor|major|set <PEP440-version>>`.
- The `set` mode is required for prereleases (e.g. `set 0.1.0rc1`) — `cargo metadata` rejects
  PEP 440 prerelease syntax, so the script writes the SemVer 2.0 equivalent (`0.1.0-rc.1`)
  to `Cargo.toml` while writing the PEP 440 form (`0.1.0rc1`) to `pyproject.toml`.
- Tag pyshed releases with the `pyshed-v*` prefix (e.g. `pyshed-v0.1.0rc1`,
  `pyshed-v0.1.0`) to avoid colliding with the workspace's `v*` tags.

### Quick Reference

| Command | Effect |
|---|---|
| `./scripts/bump-version.sh patch` | `0.1.0` → `0.1.1` |
| `./scripts/bump-version.sh minor` | `0.1.1` → `0.2.0` |
| `./scripts/bump-version.sh major` | `0.2.0` → `1.0.0` |
| `grep '^version' Cargo.toml` | Show current version |

## Rust Coding Conventions

### Logging: `tracing`, not `log`

Use the `tracing` crate exclusively. Never use `println!` or the `log` crate for diagnostics.

```rust
use tracing::{info, debug, warn, error, instrument};

#[instrument(skip(raster))]
pub fn snap_pour_point(point: &Coord, raster: &FlowDir) -> Result<Coord> {
    debug!(x = point.x, y = point.y, "snapping pour point");
    // ...
    info!(snapped_x = result.x, snapped_y = result.y, "pour point snapped");
    Ok(result)
}
```

- Use structured fields (`key = value`) over format strings.
- Use `#[instrument]` on public functions. Use `skip` for large args.
- Levels: `error` = broken, `warn` = degraded, `info` = milestones, `debug` = internals, `trace` = hot loops.

### Error Handling

- **Library code** (`crates/`): Use `thiserror`. Every variant gets a doc comment explaining _when_ it fires. Use named fields, not tuples.
- **Application code** (`src/`): Use `anyhow` with `.context()` for enriched backtraces.
- **Never `.unwrap()` or `.expect()` in library code.** In `main.rs` / CLI glue, `.expect("reason")` is acceptable for truly unrecoverable situations.

```rust
/// Errors from pour-point snapping.
#[derive(Debug, thiserror::Error)]
pub enum SnapError {
    /// Returned when no flow-accumulation cell exceeds the threshold
    /// within the search radius.
    #[error("no cell above threshold {threshold} within {radius} cells of ({x}, {y})")]
    NoCellAboveThreshold {
        /// Minimum accumulation value required.
        threshold: f64,
        /// Search radius in grid cells.
        radius: usize,
        /// X coordinate of the input point.
        x: f64,
        /// Y coordinate of the input point.
        y: f64,
    },
}
```

### Documentation — LLM-Agent-First, Intentional

Documentation exists to help LLM agents (and humans) navigate the codebase. It is NOT decoration. Apply it proportionally to complexity.

#### When to document

- **Simple module (<~150 lines, readable code)**: A one-line `//!` purpose comment at the top is enough. The code speaks for itself.
- **Complex crate (multiple files, non-obvious interactions)**: Add a `README.md` at the crate root (`crates/foo/README.md`). This is the primary entry point for an agent exploring the crate.
- **Domain-specific or algorithmic code**: Document the _why_ and the _domain context_ an agent wouldn't know from reading the code alone.

#### Crate-level README (for complex crates only)

Place a `README.md` in the crate directory. Structure it for an agent that just landed in the crate and needs to orient fast:

- **Purpose**: One paragraph — what problem does this crate solve.
- **Architecture**: Mermaid diagram showing how the modules/files relate.
- **Glossary**: Table of domain terms, abbreviations, or math symbols used in the code.
- **Key types**: Which structs/enums are the main entry points.

#### Function / type docs

- First line: single imperative sentence (what it does).
- Add detail only when the code isn't self-evident — algorithms, formulas, domain logic.
- `# Errors` table for fallible public functions.
- `# Panics` section if debug-asserts exist.
- Use [`backtick links`] to cross-reference types.
- **Skip doc comments on obvious helpers, private internals, and trivial getters.**

#### Diagrams

- **Use Mermaid, never ASCII art.** For architecture, data flow, state machines — always ` ```mermaid ` blocks.
- Put diagrams in crate READMEs, not in inline doc comments (keeps `.rs` files lean).

### Type Driven Development (strict)

**Principle:** Encode domain invariants in the type system. Invalid states must be unrepresentable at compile time. Types are the first line of documentation and the first line of defense — they steer both humans and LLM agents toward correct code by making wrong code fail to compile.

#### Parse, don't validate (hard rule)

Raw input (strings, numbers from files/CLI/APIs) is converted into typed domain representations **at the system boundary**. Internal functions never accept raw primitives when a domain type exists.

```rust
// WRONG — raw primitive leaks into domain logic
fn delineate(comid: u64, lat: f64, lon: f64) { ... }

// RIGHT — parsed at the boundary, domain types from here on
fn delineate(comid: Comid, pour_point: GeoCoord) -> Result<Watershed, DelineateError> { ... }
```

Parsing happens once, at the edge. Everything downstream receives types that are **valid by construction**.

#### Newtype wrappers

Wrap values where confusion between semantically different quantities is plausible:

- Coordinates (grid vs. geographic), IDs, thresholds, distances, indices
- If two `f64` parameters could be accidentally swapped, they need distinct types

Bare primitives are fine for truly unambiguous locals (loop counters, intermediate arithmetic).

```rust
/// A geographic coordinate in EPSG:4326.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoCoord { pub lon: f64, pub lat: f64 }

/// A cell position in raster grid space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridCoord { pub col: usize, pub row: usize }
```

A function accepting `GeoCoord` cannot be called with a `GridCoord` — the compiler enforces it.

#### Enums over booleans

Never use `bool` to represent a domain state with two named possibilities. Use an enum.

```rust
// WRONG
fn trace(upstream: bool) { ... }

// RIGHT
enum TraceDirection { Upstream, Downstream }
fn trace(direction: TraceDirection) { ... }
```

This applies to struct fields, function parameters, and return values. A `bool` says nothing about intent; an enum is self-documenting.

#### Typestate pattern (use when it matters)

Use zero-size-type generics to enforce valid state transitions at compile time. Apply this to **pipelines, multi-step workflows, and resources with a lifecycle** — not to every struct.

```rust
struct Unfitted;
struct Fitted;

struct Pipeline<State = Unfitted> {
    config: PipelineConfig,
    _state: std::marker::PhantomData<State>,
}

impl Pipeline<Unfitted> {
    fn fit(self, data: &TrainingData) -> Result<Pipeline<Fitted>> { ... }
}

impl Pipeline<Fitted> {
    fn predict(&self, input: &InputData) -> Result<Prediction> { ... }
}

// Pipeline::new().predict() → compile error
// Pipeline::new().fit(data)?.predict() → compiles
```

Good candidates for typestate: delineation pipeline stages, raster processing chains, anything where calling methods out of order is a logic bug.

#### Summary of rules

| Rule | Strictness |
|---|---|
| Parse, don't validate | **Hard rule** — no raw primitives past the boundary |
| Newtype wrappers | Wrap where confusion is plausible; bare primitives OK for unambiguous locals |
| Enums over booleans | **Always** — no `bool` for domain states |
| Typestate pattern | Use for pipelines and lifecycles; don't force it everywhere |

### Code Style

- **Prefer iterators** over indexed loops. Use `.iter()`, `.map()`, `.filter()`, `.collect()`.
- **Derive liberally**: `#[derive(Debug, Clone, PartialEq)]` on all public types unless there's a reason not to.
- **Builder pattern** for config structs with more than 3 fields — chainable `with_*` methods returning `Self`.
- **Struct field visibility**: Keep fields private, expose via methods. Public fields only for plain-data / config types.
- **Math-friendly names are allowed** in algorithm code (e.g., `dx`, `dy`, `acc`, `phi`), but add a glossary in the module doc.
- **No `use super::*`** — explicit imports only.
- **Group imports**: std → external crates → crate-internal, separated by blank lines.
