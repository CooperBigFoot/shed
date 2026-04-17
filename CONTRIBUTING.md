# Contributing to shed / pyshed

## Building from source

### Prerequisites

- Rust toolchain (stable) — install via [rustup](https://rustup.rs)
- [maturin](https://github.com/PyO3/maturin) ≥ 1.7 (`pip install maturin`)
- System GDAL — on macOS with Homebrew: `brew install gdal`

### Build the Python extension

```bash
cd crates/python
maturin develop --release
```

This compiles the Rust extension against your system GDAL and installs it into
the active virtual environment. On macOS, Homebrew's GDAL is picked up
automatically via `pkg-config`.

## Running tests

Rust workspace tests:

```bash
cargo test --workspace
```

Python extension tests:

```bash
cd crates/python
pytest tests/ -q
```

## Coding conventions

See [`CLAUDE.md`](CLAUDE.md) for the full coding conventions this project uses
(tracing not log, type-driven design, surgical changes, etc.). All contributions
are expected to follow those conventions.

## Commit and version policy

### Workspace Rust crates

Every commit to the workspace must include a patch version bump:

```bash
./scripts/bump-version.sh patch   # bumps workspace Cargo.toml
```

Stage `Cargo.toml` alongside your code changes and tag after each commit:

```bash
git tag v$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
```

Use `./scripts/bump-version.sh minor` or `major` only when explicitly requested.

### Pyshed exemption

`crates/python/` (`pyshed`) is **exempt** from the per-commit patch-bump rule.
Its version changes only on intentional PyPI releases and uses a separate tag
namespace (`pyshed-v*`) so it does not collide with the workspace `v*` tags.

```bash
# Stable release
./scripts/bump-pyshed-version.sh patch   # 0.1.0 → 0.1.1

# Release candidate (PEP 440 input, SemVer 2.0 written to Cargo.toml)
./scripts/bump-pyshed-version.sh set 0.1.0rc1

# Final release after rc
./scripts/bump-pyshed-version.sh set 0.1.0
```

The `set` mode is required for prereleases because `cargo metadata` rejects
PEP 440 prerelease syntax (`0.1.0rc1`) but accepts SemVer 2.0 (`0.1.0-rc.1`).
The script writes the PEP 440 form to `pyproject.toml` and the SemVer 2.0
equivalent to `Cargo.toml` automatically.

Update `crates/python/CHANGELOG.md` for every pyshed version bump, then tag:

```bash
git tag pyshed-v0.1.0rc1   # use the PEP 440 form for the tag
```

## Wanted: wheel contributions for other platforms

**Current support:** Apple Silicon macOS only (`macosx_11_0_arm64`).

Linux x86_64, Linux aarch64, macOS x86_64, and Windows x86_64 wheels are not
yet built. If you want to help port the build, `ci/config.sh` (added in the
CI phase) is the build-script template — it compiles the full native stack
(GDAL, PROJ, GEOS, libtiff, SQLite, curl, OpenSSL, etc.) from source for a
self-contained wheel. Adding a new platform means:

1. Adding a `runs-on` entry in `.github/workflows/build-wheels.yaml`.
2. Adapting `ci/config.sh` for the target OS/arch (swap Homebrew for a Linux
   package manager or cross-compilation toolchain).
3. Opening a PR with the new job and a sample wheel artifact.

Reach out via a GitHub issue if you want to coordinate before starting.

## Maintainers: first-time PyPI setup

These steps are performed once, then both release paths (TestPyPI for
release candidates, PyPI for real releases) run automatically on tag push.

### 1. Create a PyPI project-scoped API token

Go to https://pypi.org/manage/account/token/ and create a token scoped to
the `pyshed` project (create the project first by uploading once manually,
or use the account-scoped token and tighten after first release). Copy the
token (starts with `pypi-`).

### 2. Create a TestPyPI token

Same flow on https://test.pypi.org/manage/account/token/. Copy that token.

### 3. Store both tokens as GitHub repository secrets

From the repo root:

```bash
gh secret set PYPI_TOKEN     --repo CooperBigFoot/shed  # paste PyPI token
gh secret set TESTPYPI_TOKEN --repo CooperBigFoot/shed  # paste TestPyPI token
```

The `build-wheels.yaml` workflow reads these via `secrets.PYPI_TOKEN` and
`secrets.TESTPYPI_TOKEN`. No GitHub environments are required.

### Rotation

Rotate both tokens on a cadence you're comfortable with (or immediately
after exposure — e.g. if a token ever leaks into a commit, PR, or chat
transcript). Rotation means: revoke the old token on PyPI/TestPyPI, create
a new one, re-run `gh secret set` with the new value.
