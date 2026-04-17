#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYPROJECT="$REPO_ROOT/crates/python/pyproject.toml"
CARGO="$REPO_ROOT/crates/python/Cargo.toml"

usage() {
    echo "Usage: $0 <patch|minor|major|set <PEP440-version>>" >&2
    echo "" >&2
    echo "  patch|minor|major  bump the release segment (X.Y.Z) in both files" >&2
    echo "  set <version>      write version verbatim to pyproject.toml; write the" >&2
    echo "                     SemVer 2.0 equivalent to Cargo.toml" >&2
    echo "" >&2
    echo "PEP 440 → SemVer 2.0 mapping used by 'set':" >&2
    echo "  0.1.0       → 0.1.0" >&2
    echo "  0.1.0rc1    → 0.1.0-rc.1" >&2
    echo "  0.1.0a1     → 0.1.0-alpha.1" >&2
    echo "  0.1.0b1     → 0.1.0-beta.1" >&2
    echo "  0.1.0.post1 → 0.1.0+post.1" >&2
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

MODE="$1"

# Read the release segment (X.Y.Z) from pyproject.toml
read_pyproject_version() {
    grep '^version' "$PYPROJECT" | head -1 | sed 's/version = "\(.*\)"/\1/'
}

# Extract only the X.Y.Z release segment (strip any pre/post suffix)
release_segment() {
    python3 -c "
import re, sys
v = sys.argv[1]
m = re.match(r'^(\d+\.\d+\.\d+)', v)
if not m:
    sys.exit('cannot parse version: ' + v)
print(m.group(1))
" "$1"
}

# Convert a PEP 440 version to its SemVer 2.0 equivalent for Cargo.toml
pep440_to_semver() {
    python3 -c "
import re, sys
v = sys.argv[1]
# pre-release: a/b/rc
m = re.match(r'^(\d+\.\d+\.\d+)(a|b|rc)(\d+)$', v)
if m:
    pre = {'a': 'alpha', 'b': 'beta', 'rc': 'rc'}[m.group(2)]
    print(f'{m.group(1)}-{pre}.{m.group(3)}'); sys.exit(0)
# post-release
m = re.match(r'^(\d+\.\d+\.\d+)\.post(\d+)$', v)
if m:
    print(f'{m.group(1)}+post.{m.group(2)}'); sys.exit(0)
# plain release
if re.match(r'^\d+\.\d+\.\d+$', v):
    print(v); sys.exit(0)
sys.exit('unsupported version form: ' + v)
" "$1"
}

# Write version to pyproject.toml (replaces the version = "..." line under [project])
write_pyproject() {
    local new_ver="$1"
    local current
    current="$(read_pyproject_version)"
    sed -i.bak "s/^version = \"${current}\"/version = \"${new_ver}\"/" "$PYPROJECT"
    rm -f "${PYPROJECT}.bak"
}

# Write version to Cargo.toml (replaces the version = "..." line under [package])
write_cargo() {
    local new_ver="$1"
    local current
    current="$(grep '^version' "$CARGO" | head -1 | sed 's/version = "\(.*\)"/\1/')"
    sed -i.bak "s/^version = \"${current}\"/version = \"${new_ver}\"/" "$CARGO"
    rm -f "${CARGO}.bak"
}

case "$MODE" in
    patch|minor|major)
        CURRENT_PEP="$(read_pyproject_version)"
        RELEASE="$(release_segment "$CURRENT_PEP")"
        IFS='.' read -r MAJ MIN PAT <<< "$RELEASE"
        case "$MODE" in
            patch) PAT=$(( PAT + 1 )) ;;
            minor) MIN=$(( MIN + 1 )); PAT=0 ;;
            major) MAJ=$(( MAJ + 1 )); MIN=0; PAT=0 ;;
        esac
        NEW_VER="${MAJ}.${MIN}.${PAT}"
        write_pyproject "$NEW_VER"
        write_cargo "$NEW_VER"
        echo "pyproject.toml: ${CURRENT_PEP} -> ${NEW_VER}"
        echo "Cargo.toml:     ${CURRENT_PEP} -> ${NEW_VER}"
        ;;
    set)
        if [[ $# -ne 2 ]]; then
            echo "Error: 'set' requires a version argument (e.g. $0 set 0.1.0rc1)" >&2
            usage
        fi
        PEP_VER="$2"
        SEMVER="$(pep440_to_semver "$PEP_VER")"
        CURRENT_PEP="$(read_pyproject_version)"
        CURRENT_CARGO="$(grep '^version' "$CARGO" | head -1 | sed 's/version = "\(.*\)"/\1/')"
        write_pyproject "$PEP_VER"
        write_cargo "$SEMVER"
        echo "pyproject.toml: ${CURRENT_PEP} -> ${PEP_VER}"
        echo "Cargo.toml:     ${CURRENT_CARGO} -> ${SEMVER}"
        ;;
    *)
        echo "Error: unknown mode '${MODE}'" >&2
        usage
        ;;
esac

echo ""
echo "Don't forget to update crates/python/CHANGELOG.md and tag with:"
echo "  git tag pyshed-v${PEP_VER:-${NEW_VER}}"
