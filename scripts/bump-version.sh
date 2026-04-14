#!/usr/bin/env bash
set -euo pipefail

CARGO_TOML="$(cd "$(dirname "$0")/.." && pwd)/Cargo.toml"

usage() {
    echo "Usage: $0 <patch|minor|major>" >&2
    exit 1
}

if [[ $# -ne 1 ]]; then
    usage
fi

BUMP="$1"

if [[ "$BUMP" != "patch" && "$BUMP" != "minor" && "$BUMP" != "major" ]]; then
    echo "Error: argument must be 'patch', 'minor', or 'major' (got '$BUMP')" >&2
    usage
fi

# Read current version from workspace Cargo.toml
CURRENT_VERSION="$(grep '^version' "$CARGO_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/')"

# Parse into components
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Increment the appropriate component
case "$BUMP" in
    patch)
        PATCH=$(( PATCH + 1 ))
        ;;
    minor)
        MINOR=$(( MINOR + 1 ))
        PATCH=0
        ;;
    major)
        MAJOR=$(( MAJOR + 1 ))
        MINOR=0
        PATCH=0
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"

# Edit Cargo.toml in-place (compatible with both macOS and GNU sed)
sed -i.bak "s/^version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" "$CARGO_TOML"
rm -f "${CARGO_TOML}.bak"

echo "${CURRENT_VERSION} -> ${NEW_VERSION}"
