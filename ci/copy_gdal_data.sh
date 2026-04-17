#!/usr/bin/env bash
# Copies GDAL_DATA and PROJ data from the built native stack into the pyshed
# Python package source tree so that maturin's include glob picks them up when
# building the wheel.
#
# Target: crates/python/python/pyshed/_data/
#   _data/gdal/  — GDAL_DATA (gdalvrt.xsd and friends)
#   _data/proj/  — PROJ data (proj.db and friends)
#
# The _data/ directory is gitignored; this script is safe to re-run between
# builds because it cleans the destination first.
#
# Environment variables consumed:
#   BUILD_PREFIX  — root of the native stack install (set by cibuildwheel)

set -euo pipefail

if [ -z "${BUILD_PREFIX:-}" ]; then
    echo "BUILD_PREFIX not set" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$REPO_ROOT/crates/python/python/pyshed/_data"

echo "staging bundled data from $BUILD_PREFIX into $DEST"
rm -rf "$DEST"
mkdir -p "$DEST/gdal" "$DEST/proj"

cp -R "$BUILD_PREFIX/share/gdal/." "$DEST/gdal/"
cp -R "$BUILD_PREFIX/share/proj/." "$DEST/proj/"

# Sanity sentinels — fail loudly if the upstream build changed layout.
test -f "$DEST/gdal/gdalvrt.xsd" || { echo "missing gdal/gdalvrt.xsd" >&2; exit 1; }
test -f "$DEST/proj/proj.db"     || { echo "missing proj/proj.db"     >&2; exit 1; }

echo "staged $(find "$DEST" -type f | wc -l | tr -d ' ') data files"
