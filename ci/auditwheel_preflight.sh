#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 <wheel> | $0 --inside-container <wheel>" >&2
    exit 1
}

if [[ $# -ne 1 && $# -ne 2 ]]; then
    usage
fi

if [[ "${1:-}" == "--inside-container" ]]; then
    if [[ $# -ne 2 ]]; then
        usage
    fi

    WHEEL="$2"

    python -m pip install --upgrade pip
    python -m pip install "$WHEEL"

    EXTENSION_PATH="$(python -c "import pathlib, pyshed._pyshed as mod; print(pathlib.Path(mod.__file__).resolve())")"

    echo "==> ldd $EXTENSION_PATH"
    ldd "$EXTENSION_PATH"

    while IFS= read -r line; do
        case "$line" in
            *"not found"*)
                echo "Missing shared library: $line" >&2
                exit 1
                ;;
            ""|*"linux-vdso.so."*|*"/lib64/ld-linux-"*|*"/lib/ld-linux-"*)
                continue
                ;;
        esac

        lib_name="$(printf '%s\n' "$line" | awk '{print $1}')"
        resolved_path="$(printf '%s\n' "$line" | awk '/=>/ {print $3}')"

        if [[ -z "$resolved_path" || "$resolved_path" == "not" ]]; then
            continue
        fi

        case "$resolved_path" in
            *site-packages*)
                ;;
            /lib/*|/lib64/*|/usr/lib/*|/usr/lib64/*)
                case "$lib_name" in
                    libc.so.*|libcrypt.so.*|libdl.so.*|libgcc_s.so.*|libm.so.*|libnsl.so.*|libpthread.so.*|libresolv.so.*|librt.so.*|libstdc++.so.*|libutil.so.*|libz.so.*)
                        ;;
                    *)
                        echo "Unexpected external dependency: $lib_name => $resolved_path" >&2
                        exit 1
                        ;;
                esac
                ;;
            *)
                echo "Unexpected dependency path: $lib_name => $resolved_path" >&2
                exit 1
                ;;
        esac
    done < <(ldd "$EXTENSION_PATH")

    python -c "
import pyshed
from pathlib import Path
pkg = Path(pyshed.__file__).resolve().parent
assert (pkg / '_data' / 'gdal' / 'gdalvrt.xsd').is_file(), 'missing bundled gdal data'
assert (pkg / '_data' / 'proj' / 'proj.db').is_file(), 'missing bundled proj data'
assert pyshed.__version__, 'missing __version__'
try:
    pyshed.Engine('/nonexistent/path/to/dataset')
except pyshed.DatasetError:
    pass
else:
    raise AssertionError('Engine should reject missing datasets')
from pyshed import _pyshed
_pyshed._self_test_proj()
print('linux wheel smoke test passed; version=' + pyshed.__version__)
"
    exit 0
fi

WHEEL="$1"

if [[ ! -f "$WHEEL" ]]; then
    echo "Wheel not found: $WHEEL" >&2
    exit 1
fi

if ! command -v auditwheel >/dev/null 2>&1; then
    echo "auditwheel is required on the host to inspect repaired wheels" >&2
    exit 1
fi

case "$(basename "$WHEEL")" in
    *manylinux2014_x86_64*.whl|*manylinux_2_17_x86_64*.whl)
        ;;
    *)
        echo "Unexpected wheel tag: $(basename "$WHEEL")" >&2
        exit 1
        ;;
esac

echo "==> auditwheel show $WHEEL"
AUDITWHEEL_OUTPUT="$(auditwheel show "$WHEEL")"
printf '%s\n' "$AUDITWHEEL_OUTPUT"

if printf '%s\n' "$AUDITWHEEL_OUTPUT" | grep -Eiq 'not allowed|cannot repair|linux_x86_64'; then
    echo "auditwheel reported unresolved portability problems" >&2
    exit 1
fi

docker run --rm \
    -v "$(pwd)":/project \
    -w /project \
    python:3.12 \
    bash ./ci/auditwheel_preflight.sh --inside-container "$WHEEL"
