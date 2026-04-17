#!/usr/bin/env bash
set -euo pipefail

if [ -z "${BUILD_PREFIX:-}" ]; then
    echo "BUILD_PREFIX not set" >&2
    exit 1
fi

LIB_DIR="$BUILD_PREFIX/lib"

check_glob() {
    local pattern="$1"
    local matched=0

    while IFS= read -r lib_path; do
        [ -n "$lib_path" ] || continue
        matched=1

        echo "==> otool -D $lib_path"
        otool -D "$lib_path"
        echo "==> otool -L $lib_path"
        otool -L "$lib_path"

        local install_name
        install_name=$(otool -D "$lib_path" | awk 'NR==2 {print $1}')
        if [ -z "$install_name" ]; then
            echo "Missing LC_ID_DYLIB install-name for $lib_path" >&2
            exit 1
        fi

        case "$install_name" in
            /*|@rpath/*|@loader_path/*)
                ;;
            *)
                echo "Bare or unsupported LC_ID_DYLIB install-name for $lib_path: $install_name" >&2
                exit 1
                ;;
        esac
    done < <(find "$LIB_DIR" -maxdepth 1 -type f -name "$pattern" -print | sort)

    if [ "$matched" -eq 0 ]; then
        echo "No dylibs matched $pattern in $LIB_DIR" >&2
        exit 1
    fi
}

check_glob "libgdal*.dylib"
check_glob "libproj*.dylib"
check_glob "libgeos_c*.dylib"
check_glob "libtiff*.dylib"
check_glob "libsqlite3*.dylib"
