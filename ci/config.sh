#!/usr/bin/env bash
# Builds the native dependency stack (GDAL + friends) for pyshed wheel builds.
#
# Ported from https://github.com/rasterio/rasterio/blob/main/ci/config.sh
# with the following intentional divergences:
#   - macOS arm64 only (no Linux/Windows paths).
#   - Dropped: hdf5, libaec, netcdf, openjpeg, lerc, json-c (external),
#     libwebp, lcms2, giflib, blosc, pcre2, expat — not needed by pyshed.
#   - PROJ_RENAME_SYMBOLS=ON passed as cmake flag (not just CFLAGS) to avoid
#     symbol collisions when rasterio/fiona are co-installed in the same env.
#   - GDAL_USE_GEOS=ON (rasterio disables it on macOS; pyshed needs GEOS for
#     geometry repair).
#   - Minimal GDAL driver set: GTiff, VRT, MEM (raster) + GeoJSON, Shape (OGR).
#
# Environment variables consumed (set by cibuildwheel via CIBW_ENVIRONMENT_MACOS):
#   BUILD_PREFIX           — install root (e.g. ${GITHUB_WORKSPACE}/pyshed_libs)
#   CMAKE_OSX_ARCHITECTURES — e.g. arm64
#   MACOSX_DEPLOYMENT_TARGET — e.g. 11.0
#   GDAL_VERSION           — e.g. 3.12.1 (may be overridden by caller)

set -euo pipefail

# ---------------------------------------------------------------------------
# Pinned versions
# ---------------------------------------------------------------------------
PROJ_VERSION=9.7.1
GDAL_VERSION="${GDAL_VERSION:-3.12.1}"
GEOS_VERSION=3.14.1
TIFF_VERSION=4.7.1
SQLITE_VERSION=3510200
ZLIB_VERSION=1.3.2
XZ_VERSION=5.8.2
CURL_VERSION=8.18.0
OPENSSL_VERSION=3.6.1
NGHTTP2_VERSION=1.68.0
LIBDEFLATE_VERSION=1.24
ZSTD_VERSION=1.5.7
JPEGTURBO_VERSION=3.1.3
LIBPNG_VERSION=1.6.54

# ---------------------------------------------------------------------------
# Build prefix and basic exports
# ---------------------------------------------------------------------------
BUILD_PREFIX="${BUILD_PREFIX:-${GITHUB_WORKSPACE}/pyshed_libs}"

export GDAL_CONFIG="$BUILD_PREFIX/bin/gdal-config"
export PROJ_DATA="$BUILD_PREFIX/share/proj"

echo "BUILD_PREFIX: $BUILD_PREFIX"
echo "GDAL_CONFIG:  $GDAL_CONFIG"
echo "PROJ_DATA:    $PROJ_DATA"

# ---------------------------------------------------------------------------
# Platform detection (macOS arm64 only)
# ---------------------------------------------------------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
Darwin)
    IS_MACOS=1
    lib_ext="dylib"
    ;;
*)
    echo "Unsupported OS: $OS — this script targets macOS arm64 only." >&2
    exit 1
    ;;
esac

echo "Platform: ${OS}-${ARCH}"

# Set arch/optimisation flags for autoconf-based builds.
export CFLAGS="${CFLAGS:-} -arch ${CMAKE_OSX_ARCHITECTURES} -g -O2"
export CXXFLAGS="${CXXFLAGS:-} -arch ${CMAKE_OSX_ARCHITECTURES} -g -O2"

# OpenSSL Configure target
case "${CMAKE_OSX_ARCHITECTURES:-arm64}" in
arm64)   TARGET="darwin64-arm64-cc" ;;
x86_64)  TARGET="darwin64-x86_64-cc" ;;
*)
    echo "Unsupported CMAKE_OSX_ARCHITECTURES: ${CMAKE_OSX_ARCHITECTURES}" >&2
    exit 1
    ;;
esac

# Saved copies — restored by update_env_for_build_prefix on each call.
export CPPFLAGS_BACKUP="${CPPFLAGS:-}"
export LIBRARY_PATH_BACKUP="${LIBRARY_PATH:-}"
export PKG_CONFIG_PATH_BACKUP="${PKG_CONFIG_PATH:-}"

# ---------------------------------------------------------------------------
# Helper: suppress — run command, print output only on failure.
# Verbatim from rasterio/ci/config.sh.
# ---------------------------------------------------------------------------
function suppress {
    local tmp
    tmp=$(mktemp tmp.XXXXXXXXX) || return
    local errexit_set
    echo "Running $*"
    if [[ $- = *e* ]]; then errexit_set=true; fi
    set +e
    (
        if [[ -n ${errexit_set:-} ]]; then set -e; fi
        "$@" >"$tmp" 2>&1
    )
    ret=$?
    [ "$ret" -eq 0 ] || cat "$tmp"
    rm -f "$tmp"
    if [[ -n ${errexit_set:-} ]]; then set -e; fi
    return "$ret"
}

# ---------------------------------------------------------------------------
# Helper: update_env_for_build_prefix — promote BUILD_PREFIX on search paths.
# Verbatim from rasterio/ci/config.sh.
# ---------------------------------------------------------------------------
function update_env_for_build_prefix {
    export CPPFLAGS="-I$BUILD_PREFIX/include $CPPFLAGS_BACKUP"
    export LIBRARY_PATH="$BUILD_PREFIX/lib:$LIBRARY_PATH_BACKUP"
    export PKG_CONFIG_PATH="$BUILD_PREFIX/lib/pkgconfig/:$PKG_CONFIG_PATH_BACKUP"
    export PATH="$BUILD_PREFIX/bin:$PATH"
}

# ---------------------------------------------------------------------------
# Helper: fetch_untar — wget with retry + tar extraction.
# Verbatim from rasterio/ci/config.sh.
# ---------------------------------------------------------------------------
function fetch_untar() {
    local opts="--retry-connrefused \
          --waitretry=30 \
          --dns-timeout=20 \
          --connect-timeout=20 \
          --read-timeout=300 \
          --timeout=300 \
          -t 5"

    if [[ "$#" -eq 1 ]]; then
        # Only URL
        wget $opts "$1"

    elif [[ "$#" -eq 2 ]]; then
        # URL + TAR_FILE (show hash, no check)
        wget $opts "$1"
        local TAR_FILE="$2"
        echo "SHA256: "
        sha256sum "$TAR_FILE"
        if [[ $TAR_FILE == *.gz ]]; then
            tar -xzf "$TAR_FILE"
        elif [[ $TAR_FILE == *.bz2 ]]; then
            tar -xjf "$TAR_FILE"
        else
            echo "Unsupported file type: $TAR_FILE" >&2
            return 1
        fi

    elif [[ "$#" -eq 3 && "$2" == "-O" ]]; then
        # URL + -O new-name (rename, show hash, no check)
        wget $opts "$1" -O "$3"
        local TAR_FILE="$3"
        echo "SHA256: "
        sha256sum "$TAR_FILE"
        if [[ $TAR_FILE == *.gz ]]; then
            tar -xzf "$TAR_FILE"
        elif [[ $TAR_FILE == *.bz2 ]]; then
            tar -xjf "$TAR_FILE"
        else
            echo "Unsupported file type: $TAR_FILE" >&2
            return 1
        fi

    elif [[ "$#" -eq 3 ]]; then
        # URL + TAR_FILE + SHA256 (hash check, no rename)
        wget $opts "$1"
        local TAR_FILE="$2"
        local EXPECTED_HASH="$3"
        local ACTUAL_HASH
        ACTUAL_HASH=$(sha256sum "$TAR_FILE" | cut -d ' ' -f1)

        echo "Expected hash: $EXPECTED_HASH"
        echo "Actual hash:   $ACTUAL_HASH"

        if [ "$EXPECTED_HASH" = "$ACTUAL_HASH" ]; then
            echo "SHA256 hash verified. Extracting..."
            if [[ $TAR_FILE == *.gz ]]; then
                tar -xzf "$TAR_FILE"
            elif [[ $TAR_FILE == *.bz2 ]]; then
                tar -xjf "$TAR_FILE"
            else
                echo "Unsupported file type: $TAR_FILE" >&2
                return 1
            fi
        else
            echo "Hash mismatch! Aborting." >&2
            exit 1
        fi

    elif [[ "$#" -eq 4 && "$2" == "-O" ]]; then
        # URL + -O new-name + SHA256 (rename + hash check)
        wget $opts "$1" -O "$3"
        local TAR_FILE="$3"
        local EXPECTED_HASH="$4"
        local ACTUAL_HASH
        ACTUAL_HASH=$(sha256sum "$TAR_FILE" | cut -d ' ' -f1)

        echo "Expected hash: $EXPECTED_HASH"
        echo "Actual hash:   $ACTUAL_HASH"

        if [ "$EXPECTED_HASH" = "$ACTUAL_HASH" ]; then
            echo "SHA256 hash verified. Extracting..."
            if [[ $TAR_FILE == *.gz ]]; then
                tar -xzf "$TAR_FILE"
            elif [[ $TAR_FILE == *.bz2 ]]; then
                tar -xjf "$TAR_FILE"
            else
                echo "Unsupported file type: $TAR_FILE" >&2
                return 1
            fi
        else
            echo "Hash mismatch! Aborting." >&2
            exit 1
        fi
    fi
}

# ---------------------------------------------------------------------------
# Download all source tarballs upfront
# ---------------------------------------------------------------------------
echo "Downloading source tarballs..."

ZLIB_URL="https://github.com/madler/zlib/releases/download/v${ZLIB_VERSION}/zlib-${ZLIB_VERSION}.tar.gz"
ZLIB_FNAME="zlib-${ZLIB_VERSION}"
ZLIB_SHA256="bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
fetch_untar "${ZLIB_URL}" "${ZLIB_FNAME}.tar.gz" "${ZLIB_SHA256}"

XZ_URL="https://tukaani.org/xz/xz-${XZ_VERSION}.tar.gz"
XZ_FNAME="xz-${XZ_VERSION}"
XZ_SHA256="ce09c50a5962786b83e5da389c90dd2c15ecd0980a258dd01f70f9e7ce58a8f1"
fetch_untar "${XZ_URL}" "${XZ_FNAME}.tar.gz" "${XZ_SHA256}"

LIBDEFLATE_URL="https://github.com/ebiggers/libdeflate/archive/refs/tags/v${LIBDEFLATE_VERSION}.tar.gz"
LIBDEFLATE_FNAME="libdeflate-${LIBDEFLATE_VERSION}"
fetch_untar "${LIBDEFLATE_URL}" -O "${LIBDEFLATE_FNAME}.tar.gz"

ZSTD_URL="https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz"
ZSTD_FNAME="zstd-${ZSTD_VERSION}"
fetch_untar "${ZSTD_URL}" -O "${ZSTD_FNAME}.tar.gz"

JPEGTURBO_URL="https://github.com/libjpeg-turbo/libjpeg-turbo/releases/download/${JPEGTURBO_VERSION}/libjpeg-turbo-${JPEGTURBO_VERSION}.tar.gz"
JPEGTURBO_FNAME="libjpeg-turbo-${JPEGTURBO_VERSION}"
fetch_untar "${JPEGTURBO_URL}" "${JPEGTURBO_FNAME}.tar.gz"

LIBPNG_URL="https://github.com/pnggroup/libpng/archive/refs/tags/v${LIBPNG_VERSION}.tar.gz"
LIBPNG_FNAME="libpng-${LIBPNG_VERSION}"
fetch_untar "${LIBPNG_URL}" -O "${LIBPNG_FNAME}.tar.gz"

NGHTTP2_URL="https://github.com/nghttp2/nghttp2/releases/download/v${NGHTTP2_VERSION}/nghttp2-${NGHTTP2_VERSION}.tar.gz"
NGHTTP2_FNAME="nghttp2-${NGHTTP2_VERSION}"
fetch_untar "${NGHTTP2_URL}" "${NGHTTP2_FNAME}.tar.gz"

OPENSSL_URL="https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/openssl-${OPENSSL_VERSION}.tar.gz"
OPENSSL_FNAME="openssl-${OPENSSL_VERSION}"
OPENSSL_SHA256="b1bfedcd5b289ff22aee87c9d600f515767ebf45f77168cb6d64f231f518a82e"
fetch_untar "${OPENSSL_URL}" "${OPENSSL_FNAME}.tar.gz" "${OPENSSL_SHA256}"

CURL_URL="https://curl.se/download/curl-${CURL_VERSION}.tar.gz"
CURL_FNAME="curl-${CURL_VERSION}"
CURL_SHA256="e9274a5f8ab5271c0e0e6762d2fce194d5f98acc568e4ce816845b2dcc0cf88f"
fetch_untar "${CURL_URL}" "${CURL_FNAME}.tar.gz" "${CURL_SHA256}"

SQLITE_URL="https://www.sqlite.org/2026/sqlite-autoconf-${SQLITE_VERSION}.tar.gz"
SQLITE_FNAME="sqlite-autoconf-${SQLITE_VERSION}"
fetch_untar "${SQLITE_URL}" "${SQLITE_FNAME}.tar.gz"

PROJ_URL="https://download.osgeo.org/proj/proj-${PROJ_VERSION}.tar.gz"
PROJ_FNAME="proj-${PROJ_VERSION}"
fetch_untar "${PROJ_URL}" "${PROJ_FNAME}.tar.gz"

GEOS_URL="https://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2"
GEOS_FNAME="geos-${GEOS_VERSION}"
fetch_untar "${GEOS_URL}" "${GEOS_FNAME}.tar.bz2"

TIFF_URL="https://download.osgeo.org/libtiff/tiff-${TIFF_VERSION}.tar.gz"
TIFF_FNAME="tiff-${TIFF_VERSION}"
fetch_untar "${TIFF_URL}" "${TIFF_FNAME}.tar.gz"

GDAL_URL="https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz"
GDAL_FNAME="gdal-${GDAL_VERSION}"
fetch_untar "${GDAL_URL}" "${GDAL_FNAME}.tar.gz"

# ---------------------------------------------------------------------------
# Build functions
# ---------------------------------------------------------------------------

function build_zlib {
    if [ -e zlib-stamp ]; then return; fi
    echo "Running build_zlib"
    (cd "${ZLIB_FNAME}" &&
        ./configure --prefix="$BUILD_PREFIX" &&
        make &&
        make install)
    touch zlib-stamp
}

function build_xz {
    if [ -e xz-stamp ]; then return; fi
    echo "Running build_xz"
    (cd "${XZ_FNAME}" &&
        ./configure --prefix="$BUILD_PREFIX" &&
        make &&
        make install)
    touch xz-stamp
}

function build_libdeflate {
    if [ -e libdeflate-stamp ]; then return; fi
    echo "Running build_libdeflate"
    local cmake=cmake
    (cd "${LIBDEFLATE_FNAME}" &&
        mkdir build && cd build &&
        $cmake .. \
            -DCMAKE_INSTALL_PREFIX:PATH="$BUILD_PREFIX" \
            -DCMAKE_PREFIX_PATH="${BUILD_PREFIX}" \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET}" \
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}" \
            -DBUILD_SHARED_LIBS=ON \
            -DCMAKE_BUILD_TYPE=Release &&
        $cmake --build . -j4 &&
        $cmake --install .)
    touch libdeflate-stamp
}

function build_zstd {
    if [ -e zstd-stamp ]; then return; fi
    echo "Running build_zstd"
    local cmake=cmake
    local sed_ere_opt="-E"
    (cd "${ZSTD_FNAME}/build/cmake" &&
        $cmake . \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX:PATH="$BUILD_PREFIX" \
            -DCMAKE_PREFIX_PATH="${BUILD_PREFIX}" \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET}" \
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}" \
            -DZSTD_LEGACY_SUPPORT=0 \
            -DSED_ERE_OPT="$sed_ere_opt" &&
        $cmake --build . &&
        $cmake --install .)
    touch zstd-stamp
}

function build_jpegturbo {
    if [ -e jpeg-stamp ]; then return; fi
    echo "Running build_jpegturbo"
    local cmake=cmake
    (cd "${JPEGTURBO_FNAME}" &&
        $cmake -G "Unix Makefiles" \
            -DCMAKE_INSTALL_PREFIX="$BUILD_PREFIX" \
            -DCMAKE_PREFIX_PATH="$BUILD_PREFIX" \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET}" \
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}" \
            -DCMAKE_INSTALL_LIBDIR="$BUILD_PREFIX/lib" \
            -DCMAKE_INSTALL_NAME_DIR="$BUILD_PREFIX/lib" \
            -DWITH_JPEG8=1 \
            . &&
        make -j4 &&
        make install)
    touch jpeg-stamp
}

function build_libpng {
    if [ -e libpng-stamp ]; then return; fi
    build_zlib
    echo "Running build_libpng"
    (cd "${LIBPNG_FNAME}" &&
        ./configure --prefix="$BUILD_PREFIX" &&
        make &&
        make install)
    touch libpng-stamp
}

function build_nghttp2 {
    if [ -e nghttp2-stamp ]; then return; fi
    echo "Running build_nghttp2"
    (cd "${NGHTTP2_FNAME}" &&
        ./configure --enable-lib-only --prefix="$BUILD_PREFIX" &&
        make -j4 &&
        make install)
    touch nghttp2-stamp
}

function build_openssl {
    if [ -e openssl-stamp ]; then return; fi
    echo "Running build_openssl"
    (cd "${OPENSSL_FNAME}" &&
        ./Configure "$TARGET" -fPIC --prefix="$BUILD_PREFIX" &&
        make -j4 &&
        make install)
    touch openssl-stamp
}

function build_curl {
    if [ -e curl-stamp ]; then return; fi
    suppress build_openssl
    build_nghttp2
    echo "Running build_curl"
    local flags="--prefix=$BUILD_PREFIX --with-nghttp2=$BUILD_PREFIX --with-zlib=$BUILD_PREFIX --with-ssl=$BUILD_PREFIX --enable-shared --without-libidn2 --without-libpsl"
    (cd "${CURL_FNAME}" &&
        DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-}:$BUILD_PREFIX/lib" ./configure $flags &&
        make -j4 &&
        make install)
    touch curl-stamp
}

function build_sqlite {
    if [ -e sqlite-stamp ]; then return; fi
    echo "Running build_sqlite"
    (cd "${SQLITE_FNAME}" &&
        ./configure --enable-rtree --enable-threadsafe --prefix="$BUILD_PREFIX" &&
        make &&
        make install)
    touch sqlite-stamp
}

function build_proj {
    if [ -e proj-stamp ]; then return; fi
    echo "Running build_proj"
    # -DPROJ_RENAME_SYMBOLS=ON prevents symbol collisions when rasterio/fiona
    # are co-installed in the same Python environment — a pyshed-specific
    # requirement not present in rasterio's own build.
    local cmake=cmake
    (cd "${PROJ_FNAME}" &&
        $cmake . \
            -DCMAKE_INSTALL_PREFIX:PATH="$BUILD_PREFIX" \
            -DCMAKE_PREFIX_PATH="${BUILD_PREFIX}" \
            -DCMAKE_INCLUDE_PATH="$BUILD_PREFIX/include" \
            -DSQLite3_INCLUDE_DIR="$BUILD_PREFIX/include" \
            -DSQLite3_LIBRARY="$BUILD_PREFIX/lib/libsqlite3.$lib_ext" \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET}" \
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}" \
            -DBUILD_SHARED_LIBS=ON \
            -DCMAKE_BUILD_TYPE=Release \
            -DENABLE_IPO=ON \
            -DPROJ_RENAME_SYMBOLS=ON \
            -DBUILD_APPS:BOOL=OFF \
            -DBUILD_TESTING:BOOL=OFF &&
        $cmake --build . -j4 &&
        $cmake --install .)
    touch proj-stamp
}

function build_geos {
    if [ -e geos-stamp ]; then return; fi
    echo "Running build_geos"
    local cmake=cmake
    (cd "${GEOS_FNAME}" &&
        mkdir build && cd build &&
        $cmake .. \
            -DCMAKE_INSTALL_PREFIX:PATH="$BUILD_PREFIX" \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET}" \
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}" \
            -DBUILD_SHARED_LIBS=ON \
            -DCMAKE_BUILD_TYPE=Release \
            -DENABLE_IPO=ON \
            -DBUILD_APPS:BOOL=OFF \
            -DBUILD_TESTING:BOOL=OFF &&
        $cmake --build . -j4 &&
        $cmake --install .)
    touch geos-stamp
}

function build_tiff {
    if [ -e tiff-stamp ]; then return; fi
    build_jpegturbo
    build_zlib
    build_zstd
    build_xz
    echo "Running build_tiff"
    (cd "${TIFF_FNAME}" &&
        ./configure --prefix="$BUILD_PREFIX" \
            --libdir="$BUILD_PREFIX/lib" \
            --enable-zstd \
            --with-jpeg-include-dir="$BUILD_PREFIX/include" \
            --with-jpeg-lib-dir="$BUILD_PREFIX/lib" &&
        make -j4 &&
        make install)
    touch tiff-stamp
}

function build_gdal {
    if [ -e gdal-stamp ]; then return; fi
    echo "Running build_gdal"
    # Pass PROJ_RENAME_SYMBOLS defines so that GDAL's PROJ usage also uses the
    # renamed symbols — required when PROJ was built with PROJ_RENAME_SYMBOLS=ON.
    CFLAGS="$CFLAGS -DPROJ_RENAME_SYMBOLS"
    CXXFLAGS="$CXXFLAGS -DPROJ_RENAME_SYMBOLS -DPROJ_INTERNAL_CPP_NAMESPACE"

    local cmake=cmake
    (cd "${GDAL_FNAME}" &&
        mkdir build &&
        cd build &&
        $cmake .. \
            -DCMAKE_INSTALL_PREFIX="$BUILD_PREFIX" \
            -DCMAKE_PREFIX_PATH="${BUILD_PREFIX}" \
            -DCMAKE_INCLUDE_PATH="$BUILD_PREFIX/include" \
            -DCMAKE_LIBRARY_PATH="$BUILD_PREFIX/lib" \
            -DCMAKE_PROGRAM_PATH="$BUILD_PREFIX/bin" \
            -DCMAKE_OSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET}" \
            -DCMAKE_OSX_ARCHITECTURES="${CMAKE_OSX_ARCHITECTURES}" \
            -DBUILD_SHARED_LIBS=ON \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
            -DSQLite3_INCLUDE_DIR="$BUILD_PREFIX/include" \
            -DSQLite3_LIBRARY="$BUILD_PREFIX/lib/libsqlite3.$lib_ext" \
            -DGDAL_BUILD_OPTIONAL_DRIVERS=OFF \
            -DOGR_BUILD_OPTIONAL_DRIVERS=OFF \
            -DGDAL_ENABLE_DRIVER_GTIFF=ON \
            -DGDAL_ENABLE_DRIVER_VRT=ON \
            -DGDAL_ENABLE_DRIVER_MEM=ON \
            -DOGR_ENABLE_DRIVER_GEOJSON=ON \
            -DOGR_ENABLE_DRIVER_SHAPE=ON \
            -DGDAL_USE_GEOS=ON \
            -DGDAL_USE_TIFF=ON \
            -DGDAL_USE_GEOTIFF_INTERNAL=ON \
            -DGDAL_USE_TIFF_INTERNAL=OFF \
            -DGDAL_USE_CURL=ON \
            -DGDAL_USE_SQLITE3=ON \
            -DGDAL_USE_PROJ=ON \
            -DGDAL_USE_JSONC_INTERNAL=ON \
            -DGDAL_USE_HDF5=OFF \
            -DGDAL_USE_NETCDF=OFF \
            -DGDAL_USE_OPENJPEG=OFF \
            -DGDAL_USE_WEBP=OFF \
            -DGDAL_USE_LERC=OFF \
            -DGDAL_USE_PCRE2=OFF \
            -DGDAL_USE_JXL=OFF \
            -DGDAL_USE_HEIF=OFF \
            -DGDAL_USE_OPENEXR=OFF \
            -DGDAL_USE_POSTGRESQL=OFF \
            -DGDAL_USE_ODBC=OFF \
            -DGDAL_USE_XERCESC=OFF \
            -DGDAL_USE_LIBXML2=OFF \
            -DGDAL_USE_ICONV=ON \
            -DBUILD_PYTHON_BINDINGS=OFF \
            -DBUILD_JAVA_BINDINGS=OFF \
            -DBUILD_CSHARP_BINDINGS=OFF \
            -DBUILD_TESTING=OFF \
            -DGDAL_BUILD_DOCS=OFF &&
        $cmake --build . -j4 &&
        $cmake --install .)
    touch gdal-stamp
}

# ---------------------------------------------------------------------------
# Build sequence
# ---------------------------------------------------------------------------
echo "Compiling native stack..."

suppress update_env_for_build_prefix
build_zlib
suppress build_xz
build_libdeflate
build_zstd
build_jpegturbo
build_libpng
suppress build_nghttp2
# Remove any Homebrew curl that may have leaked through.
rm -rf "$BUILD_PREFIX/lib/libcurl"* || true
suppress build_curl
build_sqlite
build_tiff
build_proj
suppress build_geos
build_gdal

echo ""
echo "Contents of $BUILD_PREFIX/lib:"
ls "$BUILD_PREFIX/lib"

echo ""
echo "GDAL version:"
"$GDAL_CONFIG" --version
