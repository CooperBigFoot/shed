# Third-Party Licenses

The `pyshed` wheel bundles the following native libraries. Their license texts
are in [`LICENSES/`](LICENSES/).

| Library | Version | SPDX Identifier | Upstream URL | License file | Bundled in wheel? |
|---|---|---|---|---|---|
| GDAL | 3.12.1 | MIT | https://github.com/OSGeo/gdal | [LICENSES/GDAL.txt](LICENSES/GDAL.txt) | Y |
| PROJ | 9.7.1 | MIT | https://github.com/OSGeo/PROJ | [LICENSES/PROJ.txt](LICENSES/PROJ.txt) | Y |
| GEOS | 3.14.1 | LGPL-2.1-only | https://github.com/libgeos/geos | [LICENSES/GEOS.txt](LICENSES/GEOS.txt) | Y |
| libtiff | 4.7.1 | libtiff | https://gitlab.com/libtiff/libtiff | [LICENSES/libtiff.txt](LICENSES/libtiff.txt) | Y |
| SQLite | 3.51.0 | blessing | https://www.sqlite.org | [LICENSES/sqlite.txt](LICENSES/sqlite.txt) | Y |
| zlib | 1.3.2 | Zlib | https://www.zlib.net | [LICENSES/zlib.txt](LICENSES/zlib.txt) | Y |
| xz / liblzma | 5.8.2 | 0BSD | https://tukaani.org/xz/ | [LICENSES/xz.txt](LICENSES/xz.txt) | Y |
| libdeflate | 1.24 | MIT | https://github.com/ebiggers/libdeflate | [LICENSES/libdeflate.txt](LICENSES/libdeflate.txt) | Y |
| Zstandard | 1.5.7 | BSD-3-Clause | https://github.com/facebook/zstd | [LICENSES/zstd.txt](LICENSES/zstd.txt) | Y |
| libjpeg-turbo | 3.1.3 | IJG AND BSD-3-Clause AND Zlib | https://github.com/libjpeg-turbo/libjpeg-turbo | [LICENSES/jpeg-turbo.txt](LICENSES/jpeg-turbo.txt) | Y |
| libpng | 1.6.54 | libpng-2.0 | https://github.com/glennrp/libpng | [LICENSES/libpng.txt](LICENSES/libpng.txt) | Y |
| nghttp2 | 1.68.0 | MIT | https://github.com/nghttp2/nghttp2 | [LICENSES/nghttp2.txt](LICENSES/nghttp2.txt) | Y |
| OpenSSL | 3.6.1 | Apache-2.0 | https://www.openssl.org | [LICENSES/openssl.txt](LICENSES/openssl.txt) | Y |
| curl | 8.18.0 | curl | https://curl.se | [LICENSES/curl.txt](LICENSES/curl.txt) | Y |

## Special notices

### GEOS (LGPL-2.1-only)

GEOS is licensed under the GNU Lesser General Public License v2.1 and is
**dynamically linked** into the `pyshed` wheel (via `delocate` on macOS and
`auditwheel` on Linux).
Redistributors must preserve the LGPL-2.1 license text (see
[`LICENSES/GEOS.txt`](LICENSES/GEOS.txt)). End users have the right to replace
the bundled GEOS shared library with a compatible version of their own choosing.

### OpenSSL (Apache-2.0)

OpenSSL 3.x is licensed under the Apache License 2.0 and is bundled because
`curl` (and therefore GDAL's HTTP/cloud support) links against it. The full
license text is at [`LICENSES/openssl.txt`](LICENSES/openssl.txt).

Note: whether OpenSSL is bundled in a given wheel depends on the build
environment. Confirm by inspecting the wheel with `unzip -l pyshed-*.whl` and
checking for `libssl`/`libcrypto` in the wheel's bundled shared-library
directory (`pyshed/.dylibs/` on macOS or `pyshed.libs/` on Linux).
