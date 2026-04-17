"""pyshed — Python bindings for the shed watershed delineation engine.

Resolves bundled GDAL_DATA and PROJ data at import time (wheel installs).
For source/editable installs the resolution is silent on miss; the user is
expected to have a system GDAL/PROJ already configured.
"""

from __future__ import annotations

import logging
import os
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from pathlib import Path

from pyshed import _pyshed
from pyshed._pyshed import (
    AssemblyError,
    DatasetError,
    DelineationResult,
    Engine,
    ResolutionError,
    ShedError,
)

__all__ = [
    "AssemblyError",
    "DatasetError",
    "DelineationResult",
    "Engine",
    "ResolutionError",
    "ShedError",
]

try:
    __version__ = _pkg_version("pyshed")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

_log = logging.getLogger(__name__)
_PKG_DIR = Path(__file__).resolve().parent


def _bundled(subdir: str, sentinel: str) -> str | None:
    candidate = _PKG_DIR / "_data" / subdir
    return str(candidate) if (candidate / sentinel).is_file() else None


def _inject_gdal_data() -> None:
    if "GDAL_DATA" in os.environ:
        return  # respect explicit user override
    path = _bundled("gdal", "gdalvrt.xsd")
    if path is None:
        _log.debug("no bundled gdal data in %s; relying on system GDAL", _PKG_DIR)
        return
    _pyshed._set_gdal_data(path)


def _inject_proj_data() -> None:
    for env_var in ("PROJ_DATA", "PROJ_LIB"):
        if env_var in os.environ:
            _pyshed._set_proj_data(os.environ[env_var])
            return
    path = _bundled("proj", "proj.db")
    if path is None:
        _log.debug("no bundled proj data in %s; relying on system PROJ", _PKG_DIR)
        return
    _pyshed._set_proj_data(path)


_inject_gdal_data()
_inject_proj_data()
