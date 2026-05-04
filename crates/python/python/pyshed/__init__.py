"""pyshed — Python bindings for the shed watershed delineation engine.

Resolves bundled GDAL_DATA and PROJ data at import time (wheel installs).
For source/editable installs the resolution is silent on miss; the user is
expected to have a system GDAL/PROJ already configured.
"""

from __future__ import annotations

import logging
import os
import warnings
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent
_log = logging.getLogger(__name__)


def _bundled(subdir: str, sentinel: str) -> str | None:
    candidate = _PKG_DIR / "_data" / subdir
    return str(candidate) if (candidate / sentinel).is_file() else None


def _preseed_bundled_data() -> None:
    if "GDAL_DATA" not in os.environ:
        path = _bundled("gdal", "gdalvrt.xsd")
        if path is not None:
            os.environ["GDAL_DATA"] = path

    if "PROJ_DATA" not in os.environ and "PROJ_LIB" not in os.environ:
        path = _bundled("proj", "proj.db")
        if path is not None:
            os.environ["PROJ_DATA"] = path


_preseed_bundled_data()

from pyshed import _pyshed
from pyshed._pyshed import (
    AreaOnlyResult,
    AssemblyError,
    DatasetError,
    DelineationResult,
    Engine,
    ResolutionError,
    ShedError,
    _set_log_level,
)


_LOGGER_NAMES = ("pyshed", "_pyshed", "shed_core", "hfx_core")
_LOG_LEVELS = {
    "trace": ("trace", logging.DEBUG),
    "debug": ("debug", logging.DEBUG),
    "info": ("info", logging.INFO),
    "warn": ("warn", logging.WARNING),
    "warning": ("warn", logging.WARNING),
    "error": ("error", logging.ERROR),
    "critical": ("error", logging.ERROR),
}
_LOG_FORMATTER = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")


def _normalize_log_level(level: str) -> tuple[str, int]:
    try:
        return _LOG_LEVELS[level.lower()]
    except KeyError as exc:
        raise ValueError(
            "unknown log level; valid values are: trace, debug, info, warn, "
            "warning, error, critical"
        ) from exc


def set_log_level(level: str) -> None:
    """Set the pyshed log level for both the Rust bridge and Python `logging`.

    Updates the dynamic max-level used by the pyo3-log bridge and configures
    the relevant Python loggers so records actually emit. If any pyshed logger
    has no handler, a default ``StreamHandler`` is attached to that logger.

    Records originating from Rust route through pyo3-log under loggers named
    after their Rust crate (``_pyshed.*``, ``shed_core.*``, ``hfx_core.*``).
    We therefore set the level on each of those roots in addition to the
    Python ``pyshed`` facade.

    Valid levels (case-insensitive): ``"trace"``, ``"debug"``, ``"info"``,
    ``"warn"``/``"warning"``, ``"error"``/``"critical"``.
    """
    rust_level, py_level = _normalize_log_level(level)
    _set_log_level(rust_level)
    for logger_name in _LOGGER_NAMES:
        logger = logging.getLogger(logger_name)
        logger.setLevel(py_level)
        logger.propagate = False
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(_LOG_FORMATTER)
            logger.addHandler(handler)


__all__ = [
    "AreaOnlyResult",
    "AssemblyError",
    "DatasetError",
    "DelineationResult",
    "Engine",
    "ResolutionError",
    "ShedError",
    "set_log_level",
]

try:
    __version__ = _pkg_version("pyshed")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"


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

if "PYSHED_LOG" in os.environ:
    try:
        set_log_level(os.environ["PYSHED_LOG"])
    except ValueError:
        warnings.warn(
            "invalid PYSHED_LOG value; valid values are: trace, debug, info, "
            "warn, warning, error, critical",
            UserWarning,
            stacklevel=2,
        )
