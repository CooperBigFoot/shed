"""Tests for runtime GDAL/PROJ data-path injection."""

from pathlib import Path

import pytest

import pyshed
from pyshed import _pyshed


def test_gdal_data_bundled_when_present() -> None:
    """If the wheel includes bundled GDAL data, gdalvrt.xsd must be there."""
    pkg = Path(pyshed.__file__).parent
    gdal_data = pkg / "_data" / "gdal"
    if not gdal_data.is_dir():
        pytest.skip("source install (no bundled gdal_data); test applies to wheels only")
    assert (gdal_data / "gdalvrt.xsd").is_file()


def test_proj_round_trip_succeeds() -> None:
    """PROJ must be able to resolve its data and perform EPSG:4326 -> EPSG:3857."""
    _pyshed._self_test_proj()  # raises PyRuntimeError if proj.db is missing/unreachable
