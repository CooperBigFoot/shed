"""Behavioral tests for pyshed bindings.

Exercises delineation, coordinate validation, batch exception mapping,
GeoJSON output, and repr — all against a synthetic 3-atom HFX dataset
created by the ``hfx_dataset`` fixture in conftest.py.

Atom layout (non-overlapping bboxes along x-axis):
    atom 1: x=[0.5, 0.9], y=[0.0, 0.4]  — headwater
    atom 2: x=[1.0, 1.4], y=[0.0, 0.4]  — drains atom 1
    atom 3: x=[1.5, 1.9], y=[0.0, 0.4]  — drains atoms 1+2 (outlet)

Test coordinate inside atom 3: lon=1.70, lat=0.20
Test coordinate inside atom 1: lon=0.70, lat=0.20
"""

import json

import pytest

import pyshed


class TestSingleDelineation:
    """Tests for engine.delineate()."""

    def test_delineate_returns_result(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        # Coordinate inside atom 3's bbox
        result = engine.delineate(lat=0.20, lon=1.70)
        assert result.area_km2 > 0
        assert isinstance(result.geometry_wkb, bytes)
        assert len(result.geometry_wkb) > 0
        assert len(result.upstream_atom_ids) >= 1
        assert result.terminal_atom_id > 0

    def test_delineate_input_outlet(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        result = engine.delineate(lat=0.20, lon=1.70)
        lon, lat = result.input_outlet
        assert abs(lon - 1.70) < 1e-6
        assert abs(lat - 0.20) < 1e-6

    def test_delineate_resolved_outlet(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        result = engine.delineate(lat=0.20, lon=1.70)
        lon, lat = result.resolved_outlet
        assert isinstance(lon, float)
        assert isinstance(lat, float)

    def test_delineate_outside_catchments_raises_resolution_error(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        # Geographically valid but outside all catchments
        with pytest.raises(pyshed.ResolutionError):
            engine.delineate(lat=50.0, lon=50.0)


class TestCoordinateValidation:
    """Tests for coordinate validation (issue 6 fix)."""

    def test_lat_too_high(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(ValueError, match="latitude.*outside"):
            engine.delineate(lat=91.0, lon=0.0)

    def test_lat_too_low(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(ValueError, match="latitude.*outside"):
            engine.delineate(lat=-91.0, lon=0.0)

    def test_lon_too_high(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(ValueError, match="longitude.*outside"):
            engine.delineate(lat=0.0, lon=181.0)

    def test_lon_too_low(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(ValueError, match="longitude.*outside"):
            engine.delineate(lat=0.0, lon=-181.0)

    def test_batch_validates_coords(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(ValueError, match="latitude.*outside"):
            engine.delineate_batch([{"lat": 91.0, "lon": 0.0}])


class TestBatchDelineation:
    """Tests for engine.delineate_batch()."""

    def test_batch_all_succeed(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        results = engine.delineate_batch([
            {"lat": 0.20, "lon": 1.70},
            {"lat": 0.20, "lon": 0.70},
        ])
        assert len(results) == 2
        for r in results:
            assert r.area_km2 > 0

    def test_batch_raises_typed_exception(self, hfx_dataset):
        """Batch errors must use typed exceptions, not generic RuntimeError (issue 2 fix)."""
        engine = pyshed.Engine(hfx_dataset)
        # Mix a valid outlet with one outside all catchments
        with pytest.raises(pyshed.ShedError) as exc_info:
            engine.delineate_batch([
                {"lat": 0.20, "lon": 1.70},
                {"lat": 50.0, "lon": 50.0},
            ])
        # Must be ResolutionError specifically, not a generic RuntimeError
        assert isinstance(exc_info.value, pyshed.ResolutionError)


class TestGeoJSON:
    """Tests for to_geojson() output."""

    def test_geojson_is_valid_json(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        result = engine.delineate(lat=0.20, lon=1.70)
        geojson_str = result.to_geojson()
        data = json.loads(geojson_str)
        assert data["type"] == "Feature"
        assert data["geometry"]["type"] == "MultiPolygon"

    def test_geojson_has_expected_properties(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        result = engine.delineate(lat=0.20, lon=1.70)
        data = json.loads(result.to_geojson())
        props = data["properties"]
        assert "area_km2" in props
        assert "terminal_atom_id" in props
        assert "resolution_method" in props
        assert "refinement" in props
        assert "upstream_atom_count" in props
        assert props["area_km2"] > 0


class TestRepr:
    """Tests for __repr__."""

    def test_repr_contains_key_info(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        result = engine.delineate(lat=0.20, lon=1.70)
        r = repr(result)
        assert "DelineationResult" in r
        assert "area_km2" in r
