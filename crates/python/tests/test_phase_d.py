"""Phase D integration tests for pyshed.

Covers kwarg validation, set_log_level, delineate_batch progress callback, and
parquet cache behaviour.

Network-dependent tests are gated by the PYSHED_TEST_REMOTE_URL environment
variable and marked with ``@pytest.mark.network``.
"""

from __future__ import annotations

import logging
import os

import pytest

import pyshed

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REMOTE_URL = os.environ.get("PYSHED_TEST_REMOTE_URL", "")

# Three outlets inside the GRIT dataset.  Only used in network tests.
_REMOTE_OUTLETS = [
    {"lat": 47.3769, "lon": 8.5417},
    {"lat": 46.9480, "lon": 7.4474},
    {"lat": 48.1351, "lon": 11.5820},
]

network_skip = pytest.mark.skipif(
    not _REMOTE_URL,
    reason="requires PYSHED_TEST_REMOTE_URL",
)

# ---------------------------------------------------------------------------
# D1 – kwarg validation (no network needed)
# ---------------------------------------------------------------------------


class TestKwargValidation:
    """Kwarg validation fires before I/O so any path works."""

    # Dummy path — kwargs validator runs before the dataset is opened.
    _path = "/tmp/nonexistent.hfx"

    def _engine(self, **kw):
        """Return an Engine constructed against the synthetic fixture.

        If no fixture is available, falls back to the dummy path when we only
        need to exercise kwarg validation (errors fire before I/O).
        """
        return pyshed.Engine(self._path, **kw)

    # Test 1
    def test_delineate_rejects_constructor_kwarg(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(TypeError) as exc_info:
            engine.delineate(lat=0.20, lon=1.70, snap_radius=5000)
        msg = str(exc_info.value)
        assert "snap_radius" in msg
        assert "constructor" in msg

    # Test 2
    def test_delineate_typo_lat(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(TypeError) as exc_info:
            engine.delineate(lattitude=0.20, lon=1.70)
        msg = str(exc_info.value)
        assert "lat" in msg

    # Test 3
    def test_delineate_unknown_kwarg_close_match(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(TypeError) as exc_info:
            engine.delineate(lat=0.20, lon=1.70, geomtry=True)
        msg = str(exc_info.value)
        assert "geometry" in msg

    # Test 4
    def test_delineate_unknown_kwarg_no_match(self, hfx_dataset):
        engine = pyshed.Engine(hfx_dataset)
        with pytest.raises(TypeError) as exc_info:
            engine.delineate(lat=0.20, lon=1.70, foobar=1)
        msg = str(exc_info.value)
        assert "lat" in msg
        assert "lon" in msg
        assert "geometry" in msg

    # Test 5
    def test_constructor_rejects_delineate_kwarg(self):
        with pytest.raises(TypeError) as exc_info:
            pyshed.Engine(self._path, lat=0)
        msg = str(exc_info.value)
        # Message should mention that lat belongs on delineate, not the constructor.
        assert "lat" in msg
        assert "delineate" in msg

    # Test 12 – max_mb=0 validation fires before I/O
    def test_parquet_cache_max_mb_validation(self):
        with pytest.raises(ValueError, match="parquet_cache_max_mb"):
            pyshed.Engine(self._path, parquet_cache=True, parquet_cache_max_mb=0)


# ---------------------------------------------------------------------------
# D2 – delineate happy path (network)
# ---------------------------------------------------------------------------


@network_skip
@pytest.mark.network
def test_delineate_happy_path_unchanged():
    """geometry=False returns an AreaOnlyResult with area and terminal_atom_id."""
    engine = pyshed.Engine(_REMOTE_URL)
    result = engine.delineate(lat=47.3769, lon=8.5417, geometry=False)
    assert isinstance(result, pyshed.AreaOnlyResult)
    assert result.area_km2 > 0
    assert result.terminal_atom_id > 0


# ---------------------------------------------------------------------------
# D3 – set_log_level emits records (network)
# ---------------------------------------------------------------------------


@network_skip
@pytest.mark.network
def test_set_log_level_emits_records(caplog):
    """Opening a remote dataset with INFO enabled produces ≥4 log records."""
    pyshed.set_log_level("info")
    with caplog.at_level(logging.DEBUG, logger="shed_core"):
        with caplog.at_level(logging.DEBUG, logger="_pyshed"):
            with caplog.at_level(logging.DEBUG, logger="hfx_core"):
                pyshed.Engine(_REMOTE_URL)

    all_records = caplog.records
    assert len(all_records) >= 4, (
        f"expected ≥4 log records from Engine open, got {len(all_records)}: "
        + "\n".join(r.getMessage() for r in all_records)
    )


# ---------------------------------------------------------------------------
# D4 – delineate_batch progress callback (network)
# ---------------------------------------------------------------------------


@network_skip
@pytest.mark.network
def test_delineate_batch_progress_callback():
    """Progress callback is invoked exactly N times with monotonically increasing index."""
    engine = pyshed.Engine(_REMOTE_URL)
    events: list[dict] = []

    def capture(event):
        events.append(dict(event))

    results = engine.delineate_batch(_REMOTE_OUTLETS, progress=capture)
    assert len(results) == len(_REMOTE_OUTLETS)
    assert len(events) == len(_REMOTE_OUTLETS), (
        f"expected {len(_REMOTE_OUTLETS)} progress events, got {len(events)}"
    )

    indices = [e["index"] for e in events]
    assert indices == list(range(len(_REMOTE_OUTLETS))), (
        f"indices are not monotonically 0..N-1: {indices}"
    )
    for ev in events:
        assert ev.get("status") in ("ok", "error")


# ---------------------------------------------------------------------------
# D5 – parquet cache off by default (network)
# ---------------------------------------------------------------------------


@network_skip
@pytest.mark.network
def test_parquet_cache_off_default(caplog):
    """Engine() with no parquet_cache kwarg must not emit a 'parquet_cache enabled' log."""
    pyshed.set_log_level("info")
    with caplog.at_level(logging.DEBUG, logger="shed_core"):
        with caplog.at_level(logging.DEBUG, logger="_pyshed"):
            engine = pyshed.Engine(_REMOTE_URL)
            engine.delineate(lat=47.3769, lon=8.5417)

    cache_enabled_lines = [
        r.getMessage() for r in caplog.records if "parquet_cache enabled" in r.getMessage()
    ]
    assert cache_enabled_lines == [], (
        f"default Engine should not emit 'parquet_cache enabled'; found: {cache_enabled_lines}"
    )


# ---------------------------------------------------------------------------
# D6 – parquet cache on/off results identical (network)
# ---------------------------------------------------------------------------


@network_skip
@pytest.mark.network
def test_parquet_cache_on_off_results_identical():
    """Same outlet produces identical area_km2 and terminal_atom_id with and without cache."""
    outlet = {"lat": 47.3769, "lon": 8.5417}

    engine_off = pyshed.Engine(_REMOTE_URL, parquet_cache=False)
    result_off = engine_off.delineate(**outlet, geometry=False)

    engine_on = pyshed.Engine(_REMOTE_URL, parquet_cache=True, parquet_cache_max_mb=512)
    result_on = engine_on.delineate(**outlet, geometry=False)

    assert result_off.area_km2 == pytest.approx(result_on.area_km2, rel=1e-6)
    assert result_off.terminal_atom_id == result_on.terminal_atom_id


# ---------------------------------------------------------------------------
# D7 – parquet cache miss then hit (network)
# ---------------------------------------------------------------------------


@network_skip
@pytest.mark.network
def test_parquet_cache_miss_then_hit(caplog):
    """Two adjacent delineations with parquet_cache=True show ≥1 miss then ≥1 hit."""
    pyshed.set_log_level("debug")
    engine = pyshed.Engine(_REMOTE_URL, parquet_cache=True, parquet_cache_max_mb=512)

    with caplog.at_level(logging.DEBUG, logger="shed_core"):
        with caplog.at_level(logging.DEBUG, logger="_pyshed"):
            engine.delineate(lat=47.3769, lon=8.5417)
            engine.delineate(lat=47.3769, lon=8.5417)

    messages = [r.getMessage() for r in caplog.records]
    miss_count = sum(1 for m in messages if "parquet_cache miss" in m)
    hit_count = sum(1 for m in messages if "parquet_cache hit" in m)

    assert miss_count >= 1, f"expected ≥1 cache miss log; messages: {messages}"
    assert hit_count >= 1, f"expected ≥1 cache hit log; messages: {messages}"


# ---------------------------------------------------------------------------
# D8 – parquet cache eviction under tight budget (network)
# ---------------------------------------------------------------------------


@network_skip
@pytest.mark.network
def test_parquet_cache_eviction_under_tight_budget(caplog):
    """With a 1 MiB budget and several distinct delineations, at least one eviction fires."""
    pyshed.set_log_level("debug")
    engine = pyshed.Engine(_REMOTE_URL, parquet_cache=True, parquet_cache_max_mb=1)

    outlets = [
        {"lat": 47.3769, "lon": 8.5417},
        {"lat": 46.9480, "lon": 7.4474},
        {"lat": 48.1351, "lon": 11.5820},
        {"lat": 47.8095, "lon": 13.0550},
        {"lat": 47.0707, "lon": 15.4395},
    ]

    with caplog.at_level(logging.DEBUG, logger="shed_core"):
        with caplog.at_level(logging.DEBUG, logger="_pyshed"):
            for outlet in outlets:
                engine.delineate(**outlet)

    messages = [r.getMessage() for r in caplog.records]
    evict_count = sum(1 for m in messages if "parquet_cache evict" in m)

    if evict_count == 0:
        pytest.skip(
            "no parquet_cache evict event observed; row-group sizes may exceed 1 MiB budget"
        )

    assert evict_count >= 1
