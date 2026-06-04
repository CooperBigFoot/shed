"""Pytest fixtures for pyshed behavioral tests.

Creates a synthetic 3-unit HFX dataset using pyarrow that mirrors the
schema produced by shed-core's DatasetBuilder test utility.
"""

import json
import struct

import pyarrow as pa
import pyarrow.parquet
import pytest


# ---------------------------------------------------------------------------
# WKB helpers
# ---------------------------------------------------------------------------


def make_wkb_polygon(minx: float, miny: float, maxx: float, maxy: float) -> bytes:
    """Create a minimal WKB Polygon (LE, type=3, 1 ring, 5 points)."""
    coords = [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ]
    buf = struct.pack("<bII", 1, 3, 1)  # LE, Polygon, 1 ring
    buf += struct.pack("<I", 5)  # 5 points
    for x, y in coords:
        buf += struct.pack("<dd", x, y)
    return buf


# ---------------------------------------------------------------------------
# Unit specifications
# ---------------------------------------------------------------------------

# Linear chain: unit 1 is headwater, unit 2 drains unit 1, unit 3 drains unit 2.
# Bboxes are non-overlapping and spaced along the x-axis, matching the layout
# used by the Rust DatasetBuilder (i * 0.5, 0.0, i * 0.5 + 0.4, 0.4).
_UNITS = [
    {"id": 1, "minx": 0.5, "miny": 0.0, "maxx": 0.9, "maxy": 0.4},
    {"id": 2, "minx": 1.0, "miny": 0.0, "maxx": 1.4, "maxy": 0.4},
    {"id": 3, "minx": 1.5, "miny": 0.0, "maxx": 1.9, "maxy": 0.4},
]


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def hfx_dataset(tmp_path):
    """Create a synthetic 3-unit HFX dataset and return its path as a string.

    The dataset is a minimal tree topology with:
    - unit 1: headwater (no upstream)
    - unit 2: drains unit 1
    - unit 3: drains unit 2 (outlet)

    Each unit has a rectangular catchment polygon spaced along the x-axis.
    """
    _write_manifest(tmp_path)
    _write_graph(tmp_path)
    _write_catchments(tmp_path)
    return str(tmp_path)


# ---------------------------------------------------------------------------
# Artifact writers
# ---------------------------------------------------------------------------


def _write_manifest(root):
    manifest = {
        "format_version": "0.2.1",
        "fabric_name": "testfabric",
        "fabric_version": "0.0.0",
        "crs": "EPSG:4326",
        "topology": "tree",
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "unit_count": len(_UNITS),
        "created_at": "2026-01-01T00:00:00Z",
        "adapter_version": "test-v1",
        "auxiliary": [],
    }
    (root / "manifest.json").write_text(json.dumps(manifest))


def _write_graph(root):
    # Linear chain: unit 1 has no upstream, unit 2 has [1], unit 3 has [2].
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field(
                "upstream_ids",
                pa.list_(pa.field("item", pa.int64(), nullable=True)),
                nullable=False,
            ),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
        ]
    )

    table = pa.table(
        {
            "id": pa.array([unit["id"] for unit in _UNITS], type=pa.int64()),
            "level": pa.array([0, 0, 0], type=pa.int16()),
            "upstream_ids": pa.array(
                [[], [1], [2]],
                type=pa.list_(pa.field("item", pa.int64(), nullable=True)),
            ),
            "bbox_minx": pa.array([unit["minx"] for unit in _UNITS], type=pa.float32()),
            "bbox_miny": pa.array([unit["miny"] for unit in _UNITS], type=pa.float32()),
            "bbox_maxx": pa.array([unit["maxx"] for unit in _UNITS], type=pa.float32()),
            "bbox_maxy": pa.array([unit["maxy"] for unit in _UNITS], type=pa.float32()),
        },
        schema=schema,
    )

    with open(root / "graph.parquet", "wb") as fh:
        with pa.parquet.ParquetWriter(fh, schema) as writer:
            writer.write_table(table)


def _write_catchments(root):
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("outlet_lon", pa.float64(), nullable=False),
            pa.field("outlet_lat", pa.float64(), nullable=False),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )

    ids = []
    levels = []
    parent_ids = []
    areas = []
    up_areas = []
    outlet_lons = []
    outlet_lats = []
    bbox_minx = []
    bbox_miny = []
    bbox_maxx = []
    bbox_maxy = []
    geometries = []

    for unit in _UNITS:
        ids.append(unit["id"])
        levels.append(0)
        parent_ids.append(None)
        areas.append(10.0)
        up_areas.append(float(unit["id"]) * 10.0)
        outlet_lons.append((unit["minx"] + unit["maxx"]) / 2.0)
        outlet_lats.append((unit["miny"] + unit["maxy"]) / 2.0)
        bbox_minx.append(unit["minx"])
        bbox_miny.append(unit["miny"])
        bbox_maxx.append(unit["maxx"])
        bbox_maxy.append(unit["maxy"])
        geometries.append(
            make_wkb_polygon(unit["minx"], unit["miny"], unit["maxx"], unit["maxy"])
        )

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "level": pa.array(levels, type=pa.int16()),
            "parent_id": pa.array(parent_ids, type=pa.int64()),
            "area_km2": pa.array(areas, type=pa.float32()),
            "up_area_km2": pa.array(up_areas, type=pa.float32()),
            "outlet_lon": pa.array(outlet_lons, type=pa.float64()),
            "outlet_lat": pa.array(outlet_lats, type=pa.float64()),
            "bbox_minx": pa.array(bbox_minx, type=pa.float32()),
            "bbox_miny": pa.array(bbox_miny, type=pa.float32()),
            "bbox_maxx": pa.array(bbox_maxx, type=pa.float32()),
            "bbox_maxy": pa.array(bbox_maxy, type=pa.float32()),
            "geometry": pa.array(geometries, type=pa.binary()),
        },
        schema=schema,
    )

    # Open the file explicitly to avoid pyarrow filesystem registration
    # conflicts with pyshed's Arrow/GDAL runtime (which registers the
    # 'file' scheme on import).
    with open(root / "catchments.parquet", "wb") as fh:
        with pa.parquet.ParquetWriter(fh, schema) as writer:
            writer.write_table(table)
