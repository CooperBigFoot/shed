"""Pytest fixtures for pyshed behavioral tests.

Creates a synthetic 3-atom HFX dataset using pyarrow that mirrors the
schema produced by shed-core's DatasetBuilder test utility.
"""

import json
import struct

import pyarrow as pa
import pyarrow.ipc
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
# Atom specifications
# ---------------------------------------------------------------------------

# Linear chain: atom 1 is headwater, atom 2 drains atom 1, atom 3 drains atom 2.
# Bboxes are non-overlapping and spaced along the x-axis, matching the layout
# used by the Rust DatasetBuilder (i * 0.5, 0.0, i * 0.5 + 0.4, 0.4).
_ATOMS = [
    {"id": 1, "minx": 0.5, "miny": 0.0, "maxx": 0.9, "maxy": 0.4},
    {"id": 2, "minx": 1.0, "miny": 0.0, "maxx": 1.4, "maxy": 0.4},
    {"id": 3, "minx": 1.5, "miny": 0.0, "maxx": 1.9, "maxy": 0.4},
]


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def hfx_dataset(tmp_path):
    """Create a synthetic 3-atom HFX dataset and return its path as a string.

    The dataset is a minimal tree topology with:
    - atom 1: headwater (no upstream)
    - atom 2: drains atom 1
    - atom 3: drains atom 2 (outlet)

    Each atom has a rectangular catchment polygon spaced along the x-axis.
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
        "format_version": "0.1",
        "fabric_name": "testfabric",
        "crs": "EPSG:4326",
        "topology": "tree",
        "terminal_sink_id": 0,
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "atom_count": 3,
        "created_at": "2026-01-01T00:00:00Z",
        "adapter_version": "test-v1",
    }
    (root / "manifest.json").write_text(json.dumps(manifest))


def _write_graph(root):
    # Linear chain: atom 1 has no upstream, atom 2 has [1], atom 3 has [2].
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(
                "upstream_ids",
                pa.list_(pa.field("item", pa.int64(), nullable=True)),
                nullable=False,
            ),
        ]
    )

    ids = pa.array([1, 2, 3], type=pa.int64())
    upstream_ids = pa.array(
        [[], [1], [2]],
        type=pa.list_(pa.field("item", pa.int64(), nullable=True)),
    )

    batch = pa.record_batch([ids, upstream_ids], schema=schema)

    # Open the file explicitly to avoid pyarrow filesystem registration
    # conflicts with pyshed's Arrow/GDAL runtime (which registers the
    # 'file' scheme on import).
    with open(root / "graph.arrow", "wb") as fh:
        with pa.ipc.new_file(fh, schema) as writer:
            writer.write_batch(batch)


def _write_catchments(root):
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )

    ids = []
    areas = []
    up_areas = []
    bbox_minx = []
    bbox_miny = []
    bbox_maxx = []
    bbox_maxy = []
    geometries = []

    for atom in _ATOMS:
        ids.append(atom["id"])
        areas.append(10.0)
        up_areas.append(None)
        bbox_minx.append(atom["minx"])
        bbox_miny.append(atom["miny"])
        bbox_maxx.append(atom["maxx"])
        bbox_maxy.append(atom["maxy"])
        geometries.append(
            make_wkb_polygon(atom["minx"], atom["miny"], atom["maxx"], atom["maxy"])
        )

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "area_km2": pa.array(areas, type=pa.float32()),
            "up_area_km2": pa.array(up_areas, type=pa.float32()),
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
