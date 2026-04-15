//! Pure-Rust watershed delineation algorithms.

// ── Foundation types ─────────────────────────────────────────────────────────
pub mod area;
pub mod clean_epsilon;
pub mod coord;
pub mod distance;
pub mod flow_dir;
pub mod geo_transform;
pub mod snap_threshold;
pub mod tile_state;

// ── Raster infrastructure ────────────────────────────────────────────────────
pub mod accumulation_tile;
pub mod catchment_mask;
pub mod flow_direction_tile;
pub mod raster_tile;

// ── Raster algorithms ────────────────────────────────────────────────────────
pub mod polygonize;
pub mod rasterize;
pub mod snap;
pub mod trace;
pub mod refine;

// ── Graph traversal ──────────────────────────────────────────────────────────
pub mod upstream;

// ── Geometry processing ──────────────────────────────────────────────────────
pub mod clean_topology;
pub mod dissolve;
pub mod hole_fill;
pub mod largest_polygon;
pub mod watershed_area;

// ── Pipeline + traits ────────────────────────────────────────────────────────
pub mod self_intersection;
pub mod traits;
pub mod watershed_geometry;

// ── WKB decoding ─────────────────────────────────────────────────────────────
pub mod wkb;

// ── Re-exports: foundation types ─────────────────────────────────────────────
pub use area::AreaKm2;
pub use clean_epsilon::{CleanEpsilon, DEFAULT_CLEANING_EPSILON};
pub use coord::{GeoCoord, GridCoord, GridDims};
pub use distance::{DistanceMetres, geodesic_distance};
pub use flow_dir::{FlowDir, InvalidFlowDir};
pub use geo_transform::GeoTransform;
pub use snap_threshold::SnapThreshold;
pub use tile_state::{Masked, Raw};

// ── Re-exports: raster infrastructure ────────────────────────────────────────
pub use accumulation_tile::AccumulationTile;
pub use catchment_mask::CatchmentMask;
pub use flow_direction_tile::FlowDirectionTile;
pub use raster_tile::{RasterTile, RasterTileError};

// ── Re-exports: raster algorithms ────────────────────────────────────────────
pub use polygonize::polygonize;
pub use rasterize::{rasterize_multi_polygon, rasterize_polygon};
pub use snap::{SnapError, SnappedPoint, snap_pour_point};
pub use trace::trace_upstream;
pub use refine::{RefinementError, RefinementResult, refine_terminal, refine_terminal_from_source};

// ── Re-exports: graph traversal ──────────────────────────────────────────────
pub use upstream::{TraversalError, UpstreamAtoms, collect_upstream};

// ── Re-exports: geometry processing ──────────────────────────────────────────
pub use clean_topology::clean_topology;
pub use dissolve::{DissolveError, dissolve};
pub use hole_fill::{HoleFillMode, DEFAULT_FILL_THRESHOLD_PX, fill_holes};
pub use largest_polygon::largest_polygon;
pub use watershed_area::{WatershedAreaError, geodesic_area, geodesic_area_multi};

// ── Re-exports: pipeline + traits ────────────────────────────────────────────
pub use self_intersection::has_self_intersections;
pub use traits::{GeometryRepair, GeometryRepairError, RasterSource, RasterSourceError};
pub use watershed_geometry::{Dissolved, HolesFilled, TopologyCleaned, WatershedGeometry};

// ── Re-exports: WKB decoding ──────────────────────────────────────────────────
pub use wkb::{WkbDecodeError, decode_wkb, decode_wkb_multi_polygon, decode_wkb_polygon};
