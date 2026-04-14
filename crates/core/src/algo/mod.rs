//! Pure-Rust watershed delineation algorithms.

pub mod accumulation_tile;
pub mod area;
pub mod catchment_mask;
pub mod clean_epsilon;
pub mod coord;
pub mod distance;
pub mod flow_dir;
pub mod flow_direction_tile;
pub mod geo_transform;
pub mod raster_tile;
pub mod snap_threshold;
pub mod tile_state;

// Re-exports for convenience.
pub use accumulation_tile::AccumulationTile;
pub use area::AreaKm2;
pub use catchment_mask::CatchmentMask;
pub use clean_epsilon::{CleanEpsilon, DEFAULT_CLEANING_EPSILON};
pub use coord::{GeoCoord, GridCoord, GridDims};
pub use distance::{DistanceMetres, geodesic_distance};
pub use flow_dir::{FlowDir, InvalidFlowDir};
pub use flow_direction_tile::FlowDirectionTile;
pub use geo_transform::GeoTransform;
pub use raster_tile::{RasterTile, RasterTileError};
pub use snap_threshold::SnapThreshold;
pub use tile_state::{Masked, Raw};
