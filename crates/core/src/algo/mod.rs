//! Pure-Rust watershed delineation algorithms.

pub mod area;
pub mod clean_epsilon;
pub mod coord;
pub mod distance;
pub mod flow_dir;
pub mod geo_transform;
pub mod snap_threshold;
pub mod tile_state;

// Re-exports for convenience.
pub use area::AreaKm2;
pub use clean_epsilon::{CleanEpsilon, DEFAULT_CLEANING_EPSILON};
pub use coord::{GeoCoord, GridCoord, GridDims};
pub use distance::{DistanceMetres, geodesic_distance};
pub use flow_dir::{FlowDir, InvalidFlowDir};
pub use geo_transform::GeoTransform;
pub use snap_threshold::SnapThreshold;
pub use tile_state::{Masked, Raw};
