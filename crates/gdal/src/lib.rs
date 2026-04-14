//! GDAL-backed raster I/O and geometry operations for the shed engine.

pub mod convert;
pub mod error;
pub mod geometry_repair;
pub mod raster_reader;
pub mod wkb;

pub use error::{GdalRepairError, RasterReadError};
pub use geometry_repair::GdalGeometryRepair;
pub use raster_reader::GdalRasterSource;
pub use wkb::{WkbDecodeError, decode_wkb, decode_wkb_multi_polygon, decode_wkb_polygon};
