//! Trait boundaries for external capabilities (raster I/O, geometry repair).
//!
//! These traits are defined in `shed-core` and implemented by `shed-gdal`.

use geo::{MultiPolygon, Rect};

use crate::algo::accumulation_tile::AccumulationTile;
use crate::algo::clean_epsilon::CleanEpsilon;
use crate::algo::flow_direction_tile::FlowDirectionTile;
use crate::algo::tile_state::Raw;

/// Errors from raster source operations.
#[derive(Debug, thiserror::Error)]
pub enum RasterSourceError {
    /// Raster file not found on disk.
    #[error("raster file not found: {path}")]
    FileNotFound {
        /// Absolute path to the missing raster file.
        path: String,
    },

    /// Backend failed to open the raster dataset.
    #[error("failed to open raster at {path}: {reason}")]
    OpenFailed {
        /// Path of the raster that could not be opened.
        path: String,
        /// Reason reported by the backend.
        reason: String,
    },

    /// Backend failed to read raster data from the window.
    #[error("failed to read raster window from {path}: {reason}")]
    ReadFailed {
        /// Path of the raster that could not be read.
        path: String,
        /// Reason reported by the backend.
        reason: String,
    },

    /// Requested window maps to a zero-size pixel region.
    #[error("empty raster window for {path}")]
    EmptyWindow {
        /// Path of the raster for which the window collapsed.
        path: String,
    },

    /// Tile construction failed after a successful read.
    #[error("tile construction failed: {reason}")]
    TileConstruction {
        /// Reason why tile construction failed.
        reason: String,
    },
}

/// Errors from geometry repair operations.
#[derive(Debug, thiserror::Error)]
pub enum GeometryRepairError {
    /// Backend geometry operation failed.
    #[error("geometry repair failed: {reason}")]
    BackendError {
        /// Reason reported by the backend.
        reason: String,
    },

    /// Expected polygon output, got something else.
    #[error("expected polygon output, got {geometry_type}")]
    UnexpectedGeometryType {
        /// The WKT geometry type name returned by the backend.
        geometry_type: String,
    },

    /// Geometry remains invalid after all repair attempts.
    #[error("geometry remains invalid after repair")]
    StillInvalid,
}

/// Load windowed raster tiles from GeoTIFF files or GDAL virtual paths.
///
/// The canonical implementation uses GDAL and lives in `shed-gdal`.
pub trait RasterSource {
    /// Load flow direction values within `bbox` from the raster URI at `uri`.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`RasterSourceError::FileNotFound`] | Local file does not exist on disk |
    /// | [`RasterSourceError::OpenFailed`] | Backend cannot open the dataset |
    /// | [`RasterSourceError::ReadFailed`] | Backend cannot read the window |
    /// | [`RasterSourceError::EmptyWindow`] | Bbox maps to zero pixels |
    /// | [`RasterSourceError::TileConstruction`] | Tile construction fails after read |
    fn load_flow_direction(
        &self,
        uri: &str,
        bbox: &Rect<f64>,
    ) -> Result<FlowDirectionTile<Raw>, RasterSourceError>;

    /// Load flow accumulation values within `bbox` from the raster URI at `uri`.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`RasterSourceError::FileNotFound`] | Local file does not exist on disk |
    /// | [`RasterSourceError::OpenFailed`] | Backend cannot open the dataset |
    /// | [`RasterSourceError::ReadFailed`] | Backend cannot read the window |
    /// | [`RasterSourceError::EmptyWindow`] | Bbox maps to zero pixels |
    /// | [`RasterSourceError::TileConstruction`] | Tile construction fails after read |
    fn load_accumulation(
        &self,
        uri: &str,
        bbox: &Rect<f64>,
    ) -> Result<AccumulationTile<Raw>, RasterSourceError>;
}

/// Repair invalid geometries using external geometry libraries.
///
/// The canonical implementation uses GDAL/GEOS and lives in `shed-gdal`.
pub trait GeometryRepair {
    /// Validate and repair a multi-polygon, returning a geometrically valid result.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`GeometryRepairError::BackendError`] | Backend geometry operation fails |
    /// | [`GeometryRepairError::UnexpectedGeometryType`] | Backend returns a non-polygon type |
    /// | [`GeometryRepairError::StillInvalid`] | Geometry remains invalid after repair |
    fn repair(
        &self,
        geometry: MultiPolygon<f64>,
        epsilon: CleanEpsilon,
    ) -> Result<MultiPolygon<f64>, GeometryRepairError>;
}
