//! Error types for the `shed-gdal` crate.

/// Errors from loading raster tiles via GDAL.
#[derive(Debug, thiserror::Error)]
pub enum RasterReadError {
    /// Raster file not found on disk.
    #[error("raster file not found: {path}")]
    FileNotFound {
        /// Absolute path that was checked.
        path: String,
    },

    /// GDAL failed to open the dataset.
    #[error("GDAL open failed for {path}: {reason}")]
    GdalOpen {
        /// Path that was passed to GDAL.
        path: String,
        /// Stringified GDAL error.
        reason: String,
    },

    /// GDAL failed to read raster data from the window.
    #[error("GDAL read failed for {path}: {reason}")]
    GdalRead {
        /// Path of the file being read.
        path: String,
        /// Stringified GDAL error.
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

/// Errors from GDAL-backed geometry repair.
#[derive(Debug, thiserror::Error)]
pub enum GdalRepairError {
    /// GDAL geometry operation failed.
    #[error("GDAL geometry operation failed: {reason}")]
    Gdal {
        /// Stringified GDAL error.
        reason: String,
    },

    /// Expected polygon output from GDAL, got something else.
    #[error("expected polygon output, got {geometry_type}")]
    UnexpectedGeometryType {
        /// The WKT geometry type name returned by GDAL.
        geometry_type: String,
    },

    /// Geometry remains invalid after all repair attempts.
    #[error("geometry remains invalid after repair")]
    StillInvalid,
}

impl From<gdal::errors::GdalError> for GdalRepairError {
    fn from(e: gdal::errors::GdalError) -> Self {
        Self::Gdal { reason: e.to_string() }
    }
}

/// Errors from WKB decoding.
#[derive(Debug, thiserror::Error)]
pub enum WkbDecodeError {
    /// WKB decoding by the geozero backend failed.
    #[error("WKB decoding failed: {reason}")]
    DecodeFailed {
        /// Reason reported by the decoder.
        reason: String,
    },

    /// The decoded geometry type did not match what was expected.
    #[error("expected {expected}, got {actual}")]
    UnexpectedType {
        /// The geometry type that was expected.
        expected: &'static str,
        /// The geometry type that was actually decoded.
        actual: String,
    },
}
