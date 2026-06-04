//! Basin GeoParquet export helpers.

pub mod identity;
pub mod spatial;

pub use identity::{BasinId, DelineationLabel, ExportMethod, ExportOrigin, FabricIdentity};
pub use spatial::{
    BasinBbox, BasinCentroid, BasinSpatialSortKey, HilbertIndex, basin_bbox, basin_centroid,
    outward_f32_bbox,
};

/// Errors raised while preparing basin GeoParquet exports.
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    /// Fires when a caller-supplied or defaulted `basin_id` violates the documented allowlist.
    #[error("invalid basin_id {value:?}: {reason}")]
    InvalidBasinId {
        /// Rejected basin identifier value.
        value: String,
        /// Human-readable rejection reason.
        reason: &'static str,
    },

    /// Fires when default delineation label construction is requested without fabric data version.
    #[error(
        "fabric_version is required to build default delineation label for fabric {fabric_name:?}"
    )]
    MissingFabricVersion {
        /// Source fabric name from the HFX manifest.
        fabric_name: String,
    },

    /// Fires when terminal-unit defaulting sees a negative source unit identifier.
    #[error("cannot default basin_id from negative terminal unit id {terminal_unit_id}")]
    NegativeDefaultBasinId {
        /// Raw terminal unit identifier from the result boundary.
        terminal_unit_id: i64,
    },

    /// Fires when two defaulted rows produce the same basin ID from different origins.
    #[error(
        "default basin_id {basin_id} collides between {first_origin} and {second_origin}; supply explicit basin_id values"
    )]
    DefaultBasinIdCollision {
        /// Colliding default basin identifier.
        basin_id: String,
        /// First origin that produced the default ID.
        first_origin: String,
        /// Second origin that produced the default ID.
        second_origin: String,
    },

    /// Fires when bbox computation cannot produce finite bounds for a geometry.
    #[error("cannot compute basin bbox: {reason}")]
    BboxFailure {
        /// Human-readable geometry failure reason.
        reason: &'static str,
    },

    /// Fires when centroid computation cannot produce a finite point for a geometry.
    #[error("cannot compute basin centroid: {reason}")]
    CentroidFailure {
        /// Human-readable geometry failure reason.
        reason: &'static str,
    },

    /// Fires when a valid balanced row-group plan cannot be produced for the requested rows.
    #[error("cannot plan row groups for {row_count} rows: {reason}")]
    RowGroupPlanningFailure {
        /// Number of rows requested.
        row_count: usize,
        /// Human-readable planning failure reason.
        reason: &'static str,
    },
}
