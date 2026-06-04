//! Basin GeoParquet export helpers.

pub mod identity;
pub mod row_groups;
pub mod schema;
pub mod spatial;
pub mod unit_writer;
pub mod writer;

pub use identity::{BasinId, DelineationLabel, ExportMethod, ExportOrigin, FabricIdentity};
pub use row_groups::{RowGroupPlan, plan_row_groups};
pub use schema::{
    BasinExportSchemaProfile, basin_export_schema, geo_footer_json, unit_bundle_export_schema,
};
pub use spatial::{
    BasinBbox, BasinCentroid, BasinSpatialSortKey, HilbertIndex, UnitBundleSpatialSortKey,
    basin_bbox, basin_centroid, outward_f32_bbox,
};
pub use unit_writer::{UnitBundleExportInput, UnitBundleExportOptions, UnitBundleGeoParquetWriter};
pub use writer::{BasinExportInput, BasinGeoParquetWriter, ExportOptions};

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

    /// Fires when an export batch contains no basin rows.
    #[error("basin GeoParquet export requires at least one row")]
    EmptyInput,

    /// Fires when a unit-bundle export contains no drainage-unit rows.
    #[error("unit-bundle GeoParquet export requires at least one row")]
    EmptyUnitBundle,

    /// Fires when two rows use the same `(basin_id, delineation)` identity.
    #[error("duplicate export row for basin_id {basin_id} and delineation {delineation}")]
    DuplicateRow {
        /// Duplicate basin identifier.
        basin_id: String,
        /// Duplicate delineation label.
        delineation: String,
    },

    /// Fires when two unit-bundle rows use the same `(unit_id, delineation)` identity.
    #[error("duplicate unit-bundle export row for unit_id {unit_id} and delineation {delineation}")]
    DuplicateUnitBundleRow {
        /// Duplicate drainage-unit identifier.
        unit_id: i64,
        /// Duplicate delineation label.
        delineation: String,
    },

    /// Fires when a delineation geometry cannot be encoded as WKB.
    #[error("cannot encode geometry for basin_id {basin_id}: {source}")]
    GeometryEncodingFailure {
        /// Basin identifier being materialized.
        basin_id: String,
        /// Lower-level WKB encoding failure.
        source: crate::algo::WkbEncodeError,
    },

    /// Fires when a pre-merge drainage-unit geometry cannot be encoded as WKB.
    #[error("cannot encode geometry for unit_id {unit_id}: {source}")]
    UnitGeometryEncodingFailure {
        /// Drainage-unit identifier being materialized.
        unit_id: i64,
        /// Lower-level WKB encoding failure.
        source: crate::algo::WkbEncodeError,
    },

    /// Fires when Arrow array or record-batch construction fails before writing.
    #[error("cannot build Arrow export batch: {source}")]
    ArrowWriteFailure {
        /// Lower-level Arrow error.
        source: arrow::error::ArrowError,
    },

    /// Fires when Parquet writer construction, row writing, flushing, or closing fails.
    #[error("cannot write basin GeoParquet file: {source}")]
    ParquetWriteFailure {
        /// Lower-level Parquet error.
        source: parquet::errors::ParquetError,
    },

    /// Fires when the writer cannot prove `geo` metadata reached the Parquet footer.
    #[error("GeoParquet footer metadata failure: {reason}")]
    FooterMetadataFailure {
        /// Human-readable footer metadata failure reason.
        reason: &'static str,
    },
}
