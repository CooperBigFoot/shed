//! Shared helpers and sub-readers for HFX dataset artifacts.

pub mod catchment_store;
pub mod graph;
pub mod manifest;
pub mod snap_store;

use arrow::datatypes::{DataType, Schema};
use hfx_core::BoundingBox;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::error::SessionError;

/// Verify that `schema` contains a column named `name` with the expected data
/// type. Returns the column index on success.
///
/// Accepts `LargeBinary` where `Binary` is expected, and `LargeList` where
/// `List` is expected, to tolerate writers that emit large variants.
pub(crate) fn require_column(
    schema: &Schema,
    name: &'static str,
    expected_type: &DataType,
    artifact: &'static str,
) -> Result<usize, SessionError> {
    let (idx, field) = schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name() == name)
        .ok_or_else(|| {
            SessionError::parquet_schema(artifact, format!("missing required column {name:?}"))
        })?;

    let actual = field.data_type();
    if actual != expected_type && !is_accepted_large_variant(actual, expected_type) {
        return Err(SessionError::parquet_schema(
            artifact,
            format!("column {name:?} has type {actual:?}, expected {expected_type:?}"),
        ));
    }

    Ok(idx)
}

/// Return `true` if `actual` is the large variant of `expected`.
///
/// Handles `LargeBinary` ↔ `Binary` and `LargeList` ↔ `List`.
fn is_accepted_large_variant(actual: &DataType, expected: &DataType) -> bool {
    matches!(
        (actual, expected),
        (DataType::LargeBinary, DataType::Binary) | (DataType::LargeList(_), DataType::List(_))
    )
}

/// Pre-resolved column indices for the four bbox columns in a Parquet schema.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BboxColIndices {
    /// Column index for `bbox_minx`.
    pub minx: usize,
    /// Column index for `bbox_miny`.
    pub miny: usize,
    /// Column index for `bbox_maxx`.
    pub maxx: usize,
    /// Column index for `bbox_maxy`.
    pub maxy: usize,
}

/// Extract the bounding box envelope for a row group from its column statistics.
///
/// Returns `None` if any bbox column lacks min/max statistics, or if the
/// resulting values form a degenerate bounding box.
pub(crate) fn extract_row_group_bbox(
    rg: &RowGroupMetaData,
    indices: &BboxColIndices,
) -> Option<BoundingBox> {
    let minx = float_min(rg.column(indices.minx).statistics()?)?;
    let miny = float_min(rg.column(indices.miny).statistics()?)?;
    let maxx = float_max(rg.column(indices.maxx).statistics()?)?;
    let maxy = float_max(rg.column(indices.maxy).statistics()?)?;

    BoundingBox::new(minx, miny, maxx, maxy).ok()
}

/// Extract the float minimum from a `Statistics` value.
///
/// Returns `None` for non-float statistics or when `min_opt` is absent.
fn float_min(stats: &Statistics) -> Option<f32> {
    match stats {
        Statistics::Float(typed) => typed.min_opt().copied(),
        _ => None,
    }
}

/// Extract the float maximum from a `Statistics` value.
///
/// Returns `None` for non-float statistics or when `max_opt` is absent.
fn float_max(stats: &Statistics) -> Option<f32> {
    match stats {
        Statistics::Float(typed) => typed.max_opt().copied(),
        _ => None,
    }
}
