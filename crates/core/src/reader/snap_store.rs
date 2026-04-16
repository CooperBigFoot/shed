//! SnapStore — lazy parquet reader for snap targets.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, BinaryArray, BooleanArray, Float32Array, Int64Array, LargeBinaryArray};
use arrow::datatypes::DataType;
use hfx_core::{AtomId, BoundingBox, MainstemStatus, SnapId, SnapTarget, Weight, WkbGeometry};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, instrument};

use crate::error::SessionError;
use crate::reader::{BboxColIndices, extract_row_group_bbox, require_column};

/// Advance a `f32` value to the next representable float strictly greater than `v`.
///
/// Used to pad degenerate bbox axes by the smallest possible amount, ensuring
/// the result is actually greater than the input regardless of magnitude.
fn next_up_f32(v: f32) -> f32 {
    // Bit-cast to u32, increment the integer, cast back. This is the
    // standard "next representable float" trick for positive finite values.
    // For negative values and -0.0 the increment still moves toward +∞.
    let bits = v.to_bits();
    f32::from_bits(bits + 1)
}

/// Construct a [`BoundingBox`] for a snap target row, padding degenerate axes by epsilon.
///
/// The HFX spec (line 292) allows `bbox_min* <= bbox_max*`, so Point and
/// axis-aligned LineString geometries produce equal min/max values. Since
/// [`BoundingBox::new`] requires strict inequality, we pad equal axes by one
/// ULP rather than rejecting valid snap targets.
fn snap_bbox(
    minx: f32,
    miny: f32,
    maxx: f32,
    maxy: f32,
    row: usize,
) -> Result<BoundingBox, SessionError> {
    // Fast path: non-degenerate bbox (common case).
    if let Ok(bbox) = BoundingBox::new(minx, miny, maxx, maxy) {
        return Ok(bbox);
    }
    // Spec allows degenerate bboxes for snap targets (Points, axis-aligned LineStrings).
    // Bump the max by one ULP on each degenerate axis so that BoundingBox::new()'s
    // strict-inequality requirement is satisfied.
    let padded_maxx = if maxx == minx {
        next_up_f32(minx)
    } else {
        maxx
    };
    let padded_maxy = if maxy == miny {
        next_up_f32(miny)
    } else {
        maxy
    };
    BoundingBox::new(minx, miny, padded_maxx, padded_maxy).map_err(|e| {
        SessionError::invalid_row(
            ARTIFACT,
            row,
            format!("invalid snap bbox even after epsilon padding: {e}"),
        )
    })
}

const ARTIFACT: &str = "snap.parquet";

/// Row-group bounding box with metadata for pruning.
#[derive(Debug, Clone)]
struct RowGroupBbox {
    index: usize,
    bbox: BoundingBox,
    #[allow(dead_code)]
    row_count: usize,
}

/// Lazy reader for snap.parquet with row-group bbox pruning.
#[derive(Debug)]
pub struct SnapStore {
    path: PathBuf,
    row_groups: Vec<RowGroupBbox>,
    groups_without_stats: Vec<usize>,
    total_rows: u64,
    #[allow(dead_code)]
    bbox_col_indices: BboxColIndices,
}

impl SnapStore {
    /// Open `snap.parquet` at `path`, validate its schema, and index
    /// row-group bounding boxes for later pruning.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | File cannot be opened | [`SessionError::Io`] |
    /// | File is not valid Parquet | [`SessionError::ParquetParse`] |
    /// | Required column missing or wrong type | [`SessionError::ParquetSchema`] |
    #[instrument(skip_all, fields(path = %path.display()))]
    pub fn open(path: &Path) -> Result<Self, SessionError> {
        let file = File::open(path).map_err(|e| SessionError::io(ARTIFACT, e))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            }
        })?;

        let metadata = builder.metadata().clone();
        let schema = builder.schema();

        // Validate all required columns exist with the correct types.
        require_column(schema, "id", &DataType::Int64, ARTIFACT)?;
        require_column(schema, "catchment_id", &DataType::Int64, ARTIFACT)?;
        require_column(schema, "weight", &DataType::Float32, ARTIFACT)?;
        require_column(schema, "is_mainstem", &DataType::Boolean, ARTIFACT)?;
        let minx_idx = require_column(schema, "bbox_minx", &DataType::Float32, ARTIFACT)?;
        let miny_idx = require_column(schema, "bbox_miny", &DataType::Float32, ARTIFACT)?;
        let maxx_idx = require_column(schema, "bbox_maxx", &DataType::Float32, ARTIFACT)?;
        let maxy_idx = require_column(schema, "bbox_maxy", &DataType::Float32, ARTIFACT)?;
        require_column(schema, "geometry", &DataType::Binary, ARTIFACT)?;

        let bbox_col_indices = BboxColIndices {
            minx: minx_idx,
            miny: miny_idx,
            maxx: maxx_idx,
            maxy: maxy_idx,
        };

        let mut row_groups = Vec::new();
        let mut groups_without_stats = Vec::new();
        let mut total_rows: u64 = 0;

        for (i, rg) in metadata.row_groups().iter().enumerate() {
            let row_count = rg.num_rows() as usize;
            total_rows += rg.num_rows() as u64;

            match extract_row_group_bbox(rg, &bbox_col_indices) {
                Some(bbox) => row_groups.push(RowGroupBbox {
                    index: i,
                    bbox,
                    row_count,
                }),
                None => groups_without_stats.push(i),
            }
        }

        debug!(
            row_groups = row_groups.len(),
            groups_without_stats = groups_without_stats.len(),
            total_rows,
            "snap store opened"
        );

        Ok(Self {
            path: path.to_path_buf(),
            row_groups,
            groups_without_stats,
            total_rows,
            bbox_col_indices,
        })
    }

    /// Query snap targets whose bounding box intersects `query_bbox`.
    ///
    /// Uses row-group statistics to skip row groups that cannot contain
    /// matching rows, then post-filters by per-row bbox.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | File cannot be re-opened | [`SessionError::Io`] |
    /// | Row group read fails | [`SessionError::RowGroupReadError`] |
    /// | Row fails domain validation | [`SessionError::InvalidRow`] |
    #[instrument(skip_all, fields(path = %self.path.display()))]
    pub fn query_by_bbox(&self, query_bbox: &BoundingBox) -> Result<Vec<SnapTarget>, SessionError> {
        // Collect row group indices that might contain matching rows.
        let mut candidate_indices: Vec<usize> = self
            .row_groups
            .iter()
            .filter(|rg| rg.bbox.intersects(query_bbox))
            .map(|rg| rg.index)
            .collect();

        // Always include row groups that lack statistics.
        candidate_indices.extend_from_slice(&self.groups_without_stats);
        candidate_indices.sort_unstable();

        if candidate_indices.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            candidate_row_groups = candidate_indices.len(),
            "reading candidate row groups"
        );

        let file = File::open(&self.path).map_err(|e| SessionError::io(ARTIFACT, e))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            }
        })?;

        // Pre-compute the absolute start row of each selected row group so that
        // error messages report the correct row index in the file, even after
        // row-group pruning.
        let metadata = builder.metadata().clone();
        let mut rg_absolute_starts: Vec<usize> = Vec::with_capacity(candidate_indices.len());
        let mut cumulative = 0usize;
        for rg_idx in 0..metadata.num_row_groups() {
            if candidate_indices.contains(&rg_idx) {
                rg_absolute_starts.push(cumulative);
            }
            cumulative += metadata.row_group(rg_idx).num_rows() as usize;
        }
        let selected_sizes: Vec<usize> = candidate_indices
            .iter()
            .map(|&rg| metadata.row_group(rg).num_rows() as usize)
            .collect();

        let reader = builder
            .with_row_groups(candidate_indices.clone())
            .build()
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;

        let mut results = Vec::new();
        // `sel_idx` is the index into `candidate_indices` of the row group
        // currently being read. `offset_in_group` is the row within that group.
        let mut sel_idx = 0usize;
        let mut offset_in_group = 0usize;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| SessionError::RowGroupReadError {
                artifact: ARTIFACT,
                row_group: candidate_indices[sel_idx],
                source: e.into(),
            })?;

            let num_rows = batch.num_rows();

            let id_col = batch
                .column_by_name("id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(ARTIFACT, "column 'id' missing or wrong type")
                })?;

            let catchment_id_col = batch
                .column_by_name("catchment_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(
                        ARTIFACT,
                        "column 'catchment_id' missing or wrong type",
                    )
                })?;

            let weight_col = batch
                .column_by_name("weight")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(ARTIFACT, "column 'weight' missing or wrong type")
                })?;

            let is_mainstem_col = batch
                .column_by_name("is_mainstem")
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(
                        ARTIFACT,
                        "column 'is_mainstem' missing or wrong type",
                    )
                })?;

            let bbox_minx_col = batch
                .column_by_name("bbox_minx")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(
                        ARTIFACT,
                        "column 'bbox_minx' missing or wrong type",
                    )
                })?;

            let bbox_miny_col = batch
                .column_by_name("bbox_miny")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(
                        ARTIFACT,
                        "column 'bbox_miny' missing or wrong type",
                    )
                })?;

            let bbox_maxx_col = batch
                .column_by_name("bbox_maxx")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(
                        ARTIFACT,
                        "column 'bbox_maxx' missing or wrong type",
                    )
                })?;

            let bbox_maxy_col = batch
                .column_by_name("bbox_maxy")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(
                        ARTIFACT,
                        "column 'bbox_maxy' missing or wrong type",
                    )
                })?;

            let geometry_col_array = batch.column_by_name("geometry").ok_or_else(|| {
                SessionError::parquet_schema(ARTIFACT, "column 'geometry' missing")
            })?;

            for i in 0..num_rows {
                let absolute_row = rg_absolute_starts[sel_idx] + offset_in_group;
                offset_in_group += 1;

                // Null checks on all non-nullable columns.
                macro_rules! check_null {
                    ($col:expr, $name:expr) => {
                        if $col.is_null(i) {
                            return Err(SessionError::invalid_row(
                                ARTIFACT,
                                absolute_row,
                                format!("null value in non-nullable column \"{}\"", $name),
                            ));
                        }
                    };
                }
                check_null!(id_col, "id");
                check_null!(catchment_id_col, "catchment_id");
                check_null!(weight_col, "weight");
                check_null!(is_mainstem_col, "is_mainstem");
                check_null!(bbox_minx_col, "bbox_minx");
                check_null!(bbox_miny_col, "bbox_miny");
                check_null!(bbox_maxx_col, "bbox_maxx");
                check_null!(bbox_maxy_col, "bbox_maxy");
                check_null!(geometry_col_array, "geometry");

                // Build per-row bbox for post-filtering. Uses epsilon padding for
                // degenerate bboxes (Points, axis-aligned LineStrings) per the spec.
                let row_bbox = snap_bbox(
                    bbox_minx_col.value(i),
                    bbox_miny_col.value(i),
                    bbox_maxx_col.value(i),
                    bbox_maxy_col.value(i),
                    absolute_row,
                )?;

                if !row_bbox.intersects(query_bbox) {
                    continue;
                }

                let id = SnapId::new(id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(ARTIFACT, absolute_row, format!("id error: {e}"))
                })?;

                let catchment_id = AtomId::new(catchment_id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row,
                        format!("catchment_id error: {e}"),
                    )
                })?;

                let weight = Weight::new(weight_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(ARTIFACT, absolute_row, format!("weight error: {e}"))
                })?;

                let mainstem_status = if is_mainstem_col.value(i) {
                    MainstemStatus::Mainstem
                } else {
                    MainstemStatus::Tributary
                };

                let geom_bytes: Vec<u8> =
                    if let Some(arr) = geometry_col_array.as_any().downcast_ref::<BinaryArray>() {
                        arr.value(i).to_vec()
                    } else if let Some(arr) = geometry_col_array
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                    {
                        arr.value(i).to_vec()
                    } else {
                        return Err(SessionError::parquet_schema(
                            ARTIFACT,
                            "column 'geometry' has unsupported type",
                        ));
                    };

                let geometry = WkbGeometry::new(geom_bytes).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row,
                        format!("geometry error: {e}"),
                    )
                })?;

                results.push(SnapTarget::new(
                    id,
                    catchment_id,
                    weight,
                    mainstem_status,
                    row_bbox,
                    geometry,
                ));
            }

            // Advance the selected-group tracker after exhausting this batch.
            if !selected_sizes.is_empty()
                && offset_in_group >= selected_sizes[sel_idx]
                && sel_idx + 1 < candidate_indices.len()
            {
                offset_in_group = 0;
                sel_idx += 1;
            }
        }

        debug!(matched = results.len(), "query_by_bbox complete");
        Ok(results)
    }

    /// Read all catchment IDs referenced by snap targets (projection read of catchment_id column only).
    ///
    /// Used at session open time for referential integrity checks.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | File cannot be opened | [`SessionError::Io`] |
    /// | File is not valid Parquet | [`SessionError::ParquetParse`] |
    /// | `catchment_id` column missing | [`SessionError::ParquetSchema`] |
    /// | Row contains a null `catchment_id` | [`SessionError::InvalidRow`] |
    /// | `catchment_id` value fails domain validation | [`SessionError::InvalidRow`] |
    pub fn read_all_catchment_ids(&self) -> Result<Vec<hfx_core::AtomId>, SessionError> {
        let file = std::fs::File::open(&self.path).map_err(|e| SessionError::io(ARTIFACT, e))?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;

        let parquet_schema = builder.parquet_schema();
        let col_idx = parquet_schema
            .columns()
            .iter()
            .position(|c| c.name() == "catchment_id")
            .ok_or_else(|| {
                SessionError::parquet_schema(ARTIFACT, "missing column \"catchment_id\"")
            })?;

        let mask = parquet::arrow::ProjectionMask::roots(parquet_schema, [col_idx]);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(8192)
            .build()
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;

        let mut ids = Vec::new();
        let mut global_row = 0usize;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e.into(),
            })?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    SessionError::parquet_schema(ARTIFACT, "catchment_id column is not Int64")
                })?;
            for i in 0..batch.num_rows() {
                if col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        global_row + i,
                        "null catchment_id",
                    ));
                }
                let atom_id = hfx_core::AtomId::new(col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        global_row + i,
                        format!("invalid catchment_id: {e}"),
                    )
                })?;
                ids.push(atom_id);
            }
            global_row += batch.num_rows();
        }
        Ok(ids)
    }

    /// Return the total number of snap target rows across all row groups.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BinaryBuilder, BooleanBuilder, Float32Builder, Int64Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::NamedTempFile;

    use super::*;

    /// Minimal valid WKB LineString with two points.
    fn minimal_wkb_linestring(x1: f64, y1: f64, x2: f64, y2: f64) -> Vec<u8> {
        let mut wkb = Vec::new();
        wkb.push(1u8); // little-endian
        wkb.extend_from_slice(&2u32.to_le_bytes()); // linestring type
        wkb.extend_from_slice(&2u32.to_le_bytes()); // 2 points
        wkb.extend_from_slice(&x1.to_le_bytes());
        wkb.extend_from_slice(&y1.to_le_bytes());
        wkb.extend_from_slice(&x2.to_le_bytes());
        wkb.extend_from_slice(&y2.to_le_bytes());
        wkb
    }

    fn snap_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("catchment_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("is_mainstem", DataType::Boolean, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]))
    }

    struct SnapRow {
        id: i64,
        catchment_id: i64,
        weight: f32,
        is_mainstem: bool,
        minx: f32,
        miny: f32,
        maxx: f32,
        maxy: f32,
        geom: Vec<u8>,
    }

    fn write_snap_parquet(rows: &[SnapRow]) -> NamedTempFile {
        let schema = snap_schema();
        let tmp = NamedTempFile::new().unwrap();

        let mut id_b = Int64Builder::new();
        let mut catchment_id_b = Int64Builder::new();
        let mut weight_b = Float32Builder::new();
        let mut is_mainstem_b = BooleanBuilder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();

        for row in rows {
            id_b.append_value(row.id);
            catchment_id_b.append_value(row.catchment_id);
            weight_b.append_value(row.weight);
            is_mainstem_b.append_value(row.is_mainstem);
            minx_b.append_value(row.minx);
            miny_b.append_value(row.miny);
            maxx_b.append_value(row.maxx);
            maxy_b.append_value(row.maxy);
            geom_b.append_value(&row.geom);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_b.finish()),
                Arc::new(catchment_id_b.finish()),
                Arc::new(weight_b.finish()),
                Arc::new(is_mainstem_b.finish()),
                Arc::new(minx_b.finish()),
                Arc::new(miny_b.finish()),
                Arc::new(maxx_b.finish()),
                Arc::new(maxy_b.finish()),
                Arc::new(geom_b.finish()),
            ],
        )
        .unwrap();

        let file = tmp.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        tmp
    }

    #[test]
    fn test_open_valid_snap() {
        let geom = minimal_wkb_linestring(-10.0, -5.0, -9.0, -4.0);
        let rows = vec![
            SnapRow {
                id: 1,
                catchment_id: 10,
                weight: 0.5,
                is_mainstem: true,
                minx: -10.0,
                miny: -5.0,
                maxx: -9.0,
                maxy: -4.0,
                geom: geom.clone(),
            },
            SnapRow {
                id: 2,
                catchment_id: 20,
                weight: 0.8,
                is_mainstem: false,
                minx: 1.0,
                miny: 1.0,
                maxx: 2.0,
                maxy: 2.0,
                geom: geom.clone(),
            },
        ];

        let tmp = write_snap_parquet(&rows);
        let store = SnapStore::open(tmp.path()).unwrap();
        assert_eq!(store.total_rows(), 2);
    }

    #[test]
    fn test_query_by_bbox_returns_matching() {
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let rows = vec![
            SnapRow {
                id: 1,
                catchment_id: 10,
                weight: 0.5,
                is_mainstem: true,
                minx: -10.0,
                miny: -5.0,
                maxx: -9.0,
                maxy: -4.0,
                geom: minimal_wkb_linestring(-10.0, -5.0, -9.0, -4.0),
            },
            SnapRow {
                id: 2,
                catchment_id: 20,
                weight: 0.8,
                is_mainstem: false,
                minx: 1.0,
                miny: 1.0,
                maxx: 2.0,
                maxy: 2.0,
                geom: geom.clone(),
            },
        ];

        let tmp = write_snap_parquet(&rows);
        let store = SnapStore::open(tmp.path()).unwrap();

        let query = BoundingBox::new(0.5, 0.5, 3.0, 3.0).unwrap();
        let results = store.query_by_bbox(&query).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id(), SnapId::new(2).unwrap());
        assert_eq!(results[0].catchment_id(), AtomId::new(20).unwrap());
    }

    #[test]
    fn test_query_by_bbox_empty() {
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let rows = vec![SnapRow {
            id: 1,
            catchment_id: 10,
            weight: 0.5,
            is_mainstem: true,
            minx: 1.0,
            miny: 1.0,
            maxx: 2.0,
            maxy: 2.0,
            geom: geom.clone(),
        }];

        let tmp = write_snap_parquet(&rows);
        let store = SnapStore::open(tmp.path()).unwrap();

        // Disjoint bbox — no overlap with [1,2]x[1,2].
        let query = BoundingBox::new(10.0, 10.0, 20.0, 20.0).unwrap();
        let results = store.query_by_bbox(&query).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_mainstem_and_tributary() {
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let rows = vec![
            SnapRow {
                id: 1,
                catchment_id: 10,
                weight: 0.5,
                is_mainstem: true,
                minx: 1.0,
                miny: 1.0,
                maxx: 2.0,
                maxy: 2.0,
                geom: geom.clone(),
            },
            SnapRow {
                id: 2,
                catchment_id: 10,
                weight: 0.3,
                is_mainstem: false,
                minx: 1.5,
                miny: 1.5,
                maxx: 2.5,
                maxy: 2.5,
                geom: geom.clone(),
            },
        ];

        let tmp = write_snap_parquet(&rows);
        let store = SnapStore::open(tmp.path()).unwrap();

        let query = BoundingBox::new(0.0, 0.0, 5.0, 5.0).unwrap();
        let mut results = store.query_by_bbox(&query).unwrap();

        // Sort by id for deterministic assertions.
        results.sort_by_key(|r| r.id().get());

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].mainstem_status(), MainstemStatus::Mainstem);
        assert_eq!(results[1].mainstem_status(), MainstemStatus::Tributary);
    }

    /// Minimal valid WKB Point geometry.
    fn minimal_wkb_point(x: f64, y: f64) -> Vec<u8> {
        let mut wkb = Vec::new();
        wkb.push(1u8); // little-endian
        wkb.extend_from_slice(&1u32.to_le_bytes()); // point type
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
        wkb
    }

    #[test]
    fn test_degenerate_bbox_point() {
        // A snap target with a Point geometry: minx==maxx and miny==maxy.
        // The spec allows this; snap_bbox() must pad it instead of erroring.
        let geom = minimal_wkb_point(5.0, 10.0);
        let rows = vec![SnapRow {
            id: 1,
            catchment_id: 10,
            weight: 1.0,
            is_mainstem: false,
            minx: 5.0,
            miny: 10.0,
            maxx: 5.0,  // equal to minx — degenerate x axis
            maxy: 10.0, // equal to miny — degenerate y axis
            geom,
        }];

        let tmp = write_snap_parquet(&rows);
        let store = SnapStore::open(tmp.path()).unwrap();

        // Query bbox that covers the point.
        let query = BoundingBox::new(4.0, 9.0, 6.0, 11.0).unwrap();
        let results = store.query_by_bbox(&query).unwrap();

        assert_eq!(results.len(), 1, "point snap target must be returned");
        assert_eq!(results[0].id(), SnapId::new(1).unwrap());
    }

    #[test]
    fn test_degenerate_bbox_vertical_line() {
        // A snap target where minx==maxx (vertical LineString), but miny < maxy.
        let geom = minimal_wkb_linestring(5.0, 9.0, 5.0, 11.0);
        let rows = vec![SnapRow {
            id: 2,
            catchment_id: 20,
            weight: 0.5,
            is_mainstem: true,
            minx: 5.0,
            miny: 9.0,
            maxx: 5.0, // equal to minx — degenerate x axis only
            maxy: 11.0,
            geom,
        }];

        let tmp = write_snap_parquet(&rows);
        let store = SnapStore::open(tmp.path()).unwrap();

        let query = BoundingBox::new(4.0, 8.0, 6.0, 12.0).unwrap();
        let results = store.query_by_bbox(&query).unwrap();

        assert_eq!(
            results.len(),
            1,
            "vertical-line snap target must be returned"
        );
        assert_eq!(results[0].id(), SnapId::new(2).unwrap());
    }

    #[test]
    fn test_reversed_bbox_is_rejected() {
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let rows = vec![SnapRow {
            id: 1,
            catchment_id: 10,
            weight: 1.0,
            is_mainstem: true,
            minx: 2.0,
            miny: 1.0,
            maxx: 1.0,
            maxy: 2.0,
            geom,
        }];

        let tmp = write_snap_parquet(&rows);
        let store = SnapStore::open(tmp.path()).unwrap();

        let query = BoundingBox::new(0.0, 0.0, 3.0, 3.0).unwrap();
        let err = store.query_by_bbox(&query).unwrap_err();

        assert!(
            matches!(err, SessionError::InvalidRow { row: 0, .. }),
            "expected InvalidRow at row 0, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("bbox") || msg.contains("invalid snap bbox"),
            "expected bbox-related error, got: {msg}"
        );
    }

    #[test]
    fn test_null_id_returns_error() {
        use arrow::array::Int64Builder;

        // Write a snap parquet where the id column is declared nullable and
        // row 0 has a null id.  The reader must reject this with InvalidRow.
        let null_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true), // nullable so writer accepts null
            Field::new("catchment_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("is_mainstem", DataType::Boolean, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let mut id_b = Int64Builder::new();
        id_b.append_null(); // null id at row 0
        let mut cid_b = Int64Builder::new();
        cid_b.append_value(10);
        let mut w_b = Float32Builder::new();
        w_b.append_value(1.0);
        let mut ms_b = BooleanBuilder::new();
        ms_b.append_value(false);
        let mut minx_b = Float32Builder::new();
        minx_b.append_value(1.0);
        let mut miny_b = Float32Builder::new();
        miny_b.append_value(1.0);
        let mut maxx_b = Float32Builder::new();
        maxx_b.append_value(2.0);
        let mut maxy_b = Float32Builder::new();
        maxy_b.append_value(2.0);
        let mut geom_b = BinaryBuilder::new();
        geom_b.append_value(&minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0));

        let batch = RecordBatch::try_new(
            null_schema.clone(),
            vec![
                Arc::new(id_b.finish()),
                Arc::new(cid_b.finish()),
                Arc::new(w_b.finish()),
                Arc::new(ms_b.finish()),
                Arc::new(minx_b.finish()),
                Arc::new(miny_b.finish()),
                Arc::new(maxx_b.finish()),
                Arc::new(maxy_b.finish()),
                Arc::new(geom_b.finish()),
            ],
        )
        .unwrap();

        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, null_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let store = SnapStore::open(tmp.path()).unwrap();
        let query = BoundingBox::new(0.0, 0.0, 5.0, 5.0).unwrap();
        let err = store.query_by_bbox(&query).unwrap_err();
        assert!(
            matches!(err, SessionError::InvalidRow { .. }),
            "expected InvalidRow for null id, got {err:?}"
        );
    }

    #[test]
    fn test_null_weight_returns_error() {
        use arrow::array::Float32Builder as F32B;

        // Row 0 has a null weight column.
        let null_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("catchment_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, true), // nullable so writer accepts null
            Field::new("is_mainstem", DataType::Boolean, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let mut id_b = Int64Builder::new();
        id_b.append_value(1);
        let mut cid_b = Int64Builder::new();
        cid_b.append_value(10);
        let mut w_b = F32B::new();
        w_b.append_null(); // null weight at row 0
        let mut ms_b = BooleanBuilder::new();
        ms_b.append_value(false);
        let mut minx_b = Float32Builder::new();
        minx_b.append_value(1.0);
        let mut miny_b = Float32Builder::new();
        miny_b.append_value(1.0);
        let mut maxx_b = Float32Builder::new();
        maxx_b.append_value(2.0);
        let mut maxy_b = Float32Builder::new();
        maxy_b.append_value(2.0);
        let mut geom_b = BinaryBuilder::new();
        geom_b.append_value(&minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0));

        let batch = RecordBatch::try_new(
            null_schema.clone(),
            vec![
                Arc::new(id_b.finish()),
                Arc::new(cid_b.finish()),
                Arc::new(w_b.finish()),
                Arc::new(ms_b.finish()),
                Arc::new(minx_b.finish()),
                Arc::new(miny_b.finish()),
                Arc::new(maxx_b.finish()),
                Arc::new(maxy_b.finish()),
                Arc::new(geom_b.finish()),
            ],
        )
        .unwrap();

        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, null_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let store = SnapStore::open(tmp.path()).unwrap();
        let query = BoundingBox::new(0.0, 0.0, 5.0, 5.0).unwrap();
        let err = store.query_by_bbox(&query).unwrap_err();
        assert!(
            matches!(err, SessionError::InvalidRow { .. }),
            "expected InvalidRow for null weight, got {err:?}"
        );
    }

    #[test]
    fn test_missing_file() {
        let result = SnapStore::open(Path::new("/nonexistent/path/snap.parquet"));
        assert!(matches!(result, Err(SessionError::Io { .. })));
    }

    #[test]
    fn test_wrong_schema() {
        // Write a parquet file that's missing the 'weight' column.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("catchment_id", DataType::Int64, false),
            // 'weight' intentionally omitted
            Field::new("is_mainstem", DataType::Boolean, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let tmp = NamedTempFile::new().unwrap();

        let mut id_b = Int64Builder::new();
        let mut cid_b = Int64Builder::new();
        let mut ms_b = BooleanBuilder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();

        id_b.append_value(1);
        cid_b.append_value(10);
        ms_b.append_value(true);
        minx_b.append_value(1.0);
        miny_b.append_value(1.0);
        maxx_b.append_value(2.0);
        maxy_b.append_value(2.0);
        geom_b.append_value(&minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_b.finish()),
                Arc::new(cid_b.finish()),
                Arc::new(ms_b.finish()),
                Arc::new(minx_b.finish()),
                Arc::new(miny_b.finish()),
                Arc::new(maxx_b.finish()),
                Arc::new(maxy_b.finish()),
                Arc::new(geom_b.finish()),
            ],
        )
        .unwrap();

        let file = tmp.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let result = SnapStore::open(tmp.path());
        assert!(matches!(result, Err(SessionError::ParquetSchema { .. })));
    }
}
