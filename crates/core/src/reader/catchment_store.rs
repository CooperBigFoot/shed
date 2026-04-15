//! CatchmentStore — lazy parquet reader with row-group bbox pruning.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use arrow::array::{Array, BinaryArray, Float32Array, Int64Array, LargeBinaryArray};
use arrow::datatypes::{DataType, Schema};
use hfx_core::{AreaKm2, AtomId, BoundingBox, CatchmentAtom, WkbGeometry};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, instrument};

use super::{BboxColIndices, extract_row_group_bbox, require_column};
use crate::error::SessionError;

const ARTIFACT: &str = "catchments.parquet";

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Row-group spatial metadata pre-extracted from Parquet column statistics.
#[derive(Debug, Clone)]
struct RowGroupBbox {
    index: usize,
    bbox: BoundingBox,
    #[allow(dead_code)]
    row_count: usize,
}

// ---------------------------------------------------------------------------
// Public type
// ---------------------------------------------------------------------------

/// Lazy reader for catchments.parquet with row-group bbox pruning.
///
/// Holds the file path and pre-extracted row-group metadata. Re-opens the
/// file on each query. Does not hold file handles open.
#[derive(Debug)]
pub struct CatchmentStore {
    path: PathBuf,
    row_groups: Vec<RowGroupBbox>,
    /// Row groups that lacked bbox statistics (included conservatively in all queries).
    groups_without_stats: Vec<usize>,
    total_rows: u64,
    #[allow(dead_code)]
    bbox_col_indices: BboxColIndices,
}

impl CatchmentStore {
    /// Open `catchments.parquet` at `path`, validate the schema, and
    /// pre-extract row-group bounding-box statistics.
    ///
    /// No row data is loaded; the file is closed after the metadata pass.
    ///
    /// # Errors
    ///
    /// | Condition | Variant |
    /// |---|---|
    /// | File not found / unreadable | [`SessionError::Io`] |
    /// | Not valid Parquet | [`SessionError::ParquetParse`] |
    /// | Missing or mis-typed column | [`SessionError::ParquetSchema`] |
    #[instrument(skip_all, fields(path = %path.display()))]
    pub fn open(path: &Path) -> Result<Self, SessionError> {
        let file =
            std::fs::File::open(path).map_err(|e| SessionError::io(ARTIFACT, e))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| SessionError::ParquetParse { artifact: ARTIFACT, source: e })?;

        // --- Arrow schema validation ---
        let arrow_schema = builder.schema();
        require_column(arrow_schema, "id", &DataType::Int64, ARTIFACT)?;
        require_column(arrow_schema, "area_km2", &DataType::Float32, ARTIFACT)?;
        require_column(arrow_schema, "up_area_km2", &DataType::Float32, ARTIFACT)?;
        require_column(arrow_schema, "bbox_minx", &DataType::Float32, ARTIFACT)?;
        require_column(arrow_schema, "bbox_miny", &DataType::Float32, ARTIFACT)?;
        require_column(arrow_schema, "bbox_maxx", &DataType::Float32, ARTIFACT)?;
        require_column(arrow_schema, "bbox_maxy", &DataType::Float32, ARTIFACT)?;
        require_column(arrow_schema, "geometry", &DataType::Binary, ARTIFACT)?;

        // --- Parquet schema column indices for bbox statistics ---
        let parquet_schema = builder.parquet_schema();
        let find_col = |name: &str| -> Result<usize, SessionError> {
            parquet_schema
                .columns()
                .iter()
                .position(|c| c.name() == name)
                .ok_or_else(|| {
                    SessionError::parquet_schema(
                        ARTIFACT,
                        format!("missing column {name}"),
                    )
                })
        };

        let bbox_indices = BboxColIndices {
            minx: find_col("bbox_minx")?,
            miny: find_col("bbox_miny")?,
            maxx: find_col("bbox_maxx")?,
            maxy: find_col("bbox_maxy")?,
        };

        // --- Row-group metadata pass ---
        let metadata = builder.metadata().clone();
        let mut row_groups = Vec::new();
        let mut groups_without_stats = Vec::new();
        let mut total_rows = 0u64;

        for rg_idx in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(rg_idx);
            total_rows += rg.num_rows() as u64;
            match extract_row_group_bbox(rg, &bbox_indices) {
                Some(bbox) => row_groups.push(RowGroupBbox {
                    index: rg_idx,
                    bbox,
                    row_count: rg.num_rows() as usize,
                }),
                None => groups_without_stats.push(rg_idx),
            }
        }

        debug!(
            total_rows,
            row_groups_with_stats = row_groups.len(),
            row_groups_without_stats = groups_without_stats.len(),
            "opened catchment store"
        );

        Ok(Self {
            path: path.to_owned(),
            row_groups,
            groups_without_stats,
            total_rows,
            bbox_col_indices: bbox_indices,
        })
    }

    /// Return all [`CatchmentAtom`]s whose bounding boxes intersect `query_bbox`.
    ///
    /// Row groups whose statistics do not cover the query area are skipped.
    /// Row groups without statistics are always scanned conservatively.
    ///
    /// # Errors
    ///
    /// | Condition | Variant |
    /// |---|---|
    /// | File not found / unreadable | [`SessionError::Io`] |
    /// | Parquet decode error | [`SessionError::ParquetParse`] |
    /// | Row fails domain validation | [`SessionError::InvalidRow`] |
    #[instrument(skip_all, fields(path = %self.path.display()))]
    pub fn query_by_bbox(
        &self,
        query_bbox: &BoundingBox,
    ) -> Result<Vec<CatchmentAtom>, SessionError> {
        let mut matching: Vec<usize> = self
            .row_groups
            .iter()
            .filter(|rg| rg.bbox.intersects(query_bbox))
            .map(|rg| rg.index)
            .collect();
        matching.extend(&self.groups_without_stats);
        matching.sort_unstable();
        matching.dedup();

        if matching.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            row_groups = matching.len(),
            "scanning row groups for bbox query"
        );

        let file = std::fs::File::open(&self.path)
            .map_err(|e| SessionError::io(ARTIFACT, e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| SessionError::ParquetParse { artifact: ARTIFACT, source: e })?;

        // Pre-compute absolute start row for each selected row group so that
        // error messages report the correct file-level row index even after
        // row-group pruning.
        let all_metadata = builder.metadata().clone();
        let mut rg_absolute_starts: Vec<usize> = Vec::new();
        let mut cumulative = 0usize;
        for rg_idx in 0..all_metadata.num_row_groups() {
            if matching.contains(&rg_idx) {
                rg_absolute_starts.push(cumulative);
            }
            cumulative += all_metadata.row_group(rg_idx).num_rows() as usize;
        }

        let selected_sizes: Vec<usize> = matching
            .iter()
            .map(|&rg| all_metadata.row_group(rg).num_rows() as usize)
            .collect();

        let reader = builder
            .with_row_groups(matching.clone())
            .with_batch_size(8192)
            .build()
            .map_err(|e| SessionError::ParquetParse { artifact: ARTIFACT, source: e })?;

        let mut results = Vec::new();
        let mut sel_idx = 0usize;
        let mut offset_in_group = 0usize;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| SessionError::RowGroupReadError {
                artifact: ARTIFACT,
                row_group: matching[sel_idx],
                source: parquet::errors::ParquetError::ArrowError(e.to_string()),
            })?;

            let absolute_row = rg_absolute_starts[sel_idx] + offset_in_group;
            let rows = extract_atoms_from_batch(&batch, absolute_row, ARTIFACT)?;

            for atom in rows {
                if atom.bbox().intersects(query_bbox) {
                    results.push(atom);
                }
            }

            offset_in_group += batch.num_rows();
            if sel_idx + 1 < matching.len()
                && offset_in_group >= selected_sizes[sel_idx]
            {
                offset_in_group = 0;
                sel_idx += 1;
            }
        }

        Ok(results)
    }

    /// Return the [`CatchmentAtom`]s whose IDs appear in `ids`.
    ///
    /// All row groups are scanned (no bbox pruning possible for ID lookups).
    ///
    /// # Errors
    ///
    /// | Condition | Variant |
    /// |---|---|
    /// | File not found / unreadable | [`SessionError::Io`] |
    /// | Parquet decode error | [`SessionError::ParquetParse`] |
    /// | Row fails domain validation | [`SessionError::InvalidRow`] |
    #[instrument(skip_all, fields(path = %self.path.display()))]
    pub fn query_by_ids(
        &self,
        ids: &[AtomId],
    ) -> Result<Vec<CatchmentAtom>, SessionError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let id_set: HashSet<AtomId> = ids.iter().copied().collect();

        let file = std::fs::File::open(&self.path)
            .map_err(|e| SessionError::io(ARTIFACT, e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| SessionError::ParquetParse { artifact: ARTIFACT, source: e })?;
        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|e| SessionError::ParquetParse { artifact: ARTIFACT, source: e })?;

        let mut results = Vec::new();
        let mut global_row = 0usize;

        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| SessionError::ParquetParse {
                    artifact: ARTIFACT,
                    source: parquet::errors::ParquetError::ArrowError(e.to_string()),
                })?;

            let rows =
                extract_atoms_from_batch(&batch, global_row, ARTIFACT)?;

            for atom in rows {
                if id_set.contains(&atom.id()) {
                    results.push(atom);
                }
            }

            global_row += batch.num_rows();
        }

        Ok(results)
    }

    /// Read all atom IDs from the catchments file (projection read of the id column only).
    ///
    /// Used at session open time for referential integrity checks against the graph.
    ///
    /// # Errors
    ///
    /// | Condition | Variant |
    /// |---|---|
    /// | File not found / unreadable | [`SessionError::Io`] |
    /// | Not valid Parquet | [`SessionError::ParquetParse`] |
    /// | Missing or mis-typed id column | [`SessionError::ParquetSchema`] |
    /// | Null value in id column | [`SessionError::InvalidRow`] |
    pub fn read_all_ids(&self) -> Result<Vec<AtomId>, SessionError> {
        let file =
            std::fs::File::open(&self.path).map_err(|e| SessionError::io(ARTIFACT, e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| SessionError::ParquetParse { artifact: ARTIFACT, source: e })?;

        let parquet_schema = builder.parquet_schema();
        let id_col_idx = parquet_schema
            .columns()
            .iter()
            .position(|c| c.name() == "id")
            .ok_or_else(|| {
                SessionError::parquet_schema(ARTIFACT, "missing column \"id\"")
            })?;

        let mask = parquet::arrow::ProjectionMask::roots(parquet_schema, [id_col_idx]);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(8192)
            .build()
            .map_err(|e| SessionError::ParquetParse { artifact: ARTIFACT, source: e })?;

        let mut ids = Vec::new();
        let mut global_row = 0usize;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| SessionError::ParquetParse {
                    artifact: ARTIFACT,
                    source: parquet::errors::ParquetError::ArrowError(e.to_string()),
                })?;
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    SessionError::parquet_schema(ARTIFACT, "id column is not Int64")
                })?;
            for i in 0..batch.num_rows() {
                if id_col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        global_row + i,
                        "null id",
                    ));
                }
                let atom_id = AtomId::new(id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        global_row + i,
                        format!("invalid atom id: {e}"),
                    )
                })?;
                ids.push(atom_id);
            }
            global_row += batch.num_rows();
        }

        Ok(ids)
    }

    /// Return the total number of rows in the Parquet file.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }
}

// ---------------------------------------------------------------------------
// Batch extraction helper
// ---------------------------------------------------------------------------

/// Extract all [`CatchmentAtom`]s from one Arrow record batch.
///
/// `row_offset` is the global row index of the first row in this batch,
/// used in error messages.
fn extract_atoms_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    row_offset: usize,
    artifact: &'static str,
) -> Result<Vec<CatchmentAtom>, SessionError> {
    let schema = batch.schema();

    let id_col = col_as::<Int64Array>(batch, &schema, "id", artifact)?;
    let area_col = col_as::<Float32Array>(batch, &schema, "area_km2", artifact)?;
    let up_area_col =
        col_as::<Float32Array>(batch, &schema, "up_area_km2", artifact)?;
    let minx_col = col_as::<Float32Array>(batch, &schema, "bbox_minx", artifact)?;
    let miny_col = col_as::<Float32Array>(batch, &schema, "bbox_miny", artifact)?;
    let maxx_col = col_as::<Float32Array>(batch, &schema, "bbox_maxx", artifact)?;
    let maxy_col = col_as::<Float32Array>(batch, &schema, "bbox_maxy", artifact)?;

    // geometry may be Binary or LargeBinary
    let geom_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "geometry")
        .ok_or_else(|| {
            SessionError::parquet_schema(artifact, "missing column \"geometry\"")
        })?;
    let geom_array = batch.column(geom_idx);

    let n = batch.num_rows();
    let mut atoms = Vec::with_capacity(n);

    for i in 0..n {
        let global_i = row_offset + i;

        if id_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"id\"",
            ));
        }
        let raw_id = id_col.value(i);
        let atom_id = AtomId::new(raw_id).map_err(|e| {
            SessionError::invalid_row(artifact, global_i, format!("id: {e}"))
        })?;

        if area_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"area_km2\"",
            ));
        }
        let area = AreaKm2::new(area_col.value(i)).map_err(|e| {
            SessionError::invalid_row(artifact, global_i, format!("area_km2: {e}"))
        })?;

        let upstream_area = if up_area_col.is_null(i) {
            None
        } else {
            Some(AreaKm2::new(up_area_col.value(i)).map_err(|e| {
                SessionError::invalid_row(
                    artifact,
                    global_i,
                    format!("up_area_km2: {e}"),
                )
            })?)
        };

        if minx_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"bbox_minx\"",
            ));
        }
        if miny_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"bbox_miny\"",
            ));
        }
        if maxx_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"bbox_maxx\"",
            ));
        }
        if maxy_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"bbox_maxy\"",
            ));
        }
        let bbox =
            BoundingBox::new(
                minx_col.value(i),
                miny_col.value(i),
                maxx_col.value(i),
                maxy_col.value(i),
            )
            .map_err(|e| {
                SessionError::invalid_row(artifact, global_i, format!("bbox: {e}"))
            })?;

        // Check geometry nullability before dispatching on array type.
        let geom_is_null = if let Some(arr) =
            geom_array.as_any().downcast_ref::<BinaryArray>()
        {
            arr.is_null(i)
        } else if let Some(arr) =
            geom_array.as_any().downcast_ref::<LargeBinaryArray>()
        {
            arr.is_null(i)
        } else {
            return Err(SessionError::parquet_schema(
                artifact,
                "geometry column is not Binary or LargeBinary",
            ));
        };
        if geom_is_null {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"geometry\"",
            ));
        }

        let geom_bytes: Vec<u8> = if let Some(arr) =
            geom_array.as_any().downcast_ref::<BinaryArray>()
        {
            arr.value(i).to_vec()
        } else if let Some(arr) =
            geom_array.as_any().downcast_ref::<LargeBinaryArray>()
        {
            arr.value(i).to_vec()
        } else {
            return Err(SessionError::parquet_schema(
                artifact,
                "geometry column is not Binary or LargeBinary",
            ));
        };

        let geometry = WkbGeometry::new(geom_bytes).map_err(|e| {
            SessionError::invalid_row(artifact, global_i, format!("geometry: {e}"))
        })?;

        atoms.push(CatchmentAtom::new(atom_id, area, upstream_area, bbox, geometry));
    }

    Ok(atoms)
}

/// Downcast a named column in `batch` to a typed Arrow array.
fn col_as<'a, T: 'static>(
    batch: &'a arrow::record_batch::RecordBatch,
    schema: &Schema,
    name: &str,
    artifact: &'static str,
) -> Result<&'a T, SessionError> {
    let idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == name)
        .ok_or_else(|| {
            SessionError::parquet_schema(artifact, format!("missing column \"{name}\""))
        })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            SessionError::parquet_schema(
                artifact,
                format!("column \"{name}\" has unexpected array type"),
            )
        })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BinaryBuilder, Float32Builder, Int64Builder, RecordBatch,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use tempfile::NamedTempFile;

    use super::*;

    // -----------------------------------------------------------------------
    // Fixture helpers
    // -----------------------------------------------------------------------

    /// Minimal valid WKB polygon bytes for a small square.
    fn minimal_wkb_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<u8> {
        let mut wkb = Vec::new();
        wkb.push(1u8); // little-endian
        wkb.extend_from_slice(&3u32.to_le_bytes()); // polygon type
        wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
        wkb.extend_from_slice(&5u32.to_le_bytes()); // 5 points (closed)
        for (x, y) in [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ] {
            wkb.extend_from_slice(&x.to_le_bytes());
            wkb.extend_from_slice(&y.to_le_bytes());
        }
        wkb
    }

    fn catchments_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]))
    }

    /// `atoms`: (id, area_km2, up_area_km2, [minx, miny, maxx, maxy])
    fn write_fixture(
        path: &std::path::Path,
        atoms: &[(i64, f32, Option<f32>, [f32; 4])],
        row_group_size: usize,
    ) {
        let schema = catchments_schema();
        let props = WriterProperties::builder()
            .set_max_row_group_size(row_group_size)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let file = std::fs::File::create(path).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        let mut ids = Int64Builder::new();
        let mut areas = Float32Builder::new();
        let mut up_areas = Float32Builder::new();
        let mut minxs = Float32Builder::new();
        let mut minys = Float32Builder::new();
        let mut maxxs = Float32Builder::new();
        let mut maxys = Float32Builder::new();
        let mut geoms = BinaryBuilder::new();

        for &(id, area, up_area, bbox) in atoms {
            ids.append_value(id);
            areas.append_value(area);
            match up_area {
                Some(v) => up_areas.append_value(v),
                None => up_areas.append_null(),
            }
            minxs.append_value(bbox[0]);
            minys.append_value(bbox[1]);
            maxxs.append_value(bbox[2]);
            maxys.append_value(bbox[3]);
            let wkb = minimal_wkb_polygon(
                bbox[0] as f64,
                bbox[1] as f64,
                bbox[2] as f64,
                bbox[3] as f64,
            );
            geoms.append_value(&wkb);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ids.finish()),
                Arc::new(areas.finish()),
                Arc::new(up_areas.finish()),
                Arc::new(minxs.finish()),
                Arc::new(minys.finish()),
                Arc::new(maxxs.finish()),
                Arc::new(maxys.finish()),
                Arc::new(geoms.finish()),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_open_valid_catchments() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 10.0f32, Some(100.0f32), [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, Some(200.0), [1.0, 0.0, 2.0, 1.0]),
            (3, 30.0, None, [2.0, 0.0, 3.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1024);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        assert_eq!(store.total_rows(), 3);
    }

    #[test]
    fn test_query_by_bbox_returns_matching() {
        let tmp = NamedTempFile::new().unwrap();
        // Three spatially separated atoms
        let atoms = [
            (1i64, 10.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, None, [10.0, 0.0, 11.0, 1.0]),
            (3, 30.0, None, [20.0, 0.0, 21.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1024);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        // Query for only the first atom
        let q = BoundingBox::new(0.0, 0.0, 1.5, 1.5).unwrap();
        let results = store.query_by_bbox(&q).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id(), AtomId::new(1).unwrap());
    }

    #[test]
    fn test_query_by_bbox_returns_empty_for_no_overlap() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 10.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, None, [2.0, 0.0, 3.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1024);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        // Disjoint query — west of the data
        let q = BoundingBox::new(-10.0, -10.0, -5.0, -5.0).unwrap();
        let results = store.query_by_bbox(&q).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_bbox_pruning_skips_row_groups() {
        let tmp = NamedTempFile::new().unwrap();
        // 6 atoms in 3 row groups (size=2); spatially separated clusters
        let atoms = [
            (1i64, 1.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 1.0, None, [0.1, 0.1, 0.9, 0.9]),
            (3, 1.0, None, [10.0, 0.0, 11.0, 1.0]),
            (4, 1.0, None, [10.1, 0.1, 10.9, 0.9]),
            (5, 1.0, None, [20.0, 0.0, 21.0, 1.0]),
            (6, 1.0, None, [20.1, 0.1, 20.9, 0.9]),
        ];
        write_fixture(tmp.path(), &atoms, 2);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        assert_eq!(store.total_rows(), 6);

        // Query that should only intersect the first row group (atoms 1 & 2)
        let q = BoundingBox::new(0.0, 0.0, 2.0, 1.0).unwrap();
        let results = store.query_by_bbox(&q).unwrap();
        assert_eq!(results.len(), 2);
        let ids: Vec<i64> = results.iter().map(|a| a.id().get()).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    #[test]
    fn test_query_by_ids() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 10.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, None, [2.0, 0.0, 3.0, 1.0]),
            (3, 30.0, None, [4.0, 0.0, 5.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1024);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let ids = [AtomId::new(1).unwrap(), AtomId::new(3).unwrap()];
        let results = store.query_by_ids(&ids).unwrap();
        assert_eq!(results.len(), 2);
        let result_ids: Vec<i64> = results.iter().map(|a| a.id().get()).collect();
        assert!(result_ids.contains(&1));
        assert!(result_ids.contains(&3));
        assert!(!result_ids.contains(&2));
    }

    #[test]
    fn test_nullable_up_area() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 5.0f32, Some(50.0f32), [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 5.0, None, [2.0, 0.0, 3.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1024);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let q = BoundingBox::new(-1.0, -1.0, 4.0, 2.0).unwrap();
        let mut results = store.query_by_bbox(&q).unwrap();
        results.sort_by_key(|a| a.id().get());

        assert_eq!(results.len(), 2);
        let up1 = results[0].upstream_area().map(|a| a.get());
        let up2 = results[1].upstream_area();
        assert!((up1.unwrap() - 50.0f32).abs() < f32::EPSILON);
        assert!(up2.is_none());
    }

    #[test]
    fn test_missing_file() {
        let result = CatchmentStore::open(Path::new("/nonexistent/path/catchments.parquet"));
        assert!(matches!(result, Err(SessionError::Io { .. })));
    }

    #[test]
    fn test_wrong_schema() {
        let tmp = NamedTempFile::new().unwrap();
        // Write a Parquet file with an incompatible schema (missing most columns)
        let schema = Arc::new(Schema::new(vec![
            Field::new("not_id", DataType::Int64, false),
        ]));
        let props = WriterProperties::builder().build();
        let file = std::fs::File::create(tmp.path()).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Builder::new().finish())],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let result = CatchmentStore::open(tmp.path());
        assert!(matches!(result, Err(SessionError::ParquetSchema { .. })));
    }

    // -----------------------------------------------------------------------
    // Null-check tests
    // -----------------------------------------------------------------------

    /// Write a parquet fixture using a custom schema where one column is
    /// nullable, then insert a null at position `null_row` in that column.
    ///
    /// All other columns get valid values. The `null_col` name must be one of
    /// the standard schema columns.
    fn write_fixture_with_null(
        path: &std::path::Path,
        null_col: &str,
        null_row: usize,
    ) {
        // Build a schema with the target column overridden to nullable=true.
        let fields: Vec<Field> = vec![
            Field::new(
                "id",
                DataType::Int64,
                null_col == "id",
            ),
            Field::new(
                "area_km2",
                DataType::Float32,
                null_col == "area_km2",
            ),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new(
                "bbox_minx",
                DataType::Float32,
                null_col == "bbox_minx",
            ),
            Field::new(
                "bbox_miny",
                DataType::Float32,
                null_col == "bbox_miny",
            ),
            Field::new(
                "bbox_maxx",
                DataType::Float32,
                null_col == "bbox_maxx",
            ),
            Field::new(
                "bbox_maxy",
                DataType::Float32,
                null_col == "bbox_maxy",
            ),
            Field::new(
                "geometry",
                DataType::Binary,
                null_col == "geometry",
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        // Write 3 rows; row `null_row` gets a null in the target column.
        let n = 3usize;
        let mut ids = Int64Builder::new();
        let mut areas = Float32Builder::new();
        let mut up_areas = Float32Builder::new();
        let mut minxs = Float32Builder::new();
        let mut minys = Float32Builder::new();
        let mut maxxs = Float32Builder::new();
        let mut maxys = Float32Builder::new();
        let mut geoms = BinaryBuilder::new();

        for row in 0..n {
            let is_null = row == null_row;

            if null_col == "id" && is_null {
                ids.append_null();
            } else {
                ids.append_value(row as i64 + 1);
            }

            if null_col == "area_km2" && is_null {
                areas.append_null();
            } else {
                areas.append_value(1.0f32);
            }

            up_areas.append_null(); // always nullable

            if null_col == "bbox_minx" && is_null {
                minxs.append_null();
            } else {
                minxs.append_value(row as f32);
            }
            if null_col == "bbox_miny" && is_null {
                minys.append_null();
            } else {
                minys.append_value(0.0f32);
            }
            if null_col == "bbox_maxx" && is_null {
                maxxs.append_null();
            } else {
                maxxs.append_value(row as f32 + 1.0);
            }
            if null_col == "bbox_maxy" && is_null {
                maxys.append_null();
            } else {
                maxys.append_value(1.0f32);
            }

            if null_col == "geometry" && is_null {
                geoms.append_null();
            } else {
                let wkb = minimal_wkb_polygon(
                    row as f64,
                    0.0,
                    row as f64 + 1.0,
                    1.0,
                );
                geoms.append_value(&wkb);
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ids.finish()),
                Arc::new(areas.finish()),
                Arc::new(up_areas.finish()),
                Arc::new(minxs.finish()),
                Arc::new(minys.finish()),
                Arc::new(maxxs.finish()),
                Arc::new(maxys.finish()),
                Arc::new(geoms.finish()),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_null_id_returns_error() {
        let tmp = NamedTempFile::new().unwrap();
        write_fixture_with_null(tmp.path(), "id", 1);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        // Cover all 3 rows (lon ∈ [0,3], lat ∈ [0,1])
        let q = BoundingBox::new(0.0, 0.0, 5.0, 2.0).unwrap();
        let result = store.query_by_bbox(&q);
        assert!(
            matches!(result, Err(SessionError::InvalidRow { ref detail, .. }) if detail.contains("null")),
            "expected InvalidRow with 'null' detail, got: {result:?}"
        );
    }

    #[test]
    fn test_null_area_returns_error() {
        let tmp = NamedTempFile::new().unwrap();
        write_fixture_with_null(tmp.path(), "area_km2", 0);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let q = BoundingBox::new(0.0, 0.0, 5.0, 2.0).unwrap();
        let result = store.query_by_bbox(&q);
        assert!(
            matches!(result, Err(SessionError::InvalidRow { ref detail, .. }) if detail.contains("null")),
            "expected InvalidRow with 'null' detail, got: {result:?}"
        );
    }

    #[test]
    fn test_null_geometry_returns_error() {
        let tmp = NamedTempFile::new().unwrap();
        write_fixture_with_null(tmp.path(), "geometry", 2);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let q = BoundingBox::new(0.0, 0.0, 5.0, 2.0).unwrap();
        let result = store.query_by_bbox(&q);
        assert!(
            matches!(result, Err(SessionError::InvalidRow { ref detail, .. }) if detail.contains("null")),
            "expected InvalidRow with 'null' detail, got: {result:?}"
        );
    }

    #[test]
    fn test_read_all_ids() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (10i64, 1.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (20, 2.0, None, [1.0, 0.0, 2.0, 1.0]),
            (30, 3.0, None, [2.0, 0.0, 3.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1024);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let ids = store.read_all_ids().unwrap();

        assert_eq!(ids.len(), 3);
        let raw: Vec<i64> = ids.iter().map(|id| id.get()).collect();
        assert!(raw.contains(&10));
        assert!(raw.contains(&20));
        assert!(raw.contains(&30));
    }
}
