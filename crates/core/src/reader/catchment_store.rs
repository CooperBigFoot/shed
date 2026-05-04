//! CatchmentStore — parquet reader with row-group bbox pruning and eager ID indexing.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use arrow::array::{Array, BinaryArray, Float32Array, Int64Array, LargeBinaryArray};
use arrow::datatypes::{DataType, Schema};
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use geo::{Geometry, MultiPolygon};
use geozero::ToGeo;
use geozero::wkb::Wkb;
use hfx_core::{AreaKm2, AtomId, BoundingBox, CatchmentAtom, WkbGeometry};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use tracing::{debug, info, instrument, warn};

use super::{BboxColIndices, extract_row_group_bbox, require_column};
use crate::algo::WkbDecodeError;
use crate::error::SessionError;
use crate::parquet_cache::{ArtifactIdent, CachingReader, ParquetRowGroupCache};
use crate::runtime::RT;

const ARTIFACT: &str = "catchments.parquet";
const ID_INDEX_ROW_GROUP_CONCURRENCY: usize = 16;

#[cfg(test)]
static GEOMETRY_DECODE_COUNTS_FOR_TEST: LazyLock<Mutex<HashMap<(String, AtomId), usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Decoded geometry-only catchment row used on the assembly/refinement hot path.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DecodedCatchmentGeometryRow {
    id: AtomId,
    geometry: MultiPolygon<f64>,
}

impl DecodedCatchmentGeometryRow {
    pub(crate) fn new(id: AtomId, geometry: MultiPolygon<f64>) -> Self {
        Self { id, geometry }
    }

    pub(crate) fn into_parts(self) -> (AtomId, MultiPolygon<f64>) {
        (self.id, self.geometry)
    }
}

/// Errors from geometry-only catchment queries.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CatchmentGeometryQueryError {
    /// Reading or validating the catchment store failed.
    #[error("{source}")]
    Read {
        /// Underlying session error.
        #[from]
        source: SessionError,
    },

    /// A requested catchment geometry failed WKB decode or had the wrong type.
    #[error("failed to decode geometry for atom {atom_id:?}: {source}")]
    Decode {
        /// Atom whose stored geometry failed decode.
        atom_id: AtomId,
        /// Underlying WKB decode error.
        source: WkbDecodeError,
    },
}

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

/// Reader for catchments.parquet with row-group bbox pruning.
///
/// Holds the file path, pre-extracted row-group metadata, and an eager
/// `AtomId -> row_group` index built at open time so repeated ID-based queries
/// can project only the row groups they need. Query methods still re-open the
/// Parquet file on demand and do not hold file handles open between calls.
#[derive(Debug)]
pub struct CatchmentStore {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    path_display: String,
    file_size: u64,
    row_groups: Vec<RowGroupBbox>,
    /// Row groups that lacked bbox statistics (included conservatively in all queries).
    groups_without_stats: Vec<usize>,
    total_rows: u64,
    all_ids: Vec<AtomId>,
    id_row_groups: HashMap<AtomId, usize>,
    #[allow(dead_code)]
    bbox_col_indices: BboxColIndices,
    /// Optional column-chunk cache shared across all readers for this engine.
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    /// Artifact identity used as the cache key prefix (populated iff `parquet_cache` is `Some`).
    cache_ident: Option<ArtifactIdent>,
}

impl CatchmentStore {
    /// Open `catchments.parquet` at `path`, validate the schema, and
    /// pre-extract row-group bounding-box statistics.
    ///
    /// Reads Parquet metadata plus the `id` column once up front so later
    /// ID-based queries and graph integrity checks can reuse the cached index.
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
        let (store, object_path, path_display) = local_object_artifact(path)?;
        Self::open_object(
            store,
            object_path,
            path_display,
            HeadErrorMode::LocalIo,
            None,
            None,
        )
    }

    /// Open an object-store-backed `catchments.parquet` artifact.
    #[allow(dead_code)] // used in #[cfg(test)] catchment_store_perf_tests
    #[instrument(skip_all, fields(path = %path_display))]
    pub(crate) fn open_remote(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        path_display: String,
    ) -> Result<Self, SessionError> {
        Self::open_object(
            store,
            path,
            path_display,
            HeadErrorMode::RemoteArtifact,
            None,
            None,
        )
    }

    /// Open an object-store-backed `catchments.parquet` with an optional column-chunk cache.
    ///
    /// `fabric_name` and `adapter_version` are used to construct the [`ArtifactIdent`] cache
    /// key prefix alongside the file size and ETag captured from the HEAD response.
    #[instrument(skip_all, fields(path = %path_display))]
    pub(crate) fn open_remote_with_cache(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        path_display: String,
        fabric_name: String,
        adapter_version: String,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    ) -> Result<Self, SessionError> {
        Self::open_object(
            store,
            path,
            path_display,
            HeadErrorMode::RemoteArtifact,
            Some((fabric_name, adapter_version)),
            parquet_cache,
        )
    }

    fn open_object(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        path_display: String,
        head_error_mode: HeadErrorMode,
        fabric_info: Option<(String, String)>,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    ) -> Result<Self, SessionError> {
        let head_meta = head_object_meta(store.as_ref(), &path, &path_display, head_error_mode)?;
        let file_size = head_meta.size;
        let last_modified = head_meta.last_modified;

        // Build the cache identity if a cache is provided and we have fabric info.
        let cache_ident = if parquet_cache.is_some()
            && head_meta.e_tag.is_none()
            && last_modified == DateTime::<Utc>::UNIX_EPOCH
        {
            warn!(
                artifact = ARTIFACT,
                path = %path_display,
                "disabling parquet cache because object metadata lacks both ETag and last_modified"
            );
            None
        } else if parquet_cache.is_some() {
            fabric_info.map(|(fabric_name, adapter_version)| ArtifactIdent {
                fabric_name,
                adapter_version,
                artifact: ARTIFACT,
                file_size,
                etag: head_meta.e_tag.clone(),
                last_modified,
            })
        } else {
            None
        };

        let builder = RT
            .block_on(async {
                ParquetRecordBatchStreamBuilder::new(object_reader(&store, &path, file_size)).await
            })
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;

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
                    SessionError::parquet_schema(ARTIFACT, format!("missing column {name}"))
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

        let (all_ids, id_row_groups) = read_all_ids_with_row_groups(&store, &path, file_size)?;

        debug!(
            total_rows,
            indexed_ids = all_ids.len(),
            row_groups_with_stats = row_groups.len(),
            row_groups_without_stats = groups_without_stats.len(),
            "opened catchment store"
        );

        Ok(Self {
            store,
            path,
            path_display,
            file_size,
            row_groups,
            groups_without_stats,
            total_rows,
            all_ids,
            id_row_groups,
            bbox_col_indices: bbox_indices,
            parquet_cache,
            cache_ident,
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
    #[instrument(skip_all, fields(path = %self.path_display))]
    pub fn query_by_bbox(
        &self,
        query_bbox: &BoundingBox,
    ) -> Result<Vec<CatchmentAtom>, SessionError> {
        RT.block_on(self.query_by_bbox_async(query_bbox))
    }

    async fn query_by_bbox_async(
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

        let builder = ParquetRecordBatchStreamBuilder::new(self.object_reader())
            .await
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;

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

        let mut stream = builder
            .with_row_groups(matching.clone())
            .with_batch_size(8192)
            .build()
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;

        let mut results = Vec::new();
        let mut sel_idx = 0usize;
        let mut offset_in_group = 0usize;

        while let Some(reader) =
            stream
                .next_row_group()
                .await
                .map_err(|e| SessionError::RowGroupReadError {
                    artifact: ARTIFACT,
                    row_group: matching[sel_idx],
                    source: e,
                })?
        {
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
            }
            offset_in_group = 0;
            sel_idx += 1;
        }

        Ok(results)
    }

    /// Return the [`CatchmentAtom`]s whose IDs appear in `ids`.
    ///
    /// # Errors
    ///
    /// | Condition | Variant |
    /// |---|---|
    /// | File not found / unreadable | [`SessionError::Io`] |
    /// | Parquet decode error | [`SessionError::ParquetParse`] |
    /// | Row fails domain validation | [`SessionError::InvalidRow`] |
    #[instrument(skip_all, fields(path = %self.path_display))]
    pub fn query_by_ids(&self, ids: &[AtomId]) -> Result<Vec<CatchmentAtom>, SessionError> {
        RT.block_on(self.query_by_ids_async(ids))
    }

    async fn query_by_ids_async(&self, ids: &[AtomId]) -> Result<Vec<CatchmentAtom>, SessionError> {
        let id_set: HashSet<AtomId> = ids.iter().copied().collect();
        let selected_row_groups = self.selected_row_groups_for_ids(ids);
        if id_set.is_empty() || selected_row_groups.is_empty() {
            return Ok(Vec::new());
        }

        let builder = ParquetRecordBatchStreamBuilder::new(self.object_reader())
            .await
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;
        let metadata = builder.metadata().clone();
        let rg_absolute_starts = absolute_row_starts(&metadata, &selected_row_groups);
        let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), Default::default())
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;
        let parquet_schema = reader_metadata.parquet_schema();
        let projection =
            ProjectionMask::roots(parquet_schema, full_projection_indices(parquet_schema)?);

        let mut results = Vec::new();
        for (sel_idx, &row_group) in selected_row_groups.iter().enumerate() {
            let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                self.object_reader(),
                reader_metadata.clone(),
            );
            let mut stream = builder
                .with_projection(projection.clone())
                .with_row_groups(vec![row_group])
                .with_batch_size(8192)
                .build()
                .map_err(|e| SessionError::ParquetParse {
                    artifact: ARTIFACT,
                    source: e,
                })?;

            let mut offset_in_group = 0usize;
            while let Some(reader) =
                stream
                    .next_row_group()
                    .await
                    .map_err(|e| SessionError::RowGroupReadError {
                        artifact: ARTIFACT,
                        row_group,
                        source: e,
                    })?
            {
                for batch_result in reader {
                    let batch = batch_result.map_err(|e| SessionError::RowGroupReadError {
                        artifact: ARTIFACT,
                        row_group,
                        source: parquet::errors::ParquetError::ArrowError(e.to_string()),
                    })?;

                    let absolute_row = rg_absolute_starts[sel_idx] + offset_in_group;
                    let rows = extract_atoms_from_batch(&batch, absolute_row, ARTIFACT)?;

                    for atom in rows {
                        if id_set.contains(&atom.id()) {
                            results.push(atom);
                        }
                    }

                    offset_in_group += batch.num_rows();
                }
            }
        }

        Ok(results)
    }

    /// Return geometry-only rows for the requested IDs.
    ///
    /// This is the lean hot path for watershed assembly and terminal
    /// refinement, projecting only `id` and `geometry`.
    #[instrument(skip_all, fields(path = %self.path_display))]
    pub(crate) fn query_geometries_by_ids(
        &self,
        ids: &[AtomId],
    ) -> Result<Vec<DecodedCatchmentGeometryRow>, CatchmentGeometryQueryError> {
        RT.block_on(self.query_geometries_by_ids_async(ids))
    }

    async fn query_geometries_by_ids_async(
        &self,
        ids: &[AtomId],
    ) -> Result<Vec<DecodedCatchmentGeometryRow>, CatchmentGeometryQueryError> {
        let id_set: HashSet<AtomId> = ids.iter().copied().collect();
        let selected_row_groups = self.selected_row_groups_for_ids(ids);
        if id_set.is_empty() || selected_row_groups.is_empty() {
            return Ok(Vec::new());
        }

        let builder = ParquetRecordBatchStreamBuilder::new(self.object_reader())
            .await
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;
        let metadata = builder.metadata().clone();
        let rg_absolute_starts = absolute_row_starts(&metadata, &selected_row_groups);
        let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), Default::default())
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;
        let parquet_schema = reader_metadata.parquet_schema();
        let projection =
            ProjectionMask::roots(parquet_schema, geometry_projection_indices(parquet_schema)?);

        let mut results = Vec::new();
        for (sel_idx, &row_group) in selected_row_groups.iter().enumerate() {
            let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                self.object_reader(),
                reader_metadata.clone(),
            );
            let mut stream = builder
                .with_projection(projection.clone())
                .with_row_groups(vec![row_group])
                .with_batch_size(8192)
                .build()
                .map_err(|e| SessionError::ParquetParse {
                    artifact: ARTIFACT,
                    source: e,
                })?;

            let mut offset_in_group = 0usize;
            while let Some(reader) =
                stream
                    .next_row_group()
                    .await
                    .map_err(|e| SessionError::RowGroupReadError {
                        artifact: ARTIFACT,
                        row_group,
                        source: e,
                    })?
            {
                for batch_result in reader {
                    let batch = batch_result.map_err(|e| SessionError::RowGroupReadError {
                        artifact: ARTIFACT,
                        row_group,
                        source: parquet::errors::ParquetError::ArrowError(e.to_string()),
                    })?;

                    let absolute_row = rg_absolute_starts[sel_idx] + offset_in_group;
                    let rows = extract_decoded_geometries_from_batch(
                        &batch,
                        absolute_row,
                        ARTIFACT,
                        &id_set,
                    )?;
                    #[cfg(test)]
                    for row in &rows {
                        record_geometry_decode_for_test(&self.path_display, row.id);
                    }

                    results.extend(rows);

                    offset_in_group += batch.num_rows();
                }
            }
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
        Ok(self.all_ids.clone())
    }

    /// Return whether an atom ID is present in the cached catchment index.
    pub(crate) fn contains_id(&self, id: AtomId) -> bool {
        self.id_row_groups.contains_key(&id)
    }

    /// Return the successful geometry decode count for `id` in this store.
    #[cfg(test)]
    pub(crate) fn geometry_decode_count_for_test(&self, id: AtomId) -> usize {
        let counts = GEOMETRY_DECODE_COUNTS_FOR_TEST
            .lock()
            .expect("geometry decode count mutex poisoned");
        counts
            .get(&(self.path_display.clone(), id))
            .copied()
            .unwrap_or_default()
    }

    /// Return the total number of rows in the Parquet file.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    fn selected_row_groups_for_ids(&self, ids: &[AtomId]) -> Vec<usize> {
        let mut selected: Vec<usize> = ids
            .iter()
            .filter_map(|id| self.id_row_groups.get(id).copied())
            .collect();
        selected.sort_unstable();
        selected.dedup();
        selected
    }

    fn object_reader(&self) -> Box<dyn AsyncFileReader> {
        let raw = object_reader(&self.store, &self.path, self.file_size);
        match (&self.parquet_cache, &self.cache_ident) {
            (Some(cache), Some(ident)) => {
                Box::new(CachingReader::new(raw, cache.clone(), ident.clone()))
            }
            _ => Box::new(raw),
        }
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
    let up_area_col = col_as::<Float32Array>(batch, &schema, "up_area_km2", artifact)?;
    let minx_col = col_as::<Float32Array>(batch, &schema, "bbox_minx", artifact)?;
    let miny_col = col_as::<Float32Array>(batch, &schema, "bbox_miny", artifact)?;
    let maxx_col = col_as::<Float32Array>(batch, &schema, "bbox_maxx", artifact)?;
    let maxy_col = col_as::<Float32Array>(batch, &schema, "bbox_maxy", artifact)?;

    // geometry may be Binary or LargeBinary
    let geom_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "geometry")
        .ok_or_else(|| SessionError::parquet_schema(artifact, "missing column \"geometry\""))?;
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
        let atom_id = AtomId::new(raw_id)
            .map_err(|e| SessionError::invalid_row(artifact, global_i, format!("id: {e}")))?;

        if area_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"area_km2\"",
            ));
        }
        let area = AreaKm2::new(area_col.value(i))
            .map_err(|e| SessionError::invalid_row(artifact, global_i, format!("area_km2: {e}")))?;

        let upstream_area = if up_area_col.is_null(i) {
            None
        } else {
            Some(AreaKm2::new(up_area_col.value(i)).map_err(|e| {
                SessionError::invalid_row(artifact, global_i, format!("up_area_km2: {e}"))
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
        let bbox = BoundingBox::new(
            minx_col.value(i),
            miny_col.value(i),
            maxx_col.value(i),
            maxy_col.value(i),
        )
        .map_err(|e| SessionError::invalid_row(artifact, global_i, format!("bbox: {e}")))?;

        // Check geometry nullability before dispatching on array type.
        let geom_is_null = if let Some(arr) = geom_array.as_any().downcast_ref::<BinaryArray>() {
            arr.is_null(i)
        } else if let Some(arr) = geom_array.as_any().downcast_ref::<LargeBinaryArray>() {
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

        let geom_bytes: Vec<u8> =
            if let Some(arr) = geom_array.as_any().downcast_ref::<BinaryArray>() {
                arr.value(i).to_vec()
            } else if let Some(arr) = geom_array.as_any().downcast_ref::<LargeBinaryArray>() {
                arr.value(i).to_vec()
            } else {
                return Err(SessionError::parquet_schema(
                    artifact,
                    "geometry column is not Binary or LargeBinary",
                ));
            };

        let geometry = WkbGeometry::new(geom_bytes)
            .map_err(|e| SessionError::invalid_row(artifact, global_i, format!("geometry: {e}")))?;

        atoms.push(CatchmentAtom::new(
            atom_id,
            area,
            upstream_area,
            bbox,
            geometry,
        ));
    }

    Ok(atoms)
}

fn extract_decoded_geometries_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    row_offset: usize,
    artifact: &'static str,
    requested_ids: &HashSet<AtomId>,
) -> Result<Vec<DecodedCatchmentGeometryRow>, CatchmentGeometryQueryError> {
    let schema = batch.schema();
    let id_col = col_as::<Int64Array>(batch, &schema, "id", artifact)?;

    let geom_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "geometry")
        .ok_or_else(|| SessionError::parquet_schema(artifact, "missing column \"geometry\""))?;
    let geom_array = batch.column(geom_idx);

    let n = batch.num_rows();
    let mut rows = Vec::with_capacity(n);

    for i in 0..n {
        let global_i = row_offset + i;
        if id_col.is_null(i) {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"id\"",
            )
            .into());
        }
        let atom_id = AtomId::new(id_col.value(i))
            .map_err(|e| SessionError::invalid_row(artifact, global_i, format!("id: {e}")))?;
        if !requested_ids.contains(&atom_id) {
            continue;
        }

        let geom_is_null = if let Some(arr) = geom_array.as_any().downcast_ref::<BinaryArray>() {
            arr.is_null(i)
        } else if let Some(arr) = geom_array.as_any().downcast_ref::<LargeBinaryArray>() {
            arr.is_null(i)
        } else {
            return Err(SessionError::parquet_schema(
                artifact,
                "geometry column is not Binary or LargeBinary",
            )
            .into());
        };
        if geom_is_null {
            return Err(SessionError::invalid_row(
                artifact,
                global_i,
                "null value in non-nullable column \"geometry\"",
            )
            .into());
        }

        let geometry = if let Some(arr) = geom_array.as_any().downcast_ref::<BinaryArray>() {
            decode_wkb_multi_polygon_bytes(arr.value(i))
        } else if let Some(arr) = geom_array.as_any().downcast_ref::<LargeBinaryArray>() {
            decode_wkb_multi_polygon_bytes(arr.value(i))
        } else {
            return Err(SessionError::parquet_schema(
                artifact,
                "geometry column is not Binary or LargeBinary",
            )
            .into());
        }
        .map_err(|source| CatchmentGeometryQueryError::Decode { atom_id, source })?;
        rows.push(DecodedCatchmentGeometryRow::new(atom_id, geometry));
    }

    Ok(rows)
}

fn decode_wkb_multi_polygon_bytes(wkb: &[u8]) -> Result<MultiPolygon<f64>, WkbDecodeError> {
    let geom = Wkb(wkb)
        .to_geo()
        .map_err(|e| WkbDecodeError::DecodeFailed {
            reason: e.to_string(),
        })?;
    match geom {
        Geometry::Polygon(p) => Ok(MultiPolygon::new(vec![p])),
        Geometry::MultiPolygon(mp) => Ok(mp),
        other => Err(WkbDecodeError::UnexpectedType {
            expected: "Polygon or MultiPolygon",
            actual: geometry_type_name(&other).to_owned(),
        }),
    }
}

#[cfg(test)]
fn record_geometry_decode_for_test(path: &str, atom_id: AtomId) {
    let mut counts = GEOMETRY_DECODE_COUNTS_FOR_TEST
        .lock()
        .expect("geometry decode count mutex poisoned");
    *counts.entry((path.to_owned(), atom_id)).or_default() += 1;
}

#[cfg(test)]
pub(crate) fn reset_geometry_decode_counts_for_test() {
    GEOMETRY_DECODE_COUNTS_FOR_TEST
        .lock()
        .expect("geometry decode count mutex poisoned")
        .clear();
}

fn geometry_type_name(geom: &Geometry<f64>) -> &'static str {
    match geom {
        Geometry::Point(_) => "Point",
        Geometry::Line(_) => "Line",
        Geometry::LineString(_) => "LineString",
        Geometry::Polygon(_) => "Polygon",
        Geometry::MultiPoint(_) => "MultiPoint",
        Geometry::MultiLineString(_) => "MultiLineString",
        Geometry::MultiPolygon(_) => "MultiPolygon",
        Geometry::GeometryCollection(_) => "GeometryCollection",
        Geometry::Rect(_) => "Rect",
        Geometry::Triangle(_) => "Triangle",
    }
}

#[instrument(skip(store))]
async fn read_all_ids_with_row_groups_async(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
) -> Result<(Vec<AtomId>, HashMap<AtomId, usize>), SessionError> {
    let started = Instant::now();
    let builder = ParquetRecordBatchStreamBuilder::new(object_reader(store, path, file_size))
        .await
        .map_err(|e| SessionError::ParquetParse {
            artifact: ARTIFACT,
            source: e,
        })?;
    let metadata = builder.metadata().clone();
    let num_row_groups = metadata.num_row_groups();
    debug!(num_row_groups, "indexing ids");
    if num_row_groups == 0 {
        return Ok((Vec::new(), HashMap::new()));
    }

    let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), Default::default())
        .map_err(|e| SessionError::ParquetParse {
            artifact: ARTIFACT,
            source: e,
        })?;
    let parquet_schema = reader_metadata.parquet_schema();
    let id_projection = ProjectionMask::roots(parquet_schema, [id_column_index(parquet_schema)?]);
    let selected_row_groups: Vec<usize> = (0..num_row_groups).collect();
    let rg_absolute_starts = absolute_row_starts(&metadata, &selected_row_groups);

    let row_group_results = stream::iter(selected_row_groups.into_iter().zip(rg_absolute_starts))
        .map(|(row_group, absolute_start)| {
            let store = Arc::clone(store);
            let path = path.clone();
            let reader_metadata = reader_metadata.clone();
            let id_projection = id_projection.clone();
            async move {
                read_id_row_group_async(
                    &store,
                    &path,
                    file_size,
                    reader_metadata,
                    id_projection,
                    row_group,
                    absolute_start,
                )
                .await
            }
        })
        .buffered(ID_INDEX_ROW_GROUP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut ids = Vec::new();
    let mut id_row_groups = HashMap::new();
    // Mirrors `snap_store::read_all_catchment_ids_async`. Keep both in sync.
    // DO NOT construct ParquetRecordBatchStreamBuilder::new inside this loop -- see catchment_store_perf_tests.rs.
    for row_group_result in row_group_results {
        let (row_group, row_group_ids) = row_group_result?;
        for atom_id in row_group_ids {
            ids.push(atom_id);
            if let Some(previous_row_group) = id_row_groups.insert(atom_id, row_group) {
                return Err(SessionError::integrity(format!(
                    "duplicate catchment id {} found in row groups {} and {}",
                    atom_id.get(),
                    previous_row_group,
                    row_group,
                )));
            }
        }
    }

    info!(
        num_ids = ids.len(),
        num_row_groups,
        elapsed_ms = started.elapsed().as_millis(),
        "id index built"
    );
    Ok((ids, id_row_groups))
}

async fn read_id_row_group_async(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    reader_metadata: ArrowReaderMetadata,
    id_projection: ProjectionMask,
    row_group: usize,
    absolute_start: usize,
) -> Result<(usize, Vec<AtomId>), SessionError> {
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        object_reader(store, path, file_size),
        reader_metadata,
    );
    let mut stream = builder
        .with_projection(id_projection)
        .with_row_groups(vec![row_group])
        .with_batch_size(8192)
        .build()
        .map_err(|e| SessionError::ParquetParse {
            artifact: ARTIFACT,
            source: e,
        })?;

    let mut ids = Vec::new();
    let mut offset_in_group = 0usize;
    while let Some(reader) =
        stream
            .next_row_group()
            .await
            .map_err(|e| SessionError::RowGroupReadError {
                artifact: ARTIFACT,
                row_group,
                source: e,
            })?
    {
        for batch_result in reader {
            let batch = batch_result.map_err(|e| SessionError::RowGroupReadError {
                artifact: ARTIFACT,
                row_group,
                source: parquet::errors::ParquetError::ArrowError(e.to_string()),
            })?;
            let absolute_row = absolute_start + offset_in_group;
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "id column is not Int64"))?;

            for i in 0..batch.num_rows() {
                if id_col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        "null id",
                    ));
                }
                let atom_id = AtomId::new(id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        format!("invalid atom id: {e}"),
                    )
                })?;
                ids.push(atom_id);
            }

            offset_in_group += batch.num_rows();
        }
    }

    Ok((row_group, ids))
}

fn read_all_ids_with_row_groups(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
) -> Result<(Vec<AtomId>, HashMap<AtomId, usize>), SessionError> {
    RT.block_on(read_all_ids_with_row_groups_async(store, path, file_size))
}

#[derive(Debug, Clone, Copy)]
enum HeadErrorMode {
    LocalIo,
    RemoteArtifact,
}

fn object_reader(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
) -> ParquetObjectReader {
    ParquetObjectReader::new(store.clone(), path.clone())
        .with_file_size(file_size)
        .with_footer_size_hint(1 << 20)
}

fn head_object_meta(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    path_display: &str,
    error_mode: HeadErrorMode,
) -> Result<object_store::ObjectMeta, SessionError> {
    RT.block_on(async {
        store.head(path).await.map_err(|source| match error_mode {
            HeadErrorMode::LocalIo => object_store_error_as_io(source),
            HeadErrorMode::RemoteArtifact => {
                SessionError::remote_artifact_read(ARTIFACT, path_display, source)
            }
        })
    })
}

fn local_object_artifact(
    path: &Path,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath, String), SessionError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path.file_name().ok_or_else(|| {
        SessionError::io(
            ARTIFACT,
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path does not name a parquet file",
            ),
        )
    })?;
    let file_name = file_name.to_str().ok_or_else(|| {
        SessionError::io(
            ARTIFACT,
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path is not valid UTF-8"),
        )
    })?;
    let store = LocalFileSystem::new_with_prefix(parent).map_err(object_store_error_as_io)?;

    Ok((
        Arc::new(store),
        ObjectPath::from(file_name),
        path.display().to_string(),
    ))
}

fn object_store_error_as_io(source: object_store::Error) -> SessionError {
    SessionError::io(ARTIFACT, std::io::Error::other(source.to_string()))
}

fn absolute_row_starts(
    metadata: &parquet::file::metadata::ParquetMetaData,
    selected_row_groups: &[usize],
) -> Vec<usize> {
    let mut starts = Vec::with_capacity(selected_row_groups.len());
    let mut cumulative = 0usize;
    for rg_idx in 0..metadata.num_row_groups() {
        if selected_row_groups.contains(&rg_idx) {
            starts.push(cumulative);
        }
        cumulative += metadata.row_group(rg_idx).num_rows() as usize;
    }
    starts
}

fn id_column_index(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Result<usize, SessionError> {
    parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "id")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "missing column \"id\""))
}

fn full_projection_indices(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Result<[usize; 8], SessionError> {
    Ok([
        id_column_index(parquet_schema)?,
        named_column_index(parquet_schema, "area_km2")?,
        named_column_index(parquet_schema, "up_area_km2")?,
        named_column_index(parquet_schema, "bbox_minx")?,
        named_column_index(parquet_schema, "bbox_miny")?,
        named_column_index(parquet_schema, "bbox_maxx")?,
        named_column_index(parquet_schema, "bbox_maxy")?,
        named_column_index(parquet_schema, "geometry")?,
    ])
}

fn geometry_projection_indices(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Result<[usize; 2], SessionError> {
    Ok([
        id_column_index(parquet_schema)?,
        named_column_index(parquet_schema, "geometry")?,
    ])
}

fn named_column_index(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    name: &str,
) -> Result<usize, SessionError> {
    parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == name)
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, format!("missing column \"{name}\"")))
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

    use arrow::array::{BinaryBuilder, Float32Builder, Int64Builder, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use geo::Area;
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
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

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
            (
                1i64,
                10.0f32,
                Some(100.0f32),
                [0.0f32, 0.0f32, 1.0f32, 1.0f32],
            ),
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
    fn test_query_geometries_by_ids() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 10.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, None, [2.0, 0.0, 3.0, 1.0]),
            (3, 30.0, None, [4.0, 0.0, 5.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 2);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let ids = [AtomId::new(1).unwrap(), AtomId::new(3).unwrap()];
        let results = store.query_geometries_by_ids(&ids).unwrap();

        assert_eq!(results.len(), 2);
        let expected_area = HashMap::from([(1, 1.0), (3, 1.0)]);
        for row in results {
            let (atom_id, geometry) = row.into_parts();
            let area = expected_area
                .get(&atom_id.get())
                .expect("queried IDs should be present");
            assert!((geometry.unsigned_area() - *area).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_query_geometries_by_ids_ignores_unknown_ids() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 10.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, None, [2.0, 0.0, 3.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let ids = [AtomId::new(1).unwrap(), AtomId::new(999).unwrap()];
        let results = store.query_geometries_by_ids(&ids).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results.into_iter().next().unwrap().into_parts().0,
            AtomId::new(1).unwrap()
        );
    }

    #[test]
    fn test_read_all_ids_uses_cached_index() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 10.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, None, [2.0, 0.0, 3.0, 1.0]),
            (3, 30.0, None, [4.0, 0.0, 5.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 2);

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let ids = store.read_all_ids().unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(
            ids,
            vec![
                AtomId::new(1).unwrap(),
                AtomId::new(2).unwrap(),
                AtomId::new(3).unwrap()
            ]
        );
    }

    #[test]
    fn test_nullable_up_area() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (
                1i64,
                5.0f32,
                Some(50.0f32),
                [0.0f32, 0.0f32, 1.0f32, 1.0f32],
            ),
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "not_id",
            DataType::Int64,
            false,
        )]));
        let props = WriterProperties::builder().build();
        let file = std::fs::File::create(tmp.path()).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Builder::new().finish())]).unwrap();
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
    fn write_fixture_with_null(path: &std::path::Path, null_col: &str, null_row: usize) {
        // Build a schema with the target column overridden to nullable=true.
        let fields: Vec<Field> = vec![
            Field::new("id", DataType::Int64, null_col == "id"),
            Field::new("area_km2", DataType::Float32, null_col == "area_km2"),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, null_col == "bbox_minx"),
            Field::new("bbox_miny", DataType::Float32, null_col == "bbox_miny"),
            Field::new("bbox_maxx", DataType::Float32, null_col == "bbox_maxx"),
            Field::new("bbox_maxy", DataType::Float32, null_col == "bbox_maxy"),
            Field::new("geometry", DataType::Binary, null_col == "geometry"),
        ];
        let schema = Arc::new(Schema::new(fields));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

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
                let wkb = minimal_wkb_polygon(row as f64, 0.0, row as f64 + 1.0, 1.0);
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

        let result = CatchmentStore::open(tmp.path());
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

    #[test]
    fn test_open_rejects_duplicate_ids_across_row_groups() {
        let tmp = NamedTempFile::new().unwrap();
        let atoms = [
            (1i64, 10.0f32, None, [0.0f32, 0.0f32, 1.0f32, 1.0f32]),
            (2, 20.0, None, [2.0, 0.0, 3.0, 1.0]),
            (1, 30.0, None, [4.0, 0.0, 5.0, 1.0]),
        ];
        write_fixture(tmp.path(), &atoms, 1);

        let result = CatchmentStore::open(tmp.path());
        assert!(
            matches!(result, Err(SessionError::IntegrityViolation { ref detail }) if detail.contains("duplicate catchment id 1")),
            "expected duplicate-id integrity error, got: {result:?}"
        );
    }
}
