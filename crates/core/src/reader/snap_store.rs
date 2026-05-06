//! SnapStore — lazy parquet reader for snap targets.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, BinaryArray, BooleanArray, Float32Array, Int64Array, LargeBinaryArray};
use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use hfx_core::{AtomId, BoundingBox, MainstemStatus, SnapId, SnapTarget, Weight, WkbGeometry};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use tracing::{debug, info, instrument, warn};

use super::id_index::IdIndex;
use crate::cache::ArtifactMeta;
use crate::error::SessionError;
use crate::parquet_cache::{
    ArtifactIdent, CachingReader, ParquetFooterCache, ParquetRowGroupCache,
};
use crate::reader::{BboxColIndices, extract_row_group_bbox, require_column};
use crate::runtime::RT;
use crate::telemetry::{Stage, StageGuard, record_matches, record_path, record_row_groups};

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
const ID_INDEX_ROW_GROUP_CONCURRENCY: usize = 16;
const SNAP_BBOX_ROW_GROUP_CONCURRENCY: usize = 8;

/// Row-group bounding box with metadata for pruning.
#[derive(Debug, Clone)]
struct RowGroupBbox {
    index: usize,
    bbox: BoundingBox,
    #[allow(dead_code)]
    row_count: usize,
}

#[derive(Clone)]
struct CatchmentIdRowGroupReadContext {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    file_size: u64,
    reader_metadata: ArrowReaderMetadata,
    mask: ProjectionMask,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
}

#[derive(Clone)]
struct SnapBboxRowGroupReadContext {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    file_size: u64,
    reader_metadata: ArrowReaderMetadata,
    query_bbox: BoundingBox,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
}

/// Lazy reader for snap.parquet with row-group bbox pruning.
#[derive(Debug)]
pub struct SnapStore {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    path_display: String,
    file_size: u64,
    file_etag: Option<String>,
    row_groups: Vec<RowGroupBbox>,
    groups_without_stats: Vec<usize>,
    total_rows: u64,
    all_catchment_ids: Vec<AtomId>,
    #[allow(dead_code)]
    bbox_col_indices: BboxColIndices,
    /// Optional column-chunk cache shared across all readers for this engine.
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    /// Optional footer metadata cache shared across all readers for this engine.
    footer_cache: Option<Arc<ParquetFooterCache>>,
    /// Artifact identity used as the cache key prefix (populated iff any Parquet cache is `Some`).
    cache_ident: Option<ArtifactIdent>,
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
        let (store, object_path, path_display) = local_object_artifact(path)?;
        Self::open_object(
            store,
            object_path,
            path_display,
            HeadErrorMode::LocalIo,
            None,
            None,
            None,
            None,
        )
    }

    /// Open an object-store-backed `snap.parquet` artifact.
    #[allow(dead_code)] // kept for API symmetry with CatchmentStore
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
            None,
            None,
        )
    }

    /// Open an object-store-backed `snap.parquet` with an optional column-chunk cache.
    #[allow(dead_code)]
    #[instrument(skip_all, fields(path = %path_display))]
    pub(crate) fn open_remote_with_cache(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        path_display: String,
        fabric_name: String,
        adapter_version: String,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    ) -> Result<Self, SessionError> {
        Self::open_remote_with_caches(
            store,
            path,
            path_display,
            fabric_name,
            adapter_version,
            parquet_cache,
            None,
            None,
        )
    }

    /// Open an object-store-backed `snap.parquet` with optional Parquet caches.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all, fields(path = %path_display))]
    pub(crate) fn open_remote_with_caches(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        path_display: String,
        fabric_name: String,
        adapter_version: String,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
        footer_cache: Option<Arc<ParquetFooterCache>>,
        id_index_path: Option<PathBuf>,
    ) -> Result<Self, SessionError> {
        Self::open_object(
            store,
            path,
            path_display,
            HeadErrorMode::RemoteArtifact,
            Some((fabric_name, adapter_version)),
            parquet_cache,
            footer_cache,
            id_index_path,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_object(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        path_display: String,
        head_error_mode: HeadErrorMode,
        fabric_info: Option<(String, String)>,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
        footer_cache: Option<Arc<ParquetFooterCache>>,
        id_index_path: Option<PathBuf>,
    ) -> Result<Self, SessionError> {
        let _guard = StageGuard::enter(Stage::SnapStoreOpen);
        record_path(&path_display);
        let head_meta = head_object_meta(store.as_ref(), &path, &path_display, head_error_mode)?;
        let file_size = head_meta.size;
        let last_modified = head_meta.last_modified;

        let has_parquet_cache = parquet_cache.is_some() || footer_cache.is_some();

        // Build cache identity if a cache and fabric info are both provided.
        let cache_ident = if has_parquet_cache
            && head_meta.e_tag.is_none()
            && last_modified == DateTime::<Utc>::UNIX_EPOCH
        {
            warn!(
                artifact = ARTIFACT,
                path = %path_display,
                "disabling parquet cache because object metadata lacks both ETag and last_modified"
            );
            None
        } else if has_parquet_cache {
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
                ParquetRecordBatchStreamBuilder::new(object_reader_with_cache(
                    &store,
                    &path,
                    file_size,
                    parquet_cache.clone(),
                    footer_cache.clone(),
                    cache_ident.clone(),
                ))
                .await
            })
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
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

        let all_catchment_ids = read_or_build_id_index(
            &store,
            &path,
            file_size,
            head_meta.e_tag.as_deref(),
            parquet_cache.clone(),
            footer_cache.clone(),
            cache_ident.clone(),
            id_index_path.as_deref(),
            &path_display,
        )?;

        debug!(
            row_groups = row_groups.len(),
            groups_without_stats = groups_without_stats.len(),
            total_rows,
            indexed_ids = all_catchment_ids.len(),
            "snap store opened"
        );

        Ok(Self {
            store,
            path,
            path_display,
            file_size,
            file_etag: head_meta.e_tag,
            row_groups,
            groups_without_stats,
            total_rows,
            all_catchment_ids,
            bbox_col_indices,
            parquet_cache,
            footer_cache,
            cache_ident,
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
    #[instrument(skip_all, fields(path = %self.path_display))]
    pub fn query_by_bbox(&self, query_bbox: &BoundingBox) -> Result<Vec<SnapTarget>, SessionError> {
        RT.block_on(self.query_by_bbox_async(query_bbox))
    }

    async fn query_by_bbox_async(
        &self,
        query_bbox: &BoundingBox,
    ) -> Result<Vec<SnapTarget>, SessionError> {
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
        record_row_groups(candidate_indices.len() as u64);

        if candidate_indices.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            candidate_row_groups = candidate_indices.len(),
            "reading candidate row groups"
        );

        let builder = ParquetRecordBatchStreamBuilder::new(self.object_reader())
            .await
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
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
        let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), Default::default())
            .map_err(|e| SessionError::ParquetParse {
                artifact: ARTIFACT,
                source: e,
            })?;
        let read_context = SnapBboxRowGroupReadContext {
            store: Arc::clone(&self.store),
            path: self.path.clone(),
            file_size: self.file_size,
            reader_metadata,
            query_bbox: *query_bbox,
            parquet_cache: self.parquet_cache.clone(),
            footer_cache: self.footer_cache.clone(),
            cache_ident: self.cache_ident.clone(),
        };
        let row_group_results = stream::iter(candidate_indices.into_iter().zip(rg_absolute_starts))
            .map(|(row_group, absolute_start)| {
                let read_context = read_context.clone();
                async move {
                    read_snap_bbox_row_group_async(read_context, row_group, absolute_start).await
                }
            })
            .buffered(SNAP_BBOX_ROW_GROUP_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut results = Vec::new();
        for row_group_result in row_group_results {
            results.extend(row_group_result?);
        }

        debug!(matched = results.len(), "query_by_bbox complete");
        record_matches(results.len() as u64);
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
        Ok(self.all_catchment_ids.clone())
    }

    /// Return the total number of snap target rows across all row groups.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub(crate) fn artifact_meta(&self) -> Option<ArtifactMeta> {
        ArtifactMeta::from_parts(self.file_etag.as_deref(), self.file_size)
    }

    fn object_reader(&self) -> Box<dyn AsyncFileReader> {
        object_reader_with_cache(
            &self.store,
            &self.path,
            self.file_size,
            self.parquet_cache.clone(),
            self.footer_cache.clone(),
            self.cache_ident.clone(),
        )
    }
}

fn read_all_catchment_ids_from_store(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
) -> Result<Vec<hfx_core::AtomId>, SessionError> {
    RT.block_on(read_all_catchment_ids_from_store_async(
        store,
        path,
        file_size,
        parquet_cache,
        footer_cache,
        cache_ident,
    ))
}

#[allow(clippy::too_many_arguments)]
fn read_or_build_id_index(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    file_etag: Option<&str>,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
    id_index_path: Option<&Path>,
    path_display: &str,
) -> Result<Vec<hfx_core::AtomId>, SessionError> {
    if let (Some(index_path), Some(etag)) = (id_index_path, file_etag) {
        match IdIndex::load_from_path(index_path, file_size, Some(etag)) {
            Ok(Some(index)) if index.id_row_groups.is_none() => {
                debug!(
                    path = %index_path.display(),
                    ids = index.ids.len(),
                    "loaded snap id index from cache"
                );
                return Ok(index.ids);
            }
            Ok(Some(_)) => {
                debug!(
                    path = %index_path.display(),
                    "cached snap id index has row groups; rebuilding"
                );
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    path = %index_path.display(),
                    error = %error,
                    "failed to read snap id index; rebuilding"
                );
            }
        }
    } else if id_index_path.is_some() {
        debug!(
            path = %path_display,
            "not caching snap id index because object metadata lacks ETag"
        );
    }

    let ids = {
        let _guard = StageGuard::enter(Stage::SnapIdIndex);
        record_path(path_display);
        read_all_catchment_ids_from_store(
            store,
            path,
            file_size,
            parquet_cache,
            footer_cache,
            cache_ident,
        )?
    };

    if let (Some(index_path), Some(etag)) = (id_index_path, file_etag) {
        let index = IdIndex {
            ids: ids.clone(),
            id_row_groups: None,
        };
        if let Err(error) = index.write_to_path(index_path, file_size, Some(etag)) {
            warn!(
                path = %index_path.display(),
                error = %error,
                "failed to write snap id index cache"
            );
        }
    }

    Ok(ids)
}

#[instrument(skip(store, parquet_cache, footer_cache, cache_ident), fields(path = %path))]
async fn read_all_catchment_ids_from_store_async(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
) -> Result<Vec<hfx_core::AtomId>, SessionError> {
    let started = Instant::now();
    let builder = ParquetRecordBatchStreamBuilder::new(object_reader_with_cache(
        store,
        path,
        file_size,
        parquet_cache.clone(),
        footer_cache.clone(),
        cache_ident.clone(),
    ))
    .await
    .map_err(|e| SessionError::ParquetParse {
        artifact: ARTIFACT,
        source: e,
    })?;
    let metadata = builder.metadata().clone();
    let num_row_groups = metadata.num_row_groups();
    debug!(num_row_groups, "indexing ids");

    let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), Default::default())
        .map_err(|e| SessionError::ParquetParse {
            artifact: ARTIFACT,
            source: e,
        })?;
    let parquet_schema = reader_metadata.parquet_schema();
    let col_idx = parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "catchment_id")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "missing column \"catchment_id\""))?;

    let mask = ProjectionMask::roots(parquet_schema, [col_idx]);
    let selected_row_groups: Vec<usize> = (0..num_row_groups).collect();
    let rg_absolute_starts = absolute_row_starts(&metadata, &selected_row_groups);
    let read_context = CatchmentIdRowGroupReadContext {
        store: Arc::clone(store),
        path: path.clone(),
        file_size,
        reader_metadata,
        mask,
        parquet_cache,
        footer_cache,
        cache_ident,
    };
    let row_group_results = stream::iter(selected_row_groups.into_iter().zip(rg_absolute_starts))
        .map(|(row_group, absolute_start)| {
            let read_context = read_context.clone();
            async move {
                read_catchment_id_row_group_async(read_context, row_group, absolute_start).await
            }
        })
        .buffered(ID_INDEX_ROW_GROUP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut ids = Vec::new();
    // Mirrors `catchment_store::read_all_ids_with_row_groups_async`. Keep both in sync.
    // DO NOT construct ParquetRecordBatchStreamBuilder::new inside this loop.
    for row_group_result in row_group_results {
        let row_group_ids = row_group_result?;
        ids.extend(row_group_ids);
    }
    info!(
        num_ids = ids.len(),
        num_row_groups,
        elapsed_ms = started.elapsed().as_millis(),
        "id index built"
    );
    Ok(ids)
}

async fn read_catchment_id_row_group_async(
    context: CatchmentIdRowGroupReadContext,
    row_group: usize,
    absolute_start: usize,
) -> Result<Vec<hfx_core::AtomId>, SessionError> {
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        object_reader_with_cache(
            &context.store,
            &context.path,
            context.file_size,
            context.parquet_cache,
            context.footer_cache,
            context.cache_ident,
        ),
        context.reader_metadata,
    );
    let mut stream = builder
        .with_projection(context.mask)
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
                        absolute_row + i,
                        "null catchment_id",
                    ));
                }
                let atom_id = hfx_core::AtomId::new(col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        format!("invalid catchment_id: {e}"),
                    )
                })?;
                ids.push(atom_id);
            }
            offset_in_group += batch.num_rows();
        }
    }

    Ok(ids)
}

async fn read_snap_bbox_row_group_async(
    context: SnapBboxRowGroupReadContext,
    row_group: usize,
    absolute_start: usize,
) -> Result<Vec<SnapTarget>, SessionError> {
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        object_reader_with_cache(
            &context.store,
            &context.path,
            context.file_size,
            context.parquet_cache,
            context.footer_cache,
            context.cache_ident,
        ),
        context.reader_metadata,
    );
    let mut stream = builder
        .with_row_groups(vec![row_group])
        .build()
        .map_err(|e| SessionError::ParquetParse {
            artifact: ARTIFACT,
            source: e,
        })?;

    let mut results = Vec::new();
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
            results.extend(extract_snap_targets_from_batch(
                &batch,
                absolute_row,
                &context.query_bbox,
            )?);
            offset_in_group += batch.num_rows();
        }
    }

    Ok(results)
}

fn extract_snap_targets_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    row_offset: usize,
    query_bbox: &BoundingBox,
) -> Result<Vec<SnapTarget>, SessionError> {
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
            SessionError::parquet_schema(ARTIFACT, "column 'catchment_id' missing or wrong type")
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
            SessionError::parquet_schema(ARTIFACT, "column 'is_mainstem' missing or wrong type")
        })?;
    let bbox_minx_col = batch
        .column_by_name("bbox_minx")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| {
            SessionError::parquet_schema(ARTIFACT, "column 'bbox_minx' missing or wrong type")
        })?;
    let bbox_miny_col = batch
        .column_by_name("bbox_miny")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| {
            SessionError::parquet_schema(ARTIFACT, "column 'bbox_miny' missing or wrong type")
        })?;
    let bbox_maxx_col = batch
        .column_by_name("bbox_maxx")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| {
            SessionError::parquet_schema(ARTIFACT, "column 'bbox_maxx' missing or wrong type")
        })?;
    let bbox_maxy_col = batch
        .column_by_name("bbox_maxy")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| {
            SessionError::parquet_schema(ARTIFACT, "column 'bbox_maxy' missing or wrong type")
        })?;
    let geometry_col_array = batch
        .column_by_name("geometry")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "column 'geometry' missing"))?;

    let mut results = Vec::new();
    for i in 0..batch.num_rows() {
        let absolute_row = row_offset + i;

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
            SessionError::invalid_row(ARTIFACT, absolute_row, format!("catchment_id error: {e}"))
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
            SessionError::invalid_row(ARTIFACT, absolute_row, format!("geometry error: {e}"))
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

    Ok(results)
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

fn object_reader_with_cache(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
) -> Box<dyn AsyncFileReader> {
    let raw = object_reader(store, path, file_size);
    match cache_ident {
        Some(ident) if parquet_cache.is_some() || footer_cache.is_some() => Box::new(
            CachingReader::new_with_caches(raw, parquet_cache, footer_cache, ident),
        ),
        _ => Box::new(raw),
    }
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

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::fs;
    use std::future::Future;
    use std::ops::Range;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{BinaryBuilder, BooleanBuilder, Float32Builder, Int64Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use serde_json::Value;
    use tempfile::NamedTempFile;
    use tracing_subscriber::prelude::*;

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
        write_snap_parquet_with_row_group_size(rows, rows.len().max(1))
    }

    fn write_snap_parquet_with_row_group_size(
        rows: &[SnapRow],
        row_group_size: usize,
    ) -> NamedTempFile {
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
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group_size))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        tmp
    }

    #[derive(Debug, Default)]
    struct StoreCounters {
        head_calls: AtomicUsize,
        get_range_calls: AtomicUsize,
        get_ranges_calls: AtomicUsize,
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        counters: Arc<StoreCounters>,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                counters: Arc::new(StoreCounters::default()),
            }
        }

        fn head_calls(&self) -> usize {
            self.counters.head_calls.load(Ordering::SeqCst)
        }

        fn range_read_calls(&self) -> usize {
            self.counters.get_range_calls.load(Ordering::SeqCst)
                + self.counters.get_ranges_calls.load(Ordering::SeqCst)
        }
    }

    impl fmt::Display for CountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    impl ObjectStore for CountingStore {
        fn put_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Pin<Box<dyn Future<Output = Result<PutResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.put_opts(location, payload, opts).await })
        }

        fn put_multipart_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
            opts: PutMultipartOptions,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn MultipartUpload>>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.put_multipart_opts(location, opts).await })
        }

        fn get_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
            options: GetOptions,
        ) -> Pin<Box<dyn Future<Output = Result<GetResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            if options.head {
                self.counters.head_calls.fetch_add(1, Ordering::SeqCst);
            }
            if options.range.is_some() {
                self.counters.get_range_calls.fetch_add(1, Ordering::SeqCst);
            }
            Box::pin(async move { self.inner.get_opts(location, options).await })
        }

        fn get_ranges<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
            ranges: &'life2 [Range<u64>],
        ) -> Pin<Box<dyn Future<Output = Result<Vec<Bytes>>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            self.counters
                .get_ranges_calls
                .fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { self.inner.get_ranges(location, ranges).await })
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<ObjectPath>>,
        ) -> BoxStream<'static, Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_delimiter<'life0, 'life1, 'async_trait>(
            &'life0 self,
            prefix: Option<&'life1 ObjectPath>,
        ) -> Pin<Box<dyn Future<Output = Result<ListResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.list_with_delimiter(prefix).await })
        }

        fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            from: &'life1 ObjectPath,
            to: &'life2 ObjectPath,
            options: CopyOptions,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.copy_opts(from, to, options).await })
        }
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
    fn test_read_all_catchment_ids_uses_cached_index() {
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
            SnapRow {
                id: 3,
                catchment_id: 10,
                weight: 0.2,
                is_mainstem: false,
                minx: 3.0,
                miny: 3.0,
                maxx: 4.0,
                maxy: 4.0,
                geom,
            },
        ];
        let tmp = write_snap_parquet(&rows);
        let payload = std::fs::read(tmp.path()).unwrap();
        let path = ObjectPath::from("snap.parquet");
        let base_store = Arc::new(InMemory::new());
        RT.block_on(async {
            base_store
                .put(&path, PutPayload::from(payload))
                .await
                .expect("fixture parquet should be written");
        });

        let counting_store = Arc::new(CountingStore::new(base_store));
        let object_store = Arc::clone(&counting_store) as Arc<dyn ObjectStore>;
        let store = SnapStore::open_remote(object_store, path, "memory://snap.parquet".into())
            .expect("remote snap store should open");
        let head_calls_after_open = counting_store.head_calls();
        let range_reads_after_open = counting_store.range_read_calls();
        let expected = vec![
            AtomId::new(10).unwrap(),
            AtomId::new(20).unwrap(),
            AtomId::new(10).unwrap(),
        ];

        assert_eq!(store.read_all_catchment_ids().unwrap(), expected);
        assert_eq!(store.read_all_catchment_ids().unwrap(), expected);
        assert_eq!(counting_store.head_calls(), head_calls_after_open);
        assert_eq!(counting_store.range_read_calls(), range_reads_after_open);
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
        let trace = NamedTempFile::new().unwrap();
        let (layer, guard) = crate::telemetry::jsonl::JsonlLayer::from_path(trace.path()).unwrap();
        let subscriber = tracing_subscriber::registry().with(layer);

        let results = tracing::subscriber::with_default(subscriber, || {
            let _stage = StageGuard::enter(Stage::OutletResolve);
            store.query_by_bbox(&query)
        })
        .unwrap();
        drop(guard);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id(), SnapId::new(2).unwrap());
        assert_eq!(results[0].catchment_id(), AtomId::new(20).unwrap());

        let records: Vec<Value> = fs::read_to_string(trace.path())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["stage"], "outlet_resolve");
        assert_eq!(records[0]["row_groups"], 1);
        assert_eq!(records[0]["matches"], 1);
    }

    #[test]
    fn test_query_by_bbox_preserves_row_group_order_under_concurrency() {
        let rows: Vec<_> = (1..=24)
            .map(|id| {
                let x = id as f32;
                SnapRow {
                    id,
                    catchment_id: id * 10,
                    weight: 0.5,
                    is_mainstem: id % 2 == 0,
                    minx: x,
                    miny: 0.0,
                    maxx: x + 0.5,
                    maxy: 0.5,
                    geom: minimal_wkb_linestring(x as f64, 0.0, x as f64 + 0.5, 0.5),
                }
            })
            .collect();
        let tmp = write_snap_parquet_with_row_group_size(&rows, 1);
        let store = SnapStore::open(tmp.path()).unwrap();

        let query = BoundingBox::new(5.0, 0.0, 20.75, 0.75).unwrap();
        let result_ids: Vec<_> = store
            .query_by_bbox(&query)
            .unwrap()
            .into_iter()
            .map(|target| target.id().get())
            .collect();

        let expected: Vec<_> = (5..=20).collect();
        assert_eq!(result_ids, expected);
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
        geom_b.append_value(minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0));

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
        geom_b.append_value(minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0));

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
        geom_b.append_value(minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0));

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
