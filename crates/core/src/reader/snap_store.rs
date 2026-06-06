//! SnapStore — lazy parquet reader for snap targets.

use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arrow::array::{Array, BinaryArray, Float32Array, Int64Array, LargeBinaryArray, StringArray};
use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use geo::{BoundingRect, Geometry};
use hfx_core::{BoundingBox, SnapId, SnapTarget, StemRole, UnitId, Weight, WkbGeometry};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use tracing::{Instrument, debug, info, instrument, warn};

use super::id_index::IdIndex;
use crate::algo::wkb::decode_wkb;
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

#[cfg(test)]
static SNAP_GEOMETRY_DECODE_ROWS_FOR_TEST: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static SNAP_MEMBERSHIP_ROWS_FOR_TEST: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn snap_geometry_decode_rows_for_test() -> usize {
    SNAP_GEOMETRY_DECODE_ROWS_FOR_TEST.load(Ordering::SeqCst)
}

#[cfg(test)]
pub(crate) fn reset_snap_geometry_decode_rows_for_test() {
    SNAP_GEOMETRY_DECODE_ROWS_FOR_TEST.store(0, Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn snap_membership_rows_for_test() -> usize {
    SNAP_MEMBERSHIP_ROWS_FOR_TEST.load(Ordering::SeqCst)
}

#[cfg(test)]
pub(crate) fn reset_snap_membership_rows_for_test() {
    SNAP_MEMBERSHIP_ROWS_FOR_TEST.store(0, Ordering::SeqCst);
}

/// Row-group bounding box with metadata for pruning.
#[derive(Debug, Clone)]
struct RowGroupBbox {
    index: usize,
    bbox: BoundingBox,
    #[allow(dead_code)]
    row_count: usize,
}

#[derive(Clone)]
struct UnitIdRowGroupReadContext {
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
struct SnapMembershipRowGroupReadContext {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapUnitRef {
    pub(crate) snap_id: SnapId,
    pub(crate) unit_id: UnitId,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapOpenMode {
    LazyMetadata,
    ColdMembershipValidation,
}

#[derive(Debug)]
enum SnapRefsState {
    #[cfg_attr(not(test), allow(dead_code))]
    NotLoaded,
    Loaded(Vec<SnapUnitRef>),
}

#[derive(Debug, Default)]
struct SnapValidationReadStats {
    refs: Vec<SnapUnitRef>,
    batches_read: usize,
    rows_validated: usize,
    stem_role_values_parsed: usize,
    geometry_rows_validated: usize,
}

impl SnapValidationReadStats {
    fn extend(&mut self, row_group_stats: SnapValidationReadStats) {
        self.refs.extend(row_group_stats.refs);
        self.batches_read += row_group_stats.batches_read;
        self.rows_validated += row_group_stats.rows_validated;
        self.stem_role_values_parsed += row_group_stats.stem_role_values_parsed;
        self.geometry_rows_validated += row_group_stats.geometry_rows_validated;
    }
}

#[derive(Debug, Default)]
struct SnapMembershipReadStats {
    refs: Vec<SnapUnitRef>,
    batches_read: usize,
    membership_rows: usize,
}

impl SnapMembershipReadStats {
    fn extend(&mut self, row_group_stats: SnapMembershipReadStats) {
        self.refs.extend(row_group_stats.refs);
        self.batches_read += row_group_stats.batches_read;
        self.membership_rows += row_group_stats.membership_rows;
    }
}

/// Lazy reader for snap.parquet with row-group bbox pruning.
#[derive(Debug)]
pub struct SnapStore {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    file_size: u64,
    file_etag: Option<String>,
    row_groups: Vec<RowGroupBbox>,
    groups_without_stats: Vec<usize>,
    total_rows: u64,
    #[allow(dead_code)]
    bbox_col_indices: Option<BboxColIndices>,
    snap_refs: SnapRefsState,
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
            None,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_with_mode(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        path_display: String,
        head_error_mode: HeadErrorMode,
        fabric_info: Option<(String, String)>,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
        footer_cache: Option<Arc<ParquetFooterCache>>,
        mode: SnapOpenMode,
    ) -> Result<Self, SessionError> {
        Self::open_object(
            store,
            path,
            path_display,
            head_error_mode,
            fabric_info,
            parquet_cache,
            footer_cache,
            None,
            Some(mode),
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
        mode: Option<SnapOpenMode>,
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
        require_column(schema, "unit_id", &DataType::Int64, ARTIFACT)?;
        require_column(schema, "weight", &DataType::Float32, ARTIFACT)?;
        if let Ok(field) = schema.field_with_name("stem_role")
            && field.data_type() != &DataType::Utf8
        {
            return Err(SessionError::parquet_schema(
                ARTIFACT,
                "column \"stem_role\" must be Utf8 when present",
            ));
        }
        let bbox_col_indices = optional_bbox_col_indices(schema)?;
        require_column(schema, "geometry", &DataType::Binary, ARTIFACT)?;

        let mut row_groups = Vec::new();
        let mut groups_without_stats = Vec::new();
        let mut total_rows: u64 = 0;

        for (i, rg) in metadata.row_groups().iter().enumerate() {
            let row_count = rg.num_rows() as usize;
            total_rows += rg.num_rows() as u64;

            match bbox_col_indices
                .as_ref()
                .and_then(|indices| extract_row_group_bbox(rg, indices))
            {
                Some(bbox) => {
                    row_groups.push(RowGroupBbox {
                        index: i,
                        bbox,
                        row_count,
                    });
                }
                None => groups_without_stats.push(i),
            }
        }

        let snap_refs = match mode {
            None => SnapRefsState::Loaded(read_or_build_id_index(
                &store,
                &path,
                file_size,
                head_meta.e_tag.as_deref(),
                parquet_cache.clone(),
                footer_cache.clone(),
                cache_ident.clone(),
                id_index_path.as_deref(),
                &path_display,
            )?),
            Some(SnapOpenMode::LazyMetadata) => SnapRefsState::NotLoaded,
            Some(SnapOpenMode::ColdMembershipValidation) => {
                let refs = if id_index_path.is_some() {
                    read_or_build_id_index(
                        &store,
                        &path,
                        file_size,
                        head_meta.e_tag.as_deref(),
                        parquet_cache.clone(),
                        footer_cache.clone(),
                        cache_ident.clone(),
                        id_index_path.as_deref(),
                        &path_display,
                    )?
                } else {
                    let _guard = StageGuard::enter(Stage::SnapIdIndex);
                    record_path(&path_display);
                    read_all_snap_membership_refs_from_store(
                        &store,
                        &path,
                        file_size,
                        parquet_cache.clone(),
                        footer_cache.clone(),
                        cache_ident.clone(),
                    )?
                };
                SnapRefsState::Loaded(refs)
            }
        };

        debug!(
            row_groups = row_groups.len(),
            groups_without_stats = groups_without_stats.len(),
            total_rows,
            indexed_ids = match &snap_refs {
                SnapRefsState::NotLoaded => 0,
                SnapRefsState::Loaded(refs) => refs.len(),
            },
            "snap store opened"
        );

        Ok(Self {
            store,
            path,
            file_size,
            file_etag: head_meta.e_tag,
            row_groups,
            groups_without_stats,
            total_rows,
            bbox_col_indices,
            snap_refs,
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
    pub fn query_by_bbox(&self, query_bbox: &BoundingBox) -> Result<Vec<SnapTarget>, SessionError> {
        RT.block_on(
            self.query_by_bbox_async(query_bbox)
                .instrument(tracing::Span::current()),
        )
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

    /// Read all unit IDs referenced by snap targets (projection read of unit_id column only).
    ///
    /// Used at session open time for referential integrity checks.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | File cannot be opened | [`SessionError::Io`] |
    /// | File is not valid Parquet | [`SessionError::ParquetParse`] |
    /// | `unit_id` column missing | [`SessionError::ParquetSchema`] |
    /// | Row contains a null `unit_id` | [`SessionError::InvalidRow`] |
    /// | `unit_id` value fails domain validation | [`SessionError::InvalidRow`] |
    pub fn read_all_unit_ids(&self) -> Result<Vec<hfx_core::UnitId>, SessionError> {
        match &self.snap_refs {
            SnapRefsState::Loaded(refs) => {
                Ok(refs.iter().map(|snap_ref| snap_ref.unit_id).collect())
            }
            SnapRefsState::NotLoaded => Err(SessionError::SnapRefsNotLoaded {
                artifact: ARTIFACT,
                mode: "LazyMetadata",
            }),
        }
    }

    pub(crate) fn read_all_snap_refs(&self) -> Result<Vec<SnapUnitRef>, SessionError> {
        match &self.snap_refs {
            SnapRefsState::Loaded(refs) => Ok(refs.clone()),
            SnapRefsState::NotLoaded => Err(SessionError::SnapRefsNotLoaded {
                artifact: ARTIFACT,
                mode: "LazyMetadata",
            }),
        }
    }

    /// Return the total number of snap target rows across all row groups.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub(crate) fn artifact_meta(&self) -> Option<ArtifactMeta> {
        ArtifactMeta::from_parts(
            self.path.as_ref(),
            self.file_etag.as_deref(),
            self.file_size,
        )
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

fn read_all_snap_refs_from_store(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
) -> Result<Vec<SnapUnitRef>, SessionError> {
    RT.block_on(read_all_snap_refs_from_store_async(
        store,
        path,
        file_size,
        parquet_cache,
        footer_cache,
        cache_ident,
    ))
}

fn read_all_snap_membership_refs_from_store(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
) -> Result<Vec<SnapUnitRef>, SessionError> {
    RT.block_on(read_all_snap_membership_refs_from_store_async(
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
) -> Result<Vec<SnapUnitRef>, SessionError> {
    if let (Some(index_path), Some(etag)) = (id_index_path, file_etag) {
        match IdIndex::load_from_path(index_path, file_size, Some(etag)) {
            Ok(Some(index)) if index.id_row_groups.is_none() => {
                debug!(
                    path = %index_path.display(),
                    ids = index.ids.len(),
                    "cached snap id index lacks snap ids; rebuilding"
                );
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
        read_all_snap_refs_from_store(
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
            ids: ids.iter().map(|snap_ref| snap_ref.unit_id).collect(),
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
async fn read_all_snap_membership_refs_from_store_async(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
) -> Result<Vec<SnapUnitRef>, SessionError> {
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

    let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), Default::default())
        .map_err(|e| SessionError::ParquetParse {
            artifact: ARTIFACT,
            source: e,
        })?;
    let parquet_schema = reader_metadata.parquet_schema();
    let id_col_idx = parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "id")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "missing column \"id\""))?;
    let unit_id_col_idx = parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "unit_id")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "missing column \"unit_id\""))?;

    let mask = ProjectionMask::roots(parquet_schema, vec![id_col_idx, unit_id_col_idx]);
    let selected_row_groups: Vec<usize> = (0..num_row_groups).collect();
    let rg_absolute_starts = absolute_row_starts(&metadata, &selected_row_groups);
    let read_context = SnapMembershipRowGroupReadContext {
        store: Arc::clone(store),
        path: path.clone(),
        file_size,
        reader_metadata,
        mask,
        parquet_cache,
        footer_cache,
        cache_ident,
    };
    debug!(
        row_groups = num_row_groups,
        concurrency = ID_INDEX_ROW_GROUP_CONCURRENCY,
        projected_columns = "id,unit_id",
        "reading snap membership refs for cold validation"
    );
    let row_group_results = stream::iter(selected_row_groups.into_iter().zip(rg_absolute_starts))
        .map(|(row_group, absolute_start)| {
            let read_context = read_context.clone();
            async move {
                read_snap_membership_refs_row_group_async(read_context, row_group, absolute_start)
                    .await
            }
        })
        .buffered(ID_INDEX_ROW_GROUP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut stats = SnapMembershipReadStats::default();
    for row_group_result in row_group_results {
        stats.extend(row_group_result?);
    }
    info!(
        refs = stats.refs.len(),
        row_groups = num_row_groups,
        batches = stats.batches_read,
        membership_rows = stats.membership_rows,
        concurrency = ID_INDEX_ROW_GROUP_CONCURRENCY,
        elapsed_ms = started.elapsed().as_millis(),
        projected_columns = "id,unit_id",
        "cold snap membership read complete"
    );
    Ok(stats.refs)
}

async fn read_snap_membership_refs_row_group_async(
    context: SnapMembershipRowGroupReadContext,
    row_group: usize,
    absolute_start: usize,
) -> Result<SnapMembershipReadStats, SessionError> {
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

    let mut stats = SnapMembershipReadStats::default();
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
                .column_by_name("id")
                .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "id column is not Int64"))?;
            let unit_id_col = batch
                .column_by_name("unit_id")
                .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(ARTIFACT, "unit_id column is not Int64")
                })?;
            stats.batches_read += 1;
            stats.membership_rows += batch.num_rows();
            #[cfg(test)]
            SNAP_MEMBERSHIP_ROWS_FOR_TEST.fetch_add(batch.num_rows(), Ordering::SeqCst);
            for i in 0..batch.num_rows() {
                if id_col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        "null id",
                    ));
                }
                if unit_id_col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        "null unit_id",
                    ));
                }
                let snap_id = SnapId::new(id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        format!("invalid id: {e}"),
                    )
                })?;
                let unit_id = UnitId::new(unit_id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        format!("invalid unit_id: {e}"),
                    )
                })?;
                stats.refs.push(SnapUnitRef { snap_id, unit_id });
            }
            offset_in_group += batch.num_rows();
        }
    }

    Ok(stats)
}

#[instrument(skip(store, parquet_cache, footer_cache, cache_ident), fields(path = %path))]
async fn read_all_snap_refs_from_store_async(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: u64,
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    footer_cache: Option<Arc<ParquetFooterCache>>,
    cache_ident: Option<ArtifactIdent>,
) -> Result<Vec<SnapUnitRef>, SessionError> {
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
    let id_col_idx = parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "id")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "missing column \"id\""))?;
    let unit_id_col_idx = parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "unit_id")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "missing column \"unit_id\""))?;
    let geometry_col_idx = parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "geometry")
        .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "missing column \"geometry\""))?;
    let stem_role_col_idx = parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == "stem_role");

    let mut projection = vec![id_col_idx, unit_id_col_idx, geometry_col_idx];
    if let Some(stem_role_col_idx) = stem_role_col_idx {
        projection.push(stem_role_col_idx);
    }
    let mask = ProjectionMask::roots(parquet_schema, projection);
    let selected_row_groups: Vec<usize> = (0..num_row_groups).collect();
    let rg_absolute_starts = absolute_row_starts(&metadata, &selected_row_groups);
    let read_context = UnitIdRowGroupReadContext {
        store: Arc::clone(store),
        path: path.clone(),
        file_size,
        reader_metadata,
        mask,
        parquet_cache,
        footer_cache,
        cache_ident,
    };
    debug!(
        num_row_groups,
        concurrency = ID_INDEX_ROW_GROUP_CONCURRENCY,
        "reading snap refs for cold validation"
    );
    let row_group_results =
        stream::iter(selected_row_groups.into_iter().zip(rg_absolute_starts))
            .map(|(row_group, absolute_start)| {
                let read_context = read_context.clone();
                async move {
                    read_snap_refs_row_group_async(read_context, row_group, absolute_start).await
                }
            })
            .buffered(ID_INDEX_ROW_GROUP_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

    let mut stats = SnapValidationReadStats::default();
    // Mirrors `catchment_store::read_all_ids_with_row_groups_async`. Keep both in sync.
    // DO NOT construct ParquetRecordBatchStreamBuilder::new inside this loop.
    for row_group_result in row_group_results {
        stats.extend(row_group_result?);
    }
    info!(
        snap_refs = stats.refs.len(),
        num_row_groups,
        concurrency = ID_INDEX_ROW_GROUP_CONCURRENCY,
        batches_read = stats.batches_read,
        rows_validated = stats.rows_validated,
        stem_role_values_parsed = stats.stem_role_values_parsed,
        geometry_rows_validated = stats.geometry_rows_validated,
        elapsed_ms = started.elapsed().as_millis(),
        "cold snap validation read complete"
    );
    Ok(stats.refs)
}

async fn read_snap_refs_row_group_async(
    context: UnitIdRowGroupReadContext,
    row_group: usize,
    absolute_start: usize,
) -> Result<SnapValidationReadStats, SessionError> {
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

    let mut stats = SnapValidationReadStats::default();
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
                .column_by_name("id")
                .and_then(|column| column.as_any().downcast_ref::<arrow::array::Int64Array>())
                .ok_or_else(|| SessionError::parquet_schema(ARTIFACT, "id column is not Int64"))?;
            let unit_id_col = batch
                .column_by_name("unit_id")
                .and_then(|column| column.as_any().downcast_ref::<arrow::array::Int64Array>())
                .ok_or_else(|| {
                    SessionError::parquet_schema(ARTIFACT, "unit_id column is not Int64")
                })?;
            let stem_role_col = batch
                .column_by_name("stem_role")
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            SessionError::parquet_schema(ARTIFACT, "stem_role column is not Utf8")
                        })
                })
                .transpose()?;
            let geometry_col = batch.column_by_name("geometry").ok_or_else(|| {
                SessionError::parquet_schema(ARTIFACT, "geometry column is missing")
            })?;
            stats.batches_read += 1;
            stats.rows_validated += batch.num_rows();
            for i in 0..batch.num_rows() {
                if id_col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        "null id",
                    ));
                }
                if unit_id_col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        "null unit_id",
                    ));
                }
                if geometry_col.is_null(i) {
                    return Err(SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        "null geometry",
                    ));
                }
                let snap_id = SnapId::new(id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        format!("invalid id: {e}"),
                    )
                })?;
                let unit_id = hfx_core::UnitId::new(unit_id_col.value(i)).map_err(|e| {
                    SessionError::invalid_row(
                        ARTIFACT,
                        absolute_row + i,
                        format!("invalid unit_id: {e}"),
                    )
                })?;
                if let Some(stem_role_col) = stem_role_col
                    && !stem_role_col.is_null(i)
                {
                    let value = stem_role_col.value(i);
                    value
                        .parse::<StemRole>()
                        .map_err(|_| SessionError::InvalidStemRole {
                            row: absolute_row + i,
                            value: value.to_string(),
                        })?;
                    stats.stem_role_values_parsed += 1;
                }
                let geometry = geometry_from_array(geometry_col, i, absolute_row + i)?;
                validate_snap_geometry(&geometry, absolute_row + i)?;
                stats.geometry_rows_validated += 1;
                stats.refs.push(SnapUnitRef { snap_id, unit_id });
            }
            offset_in_group += batch.num_rows();
        }
    }

    Ok(stats)
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
    let unit_id_col = batch
        .column_by_name("unit_id")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| {
            SessionError::parquet_schema(ARTIFACT, "column 'unit_id' missing or wrong type")
        })?;
    let weight_col = batch
        .column_by_name("weight")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| {
            SessionError::parquet_schema(ARTIFACT, "column 'weight' missing or wrong type")
        })?;
    let stem_role_col = batch
        .column_by_name("stem_role")
        .map(|c| {
            c.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                SessionError::parquet_schema(ARTIFACT, "column 'stem_role' has wrong type")
            })
        })
        .transpose()?;
    let bbox_cols = optional_bbox_arrays(batch)?;
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
        check_null!(unit_id_col, "unit_id");
        check_null!(weight_col, "weight");
        check_null!(geometry_col_array, "geometry");

        let geometry = geometry_from_array(geometry_col_array, i, absolute_row)?;
        let decoded_geometry = validate_snap_geometry(&geometry, absolute_row)?;
        let row_bbox = match bbox_cols {
            Some((bbox_minx_col, bbox_miny_col, bbox_maxx_col, bbox_maxy_col))
                if !bbox_minx_col.is_null(i)
                    && !bbox_miny_col.is_null(i)
                    && !bbox_maxx_col.is_null(i)
                    && !bbox_maxy_col.is_null(i) =>
            {
                snap_bbox(
                    bbox_minx_col.value(i),
                    bbox_miny_col.value(i),
                    bbox_maxx_col.value(i),
                    bbox_maxy_col.value(i),
                    absolute_row,
                )?
            }
            _ => bbox_from_snap_geometry(&decoded_geometry, absolute_row)?,
        };
        if !row_bbox.intersects(query_bbox) {
            continue;
        }

        let id = SnapId::new(id_col.value(i)).map_err(|e| {
            SessionError::invalid_row(ARTIFACT, absolute_row, format!("id error: {e}"))
        })?;
        let unit_id = UnitId::new(unit_id_col.value(i)).map_err(|e| {
            SessionError::invalid_row(ARTIFACT, absolute_row, format!("unit_id error: {e}"))
        })?;
        let weight = Weight::new(weight_col.value(i)).map_err(|e| {
            SessionError::invalid_row(ARTIFACT, absolute_row, format!("weight error: {e}"))
        })?;
        let stem_role = stem_role_col
            .and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            })
            .map(|value| {
                value
                    .parse::<StemRole>()
                    .map_err(|_| SessionError::InvalidStemRole {
                        row: absolute_row,
                        value: value.to_string(),
                    })
            })
            .transpose()?;
        results.push(SnapTarget::new(
            id,
            unit_id,
            weight,
            stem_role,
            Some(row_bbox),
            geometry,
        ));
    }

    Ok(results)
}

fn optional_bbox_col_indices(
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Option<BboxColIndices>, SessionError> {
    let names = ["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"];
    let indices = names
        .iter()
        .map(|name| match schema.field_with_name(name) {
            Ok(field) if field.data_type() == &DataType::Float32 => {
                Ok(Some(schema.index_of(name).map_err(|_| {
                    SessionError::parquet_schema(ARTIFACT, format!("column \"{name}\" missing"))
                })?))
            }
            Ok(_) => Err(SessionError::parquet_schema(
                ARTIFACT,
                format!("column \"{name}\" must be Float32 when present"),
            )),
            Err(_) => Ok(None),
        })
        .collect::<Result<Vec<_>, SessionError>>()?;

    if indices.iter().all(Option::is_none) {
        return Ok(None);
    }
    if indices.iter().any(Option::is_none) {
        return Err(SessionError::parquet_schema(
            ARTIFACT,
            "snap bbox columns must either all be present or all be absent",
        ));
    }

    let [Some(minx), Some(miny), Some(maxx), Some(maxy)] = indices.as_slice() else {
        return Err(SessionError::parquet_schema(
            ARTIFACT,
            "snap bbox columns must either all be present or all be absent",
        ));
    };

    Ok(Some(BboxColIndices {
        minx: *minx,
        miny: *miny,
        maxx: *maxx,
        maxy: *maxy,
    }))
}

type BboxArrays<'a> = (
    &'a Float32Array,
    &'a Float32Array,
    &'a Float32Array,
    &'a Float32Array,
);

fn optional_bbox_arrays(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<Option<BboxArrays<'_>>, SessionError> {
    let col = |name: &'static str| {
        batch
            .column_by_name(name)
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        SessionError::parquet_schema(
                            ARTIFACT,
                            format!("column '{name}' has wrong type"),
                        )
                    })
            })
            .transpose()
    };
    let cols = (
        col("bbox_minx")?,
        col("bbox_miny")?,
        col("bbox_maxx")?,
        col("bbox_maxy")?,
    );
    match cols {
        (None, None, None, None) => Ok(None),
        (Some(minx), Some(miny), Some(maxx), Some(maxy)) => Ok(Some((minx, miny, maxx, maxy))),
        _ => Err(SessionError::parquet_schema(
            ARTIFACT,
            "snap bbox columns must either all be present or all be absent",
        )),
    }
}

fn geometry_from_array(
    geometry_col_array: &dyn Array,
    row: usize,
    absolute_row: usize,
) -> Result<WkbGeometry, SessionError> {
    #[cfg(test)]
    SNAP_GEOMETRY_DECODE_ROWS_FOR_TEST.fetch_add(1, Ordering::SeqCst);

    let geom_bytes: Vec<u8> =
        if let Some(arr) = geometry_col_array.as_any().downcast_ref::<BinaryArray>() {
            arr.value(row).to_vec()
        } else if let Some(arr) = geometry_col_array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
        {
            arr.value(row).to_vec()
        } else {
            return Err(SessionError::parquet_schema(
                ARTIFACT,
                "column 'geometry' has unsupported type",
            ));
        };
    WkbGeometry::new(geom_bytes).map_err(|e| {
        SessionError::invalid_row(ARTIFACT, absolute_row, format!("geometry error: {e}"))
    })
}

fn validate_snap_geometry(
    geometry: &WkbGeometry,
    row: usize,
) -> Result<Geometry<f64>, SessionError> {
    let decoded = decode_wkb(geometry).map_err(|e| SessionError::SnapGeometryInvalid {
        row,
        reason: e.to_string(),
    })?;
    match decoded {
        Geometry::Point(_) | Geometry::LineString(_) => Ok(decoded),
        other => Err(SessionError::SnapGeometryInvalid {
            row,
            reason: format!(
                "expected Point or LineString, got {}",
                geometry_type_name(&other)
            ),
        }),
    }
}

fn bbox_from_snap_geometry(
    geometry: &Geometry<f64>,
    row: usize,
) -> Result<BoundingBox, SessionError> {
    let rect = geometry
        .bounding_rect()
        .ok_or_else(|| SessionError::SnapGeometryInvalid {
            row,
            reason: "geometry has no finite bounding rectangle".to_string(),
        })?;
    snap_bbox(
        rect.min().x as f32,
        rect.min().y as f32,
        rect.max().x as f32,
        rect.max().y as f32,
        row,
    )
}

fn geometry_type_name(geometry: &Geometry<f64>) -> &'static str {
    match geometry {
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

#[derive(Debug, Clone, Copy)]
pub(crate) enum HeadErrorMode {
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

    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Float32Array, Float32Builder, Int64Array, Int64Builder,
    };
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
    use crate::reader::catchment_store::GEOMETRY_DECODE_TEST_LOCK;

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
            Field::new("unit_id", DataType::Int64, false),
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
        unit_id: i64,
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
        let mut unit_id_b = Int64Builder::new();
        let mut weight_b = Float32Builder::new();
        let mut is_mainstem_b = BooleanBuilder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();

        for row in rows {
            id_b.append_value(row.id);
            unit_id_b.append_value(row.unit_id);
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
                Arc::new(unit_id_b.finish()),
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

    fn write_custom_snap_parquet(
        schema: Arc<Schema>,
        columns: Vec<Arc<dyn Array>>,
    ) -> NamedTempFile {
        let tmp = NamedTempFile::new().unwrap();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let file = tmp.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        tmp
    }

    fn binary_column(values: &[Vec<u8>]) -> Arc<dyn Array> {
        let mut builder = BinaryBuilder::new();
        for value in values {
            builder.append_value(value);
        }
        Arc::new(builder.finish())
    }

    fn open_local_with_mode(path: &Path, mode: SnapOpenMode) -> Result<SnapStore, SessionError> {
        let (store, object_path, path_display) = local_object_artifact(path)?;
        SnapStore::open_with_mode(
            store,
            object_path,
            path_display,
            HeadErrorMode::LocalIo,
            None,
            None,
            None,
            mode,
        )
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
                unit_id: 10,
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
                unit_id: 20,
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
    fn test_read_all_unit_ids_uses_cached_index() {
        let geom = minimal_wkb_linestring(-10.0, -5.0, -9.0, -4.0);
        let rows = vec![
            SnapRow {
                id: 1,
                unit_id: 10,
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
                unit_id: 20,
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
                unit_id: 10,
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
            UnitId::new(10).unwrap(),
            UnitId::new(20).unwrap(),
            UnitId::new(10).unwrap(),
        ];

        assert_eq!(store.read_all_unit_ids().unwrap(), expected);
        assert_eq!(store.read_all_unit_ids().unwrap(), expected);
        assert_eq!(counting_store.head_calls(), head_calls_after_open);
        assert_eq!(counting_store.range_read_calls(), range_reads_after_open);
    }

    #[test]
    fn test_cold_membership_open_reads_refs_without_decoding_geometry() {
        let _decode_guard = GEOMETRY_DECODE_TEST_LOCK
            .lock()
            .expect("geometry decode test lock should not be poisoned");
        reset_snap_geometry_decode_rows_for_test();
        reset_snap_membership_rows_for_test();
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let rows = vec![
            SnapRow {
                id: 1,
                unit_id: 10,
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
                unit_id: 20,
                weight: 0.8,
                is_mainstem: false,
                minx: 1.0,
                miny: 1.0,
                maxx: 2.0,
                maxy: 2.0,
                geom,
            },
        ];

        let tmp = write_snap_parquet(&rows);
        let store = open_local_with_mode(tmp.path(), SnapOpenMode::ColdMembershipValidation)
            .expect("cold membership snap store should open");

        assert_eq!(store.read_all_snap_refs().unwrap().len(), 2);
        assert!(snap_membership_rows_for_test() > 0);
        assert_eq!(snap_geometry_decode_rows_for_test(), 0);
    }

    #[test]
    fn test_lazy_open_rejects_wrong_stem_role_column_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("unit_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("stem_role", DataType::Int64, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let tmp = write_custom_snap_parquet(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![10])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![1])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![2.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![2.0])) as Arc<dyn Array>,
                binary_column(&[geom]),
            ],
        );

        let result = open_local_with_mode(tmp.path(), SnapOpenMode::LazyMetadata);

        assert!(matches!(result, Err(SessionError::ParquetSchema { .. })));
    }

    #[test]
    fn test_lazy_open_rejects_missing_geometry_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("unit_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
        ]));
        let tmp = write_custom_snap_parquet(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![10])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![2.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![2.0])) as Arc<dyn Array>,
            ],
        );

        let result = open_local_with_mode(tmp.path(), SnapOpenMode::LazyMetadata);

        assert!(matches!(result, Err(SessionError::ParquetSchema { .. })));
    }

    #[test]
    fn test_lazy_open_rejects_partial_bbox_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("unit_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let tmp = write_custom_snap_parquet(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![10])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                Arc::new(Float32Array::from(vec![1.0])) as Arc<dyn Array>,
                binary_column(&[geom]),
            ],
        );

        let result = open_local_with_mode(tmp.path(), SnapOpenMode::LazyMetadata);

        assert!(matches!(result, Err(SessionError::ParquetSchema { .. })));
    }

    #[test]
    fn test_query_by_bbox_returns_matching() {
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let rows = vec![
            SnapRow {
                id: 1,
                unit_id: 10,
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
                unit_id: 20,
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
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

        let results = tracing::dispatcher::with_default(&dispatch, || {
            let stage = StageGuard::enter(Stage::OutletResolve);
            let result = store.query_by_bbox(&query);
            drop(stage);
            result
        })
        .unwrap();
        drop(guard);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id(), SnapId::new(2).unwrap());
        assert_eq!(results[0].unit_id(), UnitId::new(20).unwrap());

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
    fn test_lazy_open_query_by_bbox_returns_matching() {
        let geom = minimal_wkb_linestring(1.0, 1.0, 2.0, 2.0);
        let rows = vec![
            SnapRow {
                id: 1,
                unit_id: 10,
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
                unit_id: 20,
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
        let store = open_local_with_mode(tmp.path(), SnapOpenMode::LazyMetadata)
            .expect("lazy snap store should open");

        let query = BoundingBox::new(0.5, 0.5, 3.0, 3.0).unwrap();
        let results = store.query_by_bbox(&query).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id(), SnapId::new(2).unwrap());
        assert_eq!(results[0].unit_id(), UnitId::new(20).unwrap());
    }

    #[test]
    fn test_query_by_bbox_preserves_row_group_order_under_concurrency() {
        let rows: Vec<_> = (1..=24)
            .map(|id| {
                let x = id as f32;
                SnapRow {
                    id,
                    unit_id: id * 10,
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
            unit_id: 10,
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
                unit_id: 10,
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
                unit_id: 10,
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
        assert_eq!(results[0].stem_role(), None);
        assert_eq!(results[1].stem_role(), None);
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
            unit_id: 10,
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
            unit_id: 20,
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
            unit_id: 10,
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
            Field::new("unit_id", DataType::Int64, false),
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

        let err = SnapStore::open(tmp.path()).unwrap_err();
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
            Field::new("unit_id", DataType::Int64, false),
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
            Field::new("unit_id", DataType::Int64, false),
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
