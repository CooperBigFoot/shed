//! Dataset session — loads an HFX dataset for repeated queries.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use futures_util::StreamExt;
use geo::Rect;
use hfx_core::{DrainageGraph, Level, Manifest, Topology, UnitId};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{debug, info, instrument};
use url::Url;

use crate::cache::{ArtifactMeta, RemoteArtifactCache, ValidationSidecar};
use crate::cog::{
    CogExtent, EXTENT_HEADER_RANGE_BYTES, LocalizedRasterWindow, RasterWindowRequest,
    read_local_extent, read_remote_extent,
};
use crate::error::{CacheError, SessionError};
use crate::parquet_cache::{
    DEFAULT_PARQUET_CACHE_MAX_BYTES, ParquetFooterCache, ParquetRowGroupCache,
};
use crate::raster_cache::RemoteRasterCache;
use crate::reader;
use crate::reader::catchment_store::CatchmentStore;
use crate::reader::manifest::{AuxDeclarations, SnapDecl};
use crate::reader::snap_store::SnapStore;
use crate::refinement::D8RasterHandle;
use crate::runtime::RT;
use crate::source::{DatasetSource, shed_get_ranges_concurrency};
use crate::source_telemetry::{HttpStatsHandle, HttpStatsSnapshot};
use crate::telemetry::{Stage, StageGuard, record_bytes, record_path};

/// Raster artifact selector for lazy localization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RasterKind {
    /// The HFX `flow_dir.tif` artifact.
    FlowDir,
    /// The HFX `flow_acc.tif` artifact.
    FlowAcc,
}

impl RasterKind {
    pub(crate) fn cache_name(self) -> &'static str {
        match self {
            Self::FlowDir => "flow-dir",
            Self::FlowAcc => "flow-acc",
        }
    }
}

/// Validated paths to the optional raster pair.
///
/// Stores raster URIs only — no reading, no GDAL.
#[derive(Debug, Clone)]
pub struct RasterPaths {
    flow_dir: String,
    flow_acc: String,
}

impl RasterPaths {
    /// Return the URI string for the flow direction raster.
    pub fn flow_dir_uri(&self) -> &str {
        &self.flow_dir
    }

    /// Return the URI string for the flow accumulation raster.
    pub fn flow_acc_uri(&self) -> &str {
        &self.flow_acc
    }

    /// Return the path view of the flow direction raster.
    pub fn flow_dir(&self) -> &Path {
        Path::new(&self.flow_dir)
    }

    /// Return the path view of the flow accumulation raster.
    pub fn flow_acc(&self) -> &Path {
        Path::new(&self.flow_acc)
    }
}

/// A loaded HFX dataset, ready for repeated queries.
///
/// Created via [`DatasetSession::open`] or [`DatasetSession::open_with_cache`].
/// Holds the manifest and drainage graph in memory. Catchment and snap data
/// are read on demand via row-group bbox pruning.
#[derive(Debug)]
pub struct DatasetSession {
    root: PathBuf,
    manifest: Manifest,
    aux_declarations: AuxDeclarations,
    graph: hfx_core::DrainageGraph,
    catchments: CatchmentStore,
    snap_stores: Vec<DeclaredSnapStore>,
    raster_paths: Option<RasterPaths>,
    raster_cache: Option<Arc<RemoteRasterCache>>,
    remote_store: Option<Arc<dyn ObjectStore>>,
    remote_root: Option<ObjectPath>,
    http_stats: Option<HttpStatsHandle>,
    fabric_cache_key: (String, String),
    /// Optional Parquet column-chunk cache shared across catchment and snap readers.
    /// Stored here for future `engine.cache_stats()` exposure (deferred to v2).
    #[allow(dead_code)]
    parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    /// Optional Parquet footer cache shared across catchment and snap readers.
    #[allow(dead_code)]
    footer_cache: Option<Arc<ParquetFooterCache>>,
}

#[derive(Debug)]
struct DeclaredSnapStore {
    decl: SnapDecl,
    store: SnapStore,
}

#[derive(Debug, Clone)]
struct ValidationSidecarInputs {
    hfx_format_version: String,
    manifest: Option<ArtifactMeta>,
    graph: Option<ArtifactMeta>,
    catchments: Option<ArtifactMeta>,
    snaps: Option<Vec<ArtifactMeta>>,
}

#[cfg(test)]
static SNAP_VALIDATION_SCAN_COUNT_FOR_TEST: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn record_snap_validation_scan_for_test() {
    SNAP_VALIDATION_SCAN_COUNT_FOR_TEST.fetch_add(1, Ordering::SeqCst);
}

#[cfg(test)]
fn snap_validation_scan_count_for_test() -> usize {
    SNAP_VALIDATION_SCAN_COUNT_FOR_TEST.load(Ordering::SeqCst)
}

#[cfg(test)]
fn reset_snap_validation_scan_count_for_test() {
    SNAP_VALIDATION_SCAN_COUNT_FOR_TEST.store(0, Ordering::SeqCst);
}

impl DatasetSession {
    /// Open an HFX dataset source and return a ready-to-query session.
    ///
    /// Local paths and `file://` URLs are opened from the local filesystem.
    /// Remote sources are opened through object-store-backed parquet readers.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | Source parsing errors | The dataset source string is malformed or unsupported |
    /// | Local session errors | Propagated from [`DatasetSession::open_path`] |
    #[instrument(skip_all, fields(input = %input))]
    pub fn open(input: &str) -> Result<Self, SessionError> {
        match DatasetSource::parse(input)? {
            DatasetSource::Local(root) => Self::open_path(&root),
            DatasetSource::Remote {
                store,
                http_stats,
                root,
                url,
            } => Self::open_remote_with_default_caches(store, &root, &url, http_stats),
        }
    }

    /// Open an HFX dataset source with an optional Parquet column-chunk cache.
    ///
    /// Behaves identically to [`DatasetSession::open`] when `cache` is `None`.
    /// When `cache` is `Some`, remote parquet reads are intercepted by
    /// [`crate::parquet_cache::CachingReader`].
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | Source parsing errors | The dataset source string is malformed or unsupported |
    /// | Local session errors | Propagated from [`DatasetSession::open_path`] |
    #[instrument(skip_all, fields(input = %input))]
    pub fn open_with_cache(
        input: &str,
        cache: Option<Arc<ParquetRowGroupCache>>,
    ) -> Result<Self, SessionError> {
        Self::open_with_caches(input, cache, None)
    }

    /// Open an HFX dataset source with optional Parquet row-group and footer caches.
    ///
    /// Behaves identically to [`DatasetSession::open`] when both caches are `None`.
    /// When caches are supplied, remote parquet readers use the shared
    /// [`crate::parquet_cache::CachingReader`].
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | Source parsing errors | The dataset source string is malformed or unsupported |
    /// | Local session errors | Propagated from [`DatasetSession::open_path`] |
    #[instrument(skip_all, fields(input = %input))]
    pub fn open_with_caches(
        input: &str,
        row_group_cache: Option<Arc<ParquetRowGroupCache>>,
        footer_cache: Option<Arc<ParquetFooterCache>>,
    ) -> Result<Self, SessionError> {
        match DatasetSource::parse(input)? {
            DatasetSource::Local(root) => Self::open_path(&root),
            DatasetSource::Remote {
                store,
                http_stats,
                root,
                url,
            } => Self::open_remote_with_stats(
                store,
                &root,
                &url,
                row_group_cache,
                footer_cache,
                http_stats,
            ),
        }
    }

    /// Open an HFX dataset rooted at a local filesystem path.
    ///
    /// Validates the directory layout against the manifest, loads the drainage
    /// graph into memory, and prepares lazy readers for catchment and snap data.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`SessionError::RootNotFound`] | `root` does not exist or is not a directory |
    /// | [`SessionError::RequiredArtifactMissing`] | A required file is absent |
    /// | [`SessionError::OptionalArtifactMissing`] | Manifest declares an optional artifact that is missing |
    /// | [`SessionError::UnitCountMismatch`] | Row count in catchments.parquet differs from manifest |
    /// | Manifest/graph/Parquet errors | Propagated from sub-readers |
    #[instrument(skip_all, fields(root = %root.display()))]
    pub fn open_path(root: &Path) -> Result<Self, SessionError> {
        if !root.is_dir() {
            return Err(SessionError::RootNotFound {
                path: root.display().to_string(),
            });
        }

        let legacy_graph = root.join("graph.arrow");
        if legacy_graph.exists() {
            return Err(SessionError::LegacyGraphArrowRejected {
                path: legacy_graph.display().to_string(),
            });
        }

        for artifact in ["manifest.json", "graph.parquet", "catchments.parquet"] {
            let p = root.join(artifact);
            if !p.exists() {
                return Err(SessionError::required_missing(
                    artifact,
                    p.display().to_string(),
                ));
            }
        }

        let parsed = reader::manifest::read_manifest(&root.join("manifest.json"))?;
        let manifest = parsed.manifest;
        let aux_declarations = parsed.aux;

        validate_local_aux_paths(root, &aux_declarations)?;

        let graph = reader::graph::load_graph(&root.join("graph.parquet"))?;

        let catchments = CatchmentStore::open(&root.join("catchments.parquet"))?;

        let catchment_levels = {
            let _guard = StageGuard::enter(Stage::ValidateGraphCatchments);
            validate_graph_catchments(&manifest, &graph, &catchments)?
        };

        let snap_stores = aux_declarations
            .snaps
            .iter()
            .map(|decl| {
                Ok(DeclaredSnapStore {
                    decl: decl.clone(),
                    store: SnapStore::open(&root.join(&decl.snap))?,
                })
            })
            .collect::<Result<Vec<_>, SessionError>>()?;

        // If snap is present, verify all snap catchment_id references exist.
        for declared_snap in &snap_stores {
            let _guard = StageGuard::enter(Stage::ValidateSnapRefs);
            validate_snap_refs(&declared_snap.store, &declared_snap.decl, &catchment_levels)?;
        }

        let raster_paths = aux_declarations.d8_rasters.first().map(|decl| RasterPaths {
            flow_dir: raster_uri_string(&root.join(&decl.flow_dir)),
            flow_acc: raster_uri_string(&root.join(&decl.flow_acc)),
        });

        info!(
            fabric = manifest.fabric_name(),
            units = manifest.unit_count().get(),
            topology = %manifest.topology(),
            "dataset session opened"
        );

        Ok(DatasetSession {
            root: root.to_path_buf(),
            fabric_cache_key: (
                manifest.fabric_name().to_string(),
                manifest.adapter_version().to_string(),
            ),
            manifest,
            aux_declarations,
            graph,
            catchments,
            snap_stores,
            raster_paths,
            raster_cache: None,
            remote_store: None,
            remote_root: None,
            http_stats: None,
            parquet_cache: None,
            footer_cache: None,
        })
    }

    fn open_remote(
        store: Arc<dyn ObjectStore>,
        root: &ObjectPath,
        url: &Url,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    ) -> Result<Self, SessionError> {
        Self::open_remote_with_stats(store, root, url, parquet_cache, None, None)
    }

    fn open_remote_with_default_caches(
        store: Arc<dyn ObjectStore>,
        root: &ObjectPath,
        url: &Url,
        http_stats: Option<HttpStatsHandle>,
    ) -> Result<Self, SessionError> {
        let parquet_cache = Some(ParquetRowGroupCache::new(DEFAULT_PARQUET_CACHE_MAX_BYTES));
        let footer_cache = Some(ParquetFooterCache::new());
        Self::open_remote_with_stats(store, root, url, parquet_cache, footer_cache, http_stats)
    }

    fn open_remote_with_stats(
        store: Arc<dyn ObjectStore>,
        root: &ObjectPath,
        url: &Url,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
        footer_cache: Option<Arc<ParquetFooterCache>>,
        http_stats: Option<HttpStatsHandle>,
    ) -> Result<Self, SessionError> {
        let session_start = std::time::Instant::now();

        let (cache, raster_cache) = {
            let _remote_open_guard = StageGuard::enter(Stage::RemoteOpen);
            record_path(url.as_str());
            info!(url = %url, "opening remote dataset");
            let cache = RemoteArtifactCache::configured()?;
            let raster_cache = Arc::new(RemoteRasterCache::new(cache.root().to_path_buf()));
            (cache, raster_cache)
        };
        let manifest_path = remote_artifact_path(root, "manifest.json");
        let graph_path = remote_artifact_path(root, "graph.parquet");
        let manifest_meta = remote_artifact_meta(store.as_ref(), &manifest_path, "manifest.json");
        let graph_meta = remote_artifact_meta(store.as_ref(), &graph_path, "graph.parquet");

        let (manifest, aux_declarations, graph) = if let Some(cached) =
            cache.read_entry_for_source(url, root)?
        {
            debug!(
                fabric = cached.manifest.fabric_name(),
                units = cached.manifest.unit_count().get(),
                graph_units = cached.graph.len(),
                "remote manifest and graph parsed from cache"
            );
            (cached.manifest, cached.aux, cached.graph)
        } else {
            let t = std::time::Instant::now();
            let manifest_bytes = {
                let _guard = StageGuard::enter(Stage::ManifestFetch);
                record_path(manifest_path.as_ref());
                let bytes =
                    read_remote_artifact(store.as_ref(), manifest_path.clone(), "manifest.json")?;
                record_bytes(bytes.len() as u64);
                bytes
            };
            info!(
                bytes = manifest_bytes.len(),
                duration_ms = t.elapsed().as_millis(),
                "fetched manifest"
            );
            let parsed = reader::manifest::read_manifest_from_bytes(&manifest_bytes)?;
            validate_remote_aux_paths(root, &parsed.aux)?;
            let manifest = parsed.manifest;
            let aux_declarations = parsed.aux;

            let t = std::time::Instant::now();
            let (graph_bytes, graph) = {
                let _guard = StageGuard::enter(Stage::GraphFetch);
                record_path(graph_path.as_ref());
                let bytes =
                    read_remote_artifact(store.as_ref(), graph_path.clone(), "graph.parquet")?;
                record_bytes(bytes.len() as u64);
                let graph = reader::graph::load_graph_from_bytes(bytes.clone())?;
                (bytes, graph)
            };
            info!(
                bytes = graph_bytes.len(),
                units = graph.len(),
                duration_ms = t.elapsed().as_millis(),
                "fetched graph"
            );
            cache.write_manifest_graph(url, root, &manifest, &manifest_bytes, &graph_bytes)?;

            debug!(
                fabric = manifest.fabric_name(),
                units = manifest.unit_count().get(),
                graph_units = graph.len(),
                "remote manifest and graph parsed"
            );
            (manifest, aux_declarations, graph)
        };

        let fabric_name = manifest.fabric_name().to_string();
        let adapter_version = manifest.adapter_version().to_string();
        let catchments_id_index_path =
            cache.id_index_path(&fabric_name, &adapter_version, "catchments.parquet");
        let catchments_path = remote_artifact_path(root, "catchments.parquet");
        let catchments = CatchmentStore::open_remote_with_caches(
            store.clone(),
            catchments_path.clone(),
            catchments_path.as_ref().to_string(),
            fabric_name.clone(),
            adapter_version.clone(),
            parquet_cache.clone(),
            footer_cache.clone(),
            Some(catchments_id_index_path),
        )?;

        let snap_stores = aux_declarations
            .snaps
            .iter()
            .map(|decl| {
                let snap_path = remote_artifact_path(root, &decl.snap);
                let snap_id_index_path =
                    cache.id_index_path(&fabric_name, &adapter_version, &decl.snap);
                Ok(DeclaredSnapStore {
                    decl: decl.clone(),
                    store: SnapStore::open_remote_with_caches(
                        store.clone(),
                        snap_path.clone(),
                        snap_path.as_ref().to_string(),
                        fabric_name.clone(),
                        adapter_version.clone(),
                        parquet_cache.clone(),
                        footer_cache.clone(),
                        Some(snap_id_index_path),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, SessionError>>()?;

        let catchments_meta = catchments.artifact_meta();
        let snap_meta = snap_stores
            .iter()
            .map(|declared_snap| {
                declared_snap
                    .store
                    .artifact_meta()
                    .map(|meta| meta.with_path(declared_snap.decl.snap.clone()))
            })
            .collect::<Option<Vec<_>>>();
        let validation_inputs = ValidationSidecarInputs {
            hfx_format_version: manifest.format_version().to_string(),
            manifest: manifest_meta,
            graph: graph_meta,
            catchments: catchments_meta,
            snaps: snap_meta,
        };
        let validation_hit =
            validation_sidecar_matches(&cache, &fabric_name, &adapter_version, &validation_inputs);

        if validation_hit {
            debug!(
                fabric = fabric_name,
                adapter_version, "remote validation sidecar matched current artifact metadata"
            );
        } else {
            let t = std::time::Instant::now();
            debug!(
                fabric = fabric_name,
                adapter_version,
                "remote validation sidecar missing or stale; validating referential integrity"
            );
            let catchment_levels = {
                let _guard = StageGuard::enter(Stage::ValidateGraphCatchments);
                validate_graph_catchments(&manifest, &graph, &catchments)?
            };
            info!(
                rows = catchment_levels.len(),
                duration_ms = t.elapsed().as_millis(),
                "indexed catchments"
            );
            for declared_snap in &snap_stores {
                let _guard = StageGuard::enter(Stage::ValidateSnapRefs);
                validate_snap_refs(&declared_snap.store, &declared_snap.decl, &catchment_levels)?;
            }
            if let Some(sidecar) =
                validation_sidecar_for_current_metadata(validation_inputs.clone())
            {
                cache.write_validation_sidecar_best_effort(
                    &fabric_name,
                    &adapter_version,
                    &sidecar,
                );
            } else {
                debug!(
                    fabric = fabric_name,
                    adapter_version,
                    "not caching validation sidecar because artifact metadata lacks ETag"
                );
            }
        }

        if validation_hit {
            debug!("skipped remote referential validation");
        }

        let raster_paths = aux_declarations.d8_rasters.first().map(|decl| RasterPaths {
            flow_dir: remote_artifact_url_string(url, &decl.flow_dir),
            flow_acc: remote_artifact_url_string(url, &decl.flow_acc),
        });

        info!(
            fabric = manifest.fabric_name(),
            units = manifest.unit_count().get(),
            topology = %manifest.topology(),
            elapsed_ms = session_start.elapsed().as_millis(),
            "remote dataset session opened"
        );

        Ok(DatasetSession {
            root: PathBuf::from(url.as_str()),
            fabric_cache_key: (fabric_name, adapter_version),
            manifest,
            aux_declarations,
            graph,
            catchments,
            snap_stores,
            raster_paths,
            raster_cache: Some(raster_cache),
            remote_store: Some(store),
            remote_root: Some(root.clone()),
            http_stats,
            parquet_cache,
            footer_cache,
        })
    }

    /// Open a remote HFX dataset with an already-constructed object store.
    #[doc(hidden)]
    pub fn open_remote_with_store(
        store: Arc<dyn ObjectStore>,
        root: &ObjectPath,
        url: &Url,
    ) -> Result<Self, SessionError> {
        Self::open_remote(store, root, url, None)
    }

    /// Return a reference to the parsed manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Return the parsed auxiliary declarations.
    pub fn auxiliary_declarations(&self) -> &AuxDeclarations {
        &self.aux_declarations
    }

    /// Return the graph topology declared in the manifest.
    pub fn topology(&self) -> Topology {
        self.manifest.topology()
    }

    /// Return a reference to the in-memory drainage graph.
    pub fn graph(&self) -> &hfx_core::DrainageGraph {
        &self.graph
    }

    /// Return the validated HFX level for a drainage unit.
    pub fn level_of(&self, unit_id: UnitId) -> Option<Level> {
        self.graph.get(unit_id).map(|row| row.level())
    }

    /// Return the sorted set of HFX levels present in the loaded dataset.
    pub fn levels(&self) -> Vec<Level> {
        self.graph
            .rows()
            .iter()
            .map(|row| row.level())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Return the finest HFX level present in the loaded dataset.
    pub fn max_level(&self) -> Option<Level> {
        self.graph.rows().iter().map(|row| row.level()).max()
    }

    /// Return a reference to the catchment store for on-demand queries.
    pub fn catchments(&self) -> &CatchmentStore {
        &self.catchments
    }

    /// Return a reference to the snap store, if present.
    pub fn snap(&self) -> Option<&SnapStore> {
        self.snap_stores
            .first()
            .map(|declared_snap| &declared_snap.store)
    }

    /// Return the snap store whose declaration is selected for `level`.
    ///
    /// M3 deliberately uses a narrow deterministic rule instead of a general
    /// strategy binding: among `hfx.aux.snap.v1` declarations whose
    /// `references_levels` contains `level`, sort by metadata `name` ascending,
    /// then artifact `snap` path ascending, and use the first declaration.
    pub(crate) fn snap_for_level(&self, level: Level) -> Option<&SnapStore> {
        self.snap_stores
            .iter()
            .filter(|declared_snap| declared_snap.decl.references_levels.contains(&level.get()))
            .min_by(|a, b| {
                a.decl
                    .name
                    .cmp(&b.decl.name)
                    .then_with(|| a.decl.snap.cmp(&b.decl.snap))
            })
            .map(|declared_snap| &declared_snap.store)
    }

    /// Return the validated raster paths, if rasters are present.
    pub fn raster_paths(&self) -> Option<&RasterPaths> {
        self.raster_paths.as_ref()
    }

    /// Return whether the manifest declares blessed D8 raster auxiliary data.
    pub fn has_d8_aux(&self) -> bool {
        !self.aux_declarations.d8_rasters.is_empty()
    }

    /// Return object-store request counters when network benchmarking is enabled.
    pub fn http_stats(&self) -> Option<HttpStatsSnapshot> {
        self.http_stats.as_ref().map(HttpStatsHandle::snapshot)
    }

    /// Select the single blessed-D8 declaration whose raster extents cover `bbox`.
    ///
    /// Coverage uses inclusive closed rectangles so equality and edge-touching
    /// count as intersection/containment.
    pub fn select_d8_raster_for_bbox(
        &self,
        bbox: Rect<f64>,
    ) -> Result<D8RasterHandle, SessionError> {
        if self.aux_declarations.d8_rasters.is_empty() {
            return Err(SessionError::MissingRequiredD8Aux);
        }

        let mut intersecting = Vec::new();
        let mut covering = Vec::new();
        for (index, decl) in self.aux_declarations.d8_rasters.iter().enumerate() {
            let flow_dir_extent = self.d8_extent(index, RasterKind::FlowDir, &decl.flow_dir)?;
            if !rects_intersect_inclusive(&flow_dir_extent.rect(), &bbox) {
                continue;
            }
            let flow_acc_extent = self.d8_extent(index, RasterKind::FlowAcc, &decl.flow_acc)?;
            if rects_intersect_inclusive(&flow_acc_extent.rect(), &bbox) {
                intersecting.push(index);
            }
            if rect_contains_inclusive(&flow_dir_extent.rect(), &bbox)
                && rect_contains_inclusive(&flow_acc_extent.rect(), &bbox)
            {
                covering.push(self.d8_handle(index)?);
            }
        }

        match covering.len() {
            1 => Ok(covering.remove(0)),
            n if n > 1 => Err(SessionError::AmbiguousD8Coverage {
                min_x: bbox.min().x,
                min_y: bbox.min().y,
                max_x: bbox.max().x,
                max_y: bbox.max().y,
                declaration_indices: covering
                    .iter()
                    .map(D8RasterHandle::declaration_index)
                    .collect(),
            }),
            _ if intersecting.len() > 1 => Err(SessionError::TerminalSpansD8Tiles {
                min_x: bbox.min().x,
                min_y: bbox.min().y,
                max_x: bbox.max().x,
                max_y: bbox.max().y,
                declaration_indices: intersecting,
            }),
            _ => Err(SessionError::NoCoveringD8Tile {
                min_x: bbox.min().x,
                min_y: bbox.min().y,
                max_x: bbox.max().x,
                max_y: bbox.max().y,
            }),
        }
    }

    /// Return a local filesystem path for a window of the selected D8 declaration.
    pub fn localize_d8_raster_window(
        &self,
        handle: &D8RasterHandle,
        kind: RasterKind,
        bbox: Rect<f64>,
    ) -> Result<LocalizedRasterWindow, SessionError> {
        if let (Some(cache), Some(store), Some(_root)) = (
            self.raster_cache.as_ref(),
            self.remote_store.as_ref(),
            self.remote_root.as_ref(),
        ) {
            let remote_path = selected_remote_path(handle, kind).ok_or_else(|| {
                SessionError::integrity("selected D8 handle has no remote object-store path")
            })?;
            let request = RasterWindowRequest::new(kind, bbox);
            let (fabric_name, adapter_version) = &self.fabric_cache_key;
            return RT
                .block_on(cache.get_or_fetch_window(
                    store.as_ref(),
                    remote_path,
                    &request,
                    fabric_name,
                    adapter_version,
                ))
                .map_err(SessionError::from);
        }

        if self.raster_cache.is_some() || self.remote_store.is_some() || self.remote_root.is_some()
        {
            return Err(SessionError::integrity(
                "remote raster localization state is incomplete",
            ));
        }

        Ok(LocalizedRasterWindow::cached(PathBuf::from(selected_uri(
            handle, kind,
        ))))
    }

    /// Return the dataset root directory path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn d8_extent(
        &self,
        declaration_index: usize,
        kind: RasterKind,
        decl_path: &str,
    ) -> Result<CogExtent, SessionError> {
        if let (Some(store), Some(root)) = (self.remote_store.as_ref(), self.remote_root.as_ref()) {
            let remote_path = remote_artifact_path(root, decl_path);
            return RT
                .block_on(read_remote_extent(store.as_ref(), &remote_path))
                .map_err(|source| map_extent_error(declaration_index, kind, decl_path, source));
        }

        let path = self.root.join(decl_path);
        read_local_extent(&path).map_err(|source| {
            map_extent_error(declaration_index, kind, &path.display().to_string(), source)
        })
    }

    fn d8_handle(&self, declaration_index: usize) -> Result<D8RasterHandle, SessionError> {
        let decl = self
            .aux_declarations
            .d8_rasters
            .get(declaration_index)
            .ok_or_else(|| SessionError::integrity("selected D8 declaration index is absent"))?;
        if let Some(root) = self.remote_root.as_ref() {
            let flow_dir_path = remote_artifact_path(root, &decl.flow_dir);
            let flow_acc_path = remote_artifact_path(root, &decl.flow_acc);
            return Ok(D8RasterHandle::new(
                declaration_index,
                remote_artifact_url_string_from_root(&self.root, &decl.flow_dir),
                remote_artifact_url_string_from_root(&self.root, &decl.flow_acc),
                Some(flow_dir_path),
                Some(flow_acc_path),
                decl.flow_dir_encoding,
            ));
        }

        Ok(D8RasterHandle::new(
            declaration_index,
            raster_uri_string(&self.root.join(&decl.flow_dir)),
            raster_uri_string(&self.root.join(&decl.flow_acc)),
            None,
            None,
            decl.flow_dir_encoding,
        ))
    }
}

fn remote_artifact_path(root: &ObjectPath, artifact: &str) -> ObjectPath {
    if root.as_ref().is_empty() {
        ObjectPath::from(artifact)
    } else {
        ObjectPath::from(format!(
            "{}/{artifact}",
            root.as_ref().trim_end_matches('/')
        ))
    }
}

fn raster_uri_string(path: &Path) -> String {
    path.display().to_string()
}

fn remote_artifact_url_string(url: &Url, artifact: &str) -> String {
    format!("{}/{}", url.as_str().trim_end_matches('/'), artifact)
}

fn remote_artifact_url_string_from_root(root: &Path, artifact: &str) -> String {
    format!(
        "{}/{}",
        root.display().to_string().trim_end_matches('/'),
        artifact
    )
}

fn selected_uri(handle: &D8RasterHandle, kind: RasterKind) -> &str {
    match kind {
        RasterKind::FlowDir => handle.flow_dir_uri(),
        RasterKind::FlowAcc => handle.flow_acc_uri(),
    }
}

fn selected_remote_path(handle: &D8RasterHandle, kind: RasterKind) -> Option<&ObjectPath> {
    match kind {
        RasterKind::FlowDir => handle.remote_flow_dir_path(),
        RasterKind::FlowAcc => handle.remote_flow_acc_path(),
    }
}

fn map_extent_error(
    declaration_index: usize,
    kind: RasterKind,
    path: &str,
    source: CacheError,
) -> SessionError {
    match &source {
        CacheError::UnsupportedCog { reason, .. } if reason.contains("extent header too large") => {
            SessionError::CogExtentHeaderTooLarge {
                declaration_index,
                kind,
                path: path.to_string(),
                limit_bytes: EXTENT_HEADER_RANGE_BYTES,
            }
        }
        _ => SessionError::CogExtentHeaderRead {
            declaration_index,
            kind,
            path: path.to_string(),
            source,
        },
    }
}

fn rect_contains_inclusive(container: &Rect<f64>, candidate: &Rect<f64>) -> bool {
    candidate.min().x >= container.min().x
        && candidate.min().y >= container.min().y
        && candidate.max().x <= container.max().x
        && candidate.max().y <= container.max().y
}

fn rects_intersect_inclusive(a: &Rect<f64>, b: &Rect<f64>) -> bool {
    a.min().x <= b.max().x
        && a.max().x >= b.min().x
        && a.min().y <= b.max().y
        && a.max().y >= b.min().y
}

fn validate_local_aux_paths(root: &Path, aux: &AuxDeclarations) -> Result<(), SessionError> {
    for decl in &aux.d8_rasters {
        validate_local_aux_artifact(root, "hfx.aux.d8_raster.v1", "flow_dir", &decl.flow_dir)?;
        validate_local_aux_artifact(root, "hfx.aux.d8_raster.v1", "flow_acc", &decl.flow_acc)?;
    }
    for decl in &aux.snaps {
        validate_local_aux_artifact(root, "hfx.aux.snap.v1", "snap", &decl.snap)?;
    }
    for decl in &aux.generic {
        for (artifact, path) in &decl.artifacts {
            validate_local_aux_artifact(root, &decl.schema, artifact, path)?;
        }
    }
    Ok(())
}

fn validate_local_aux_artifact(
    root: &Path,
    schema: &str,
    artifact: &str,
    raw_path: &str,
) -> Result<(), SessionError> {
    if path_escapes_root(raw_path) {
        return Err(SessionError::AuxiliaryPathEscape {
            schema: schema.to_string(),
            artifact: artifact.to_string(),
            path: raw_path.to_string(),
        });
    }
    let path = root.join(raw_path);
    if !path.exists() {
        return Err(SessionError::AuxiliaryArtifactMissing {
            schema: schema.to_string(),
            artifact: artifact.to_string(),
            path: path.display().to_string(),
        });
    }
    Ok(())
}

fn validate_remote_aux_paths(root: &ObjectPath, aux: &AuxDeclarations) -> Result<(), SessionError> {
    for decl in &aux.d8_rasters {
        validate_remote_aux_artifact(root, "hfx.aux.d8_raster.v1", "flow_dir", &decl.flow_dir)?;
        validate_remote_aux_artifact(root, "hfx.aux.d8_raster.v1", "flow_acc", &decl.flow_acc)?;
    }
    for decl in &aux.snaps {
        validate_remote_aux_artifact(root, "hfx.aux.snap.v1", "snap", &decl.snap)?;
    }
    for decl in &aux.generic {
        for (artifact, path) in &decl.artifacts {
            validate_remote_aux_artifact(root, &decl.schema, artifact, path)?;
        }
    }
    Ok(())
}

fn validate_remote_aux_artifact(
    _root: &ObjectPath,
    schema: &str,
    artifact: &str,
    raw_path: &str,
) -> Result<(), SessionError> {
    if path_escapes_root(raw_path) {
        return Err(SessionError::AuxiliaryPathEscape {
            schema: schema.to_string(),
            artifact: artifact.to_string(),
            path: raw_path.to_string(),
        });
    }
    Ok(())
}

fn path_escapes_root(raw_path: &str) -> bool {
    let path = Path::new(raw_path);
    path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
}

const RANGE_GET_CHUNK_TARGET_BYTES: u64 = 4 * 1024 * 1024;

/// Validate graph/catchment referential integrity.
///
/// Changing open-time referential validation semantics requires bumping
/// `validation_logic_version`.
fn validate_graph_catchments(
    manifest: &Manifest,
    graph: &DrainageGraph,
    catchments: &CatchmentStore,
) -> Result<HashMap<UnitId, Level>, SessionError> {
    let expected = manifest.unit_count().get();
    let actual = catchments.total_rows();
    if expected != actual {
        return Err(SessionError::UnitCountMismatch {
            manifest_count: expected,
            actual_count: actual,
        });
    }

    debug!("verifying graph ↔ catchment referential integrity");
    let catchment_id_level_rows = catchments.read_id_levels()?;
    let catchment_ids = catchment_id_level_rows
        .iter()
        .map(|row| row.id())
        .collect::<Vec<_>>();
    let catchment_id_set: HashSet<UnitId> = catchment_ids.iter().copied().collect();
    let catchment_levels = catchment_id_level_rows
        .iter()
        .map(|row| (row.id(), row.level()))
        .collect::<HashMap<_, _>>();

    if graph.len() != catchment_ids.len() {
        return Err(SessionError::GraphReferentialIntegrity {
            reason: format!(
                "graph row count {} does not match catchment row count {}",
                graph.len(),
                catchment_ids.len()
            ),
        });
    }

    for row in graph.rows() {
        if !catchment_id_set.contains(&row.id()) {
            return Err(SessionError::GraphReferentialIntegrity {
                reason: format!(
                    "graph unit {} has no corresponding catchment row",
                    row.id().get()
                ),
            });
        }
        let Some(row_level) = catchment_levels.get(&row.id()).copied() else {
            return Err(SessionError::GraphReferentialIntegrity {
                reason: format!("graph unit {} has no catchment level", row.id().get()),
            });
        };
        if row.level() != row_level {
            return Err(SessionError::GraphReferentialIntegrity {
                reason: format!(
                    "graph unit {} level {} differs from catchment level {}",
                    row.id().get(),
                    row.level().get(),
                    row_level.get()
                ),
            });
        }
        for &upstream_id in row.upstream_ids() {
            if !catchment_id_set.contains(&upstream_id) {
                return Err(SessionError::GraphReferentialIntegrity {
                    reason: format!(
                        "graph unit {} references upstream unit {} which has no catchment row",
                        row.id().get(),
                        upstream_id.get()
                    ),
                });
            }
            let upstream_level = catchment_levels.get(&upstream_id).copied().ok_or_else(|| {
                SessionError::GraphReferentialIntegrity {
                    reason: format!("upstream unit {} has no catchment level", upstream_id.get()),
                }
            })?;
            if upstream_level != row.level() {
                return Err(SessionError::GraphReferentialIntegrity {
                    reason: format!(
                        "graph edge {} -> {} crosses levels {} -> {}",
                        upstream_id.get(),
                        row.id().get(),
                        upstream_level.get(),
                        row.level().get()
                    ),
                });
            }
        }
    }

    for &catchment_id in &catchment_ids {
        if graph.get(catchment_id).is_none() {
            return Err(SessionError::GraphReferentialIntegrity {
                reason: format!(
                    "catchment unit {} has no corresponding graph row",
                    catchment_id.get()
                ),
            });
        }
    }

    debug!(
        graph_units = graph.len(),
        catchment_units = catchment_ids.len(),
        "integrity checks passed"
    );

    Ok(catchment_levels)
}

/// Validate snap/catchment referential integrity.
///
/// Changing open-time referential validation semantics requires bumping
/// `validation_logic_version`.
fn validate_snap_refs(
    snap_store: &SnapStore,
    decl: &SnapDecl,
    catchment_levels: &HashMap<UnitId, Level>,
) -> Result<(), SessionError> {
    #[cfg(test)]
    record_snap_validation_scan_for_test();

    let snap_refs = snap_store.read_all_snap_refs()?;
    for snap_ref in &snap_refs {
        let unit_id = snap_ref.unit_id;
        let Some(level) = catchment_levels.get(&unit_id).copied() else {
            return Err(SessionError::SnapReferentialIntegrity {
                snap_id: snap_ref.snap_id.get(),
                unit_id: unit_id.get(),
                reason: "snap target references a unit with no catchment row".to_string(),
            });
        };
        if !decl.references_levels.contains(&level.get()) {
            return Err(SessionError::SnapReferentialIntegrity {
                snap_id: snap_ref.snap_id.get(),
                unit_id: unit_id.get(),
                reason: format!(
                    "unit level {} is not declared in references_levels {:?}",
                    level.get(),
                    decl.references_levels
                ),
            });
        }
    }
    debug!(
        snap_refs = snap_refs.len(),
        "snap unit_id integrity verified"
    );
    Ok(())
}

fn read_remote_artifact(
    store: &dyn ObjectStore,
    path: ObjectPath,
    artifact: &'static str,
) -> Result<bytes::Bytes, SessionError> {
    let path_display = path.as_ref().to_string();
    RT.block_on(async {
        let meta = store.head(&path).await.map_err(|source| {
            SessionError::remote_artifact_read(artifact, &path_display, source)
        })?;

        if meta.size > RANGE_GET_CHUNK_TARGET_BYTES {
            let concurrency = shed_get_ranges_concurrency();
            let ranges = remote_artifact_ranges(meta.size, concurrency);
            let chunks = futures_util::stream::iter(
                ranges
                    .into_iter()
                    .map(|range| store.get_range(&path, range)),
            )
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;
            let capacity = usize::try_from(meta.size).map_err(|_| {
                SessionError::integrity(format!(
                    "remote artifact {artifact} at {path_display} is too large to buffer"
                ))
            })?;
            let mut bytes = BytesMut::with_capacity(capacity);
            for chunk in chunks {
                let chunk = chunk.map_err(|source| {
                    SessionError::remote_artifact_read(artifact, &path_display, source)
                })?;
                bytes.extend_from_slice(&chunk);
            }
            return Ok(bytes.freeze());
        }

        let result = store.get(&path).await.map_err(|source| {
            SessionError::remote_artifact_read(artifact, &path_display, source)
        })?;

        result
            .bytes()
            .await
            .map_err(|source| SessionError::remote_artifact_read(artifact, path_display, source))
    })
}

fn remote_artifact_meta(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    artifact: &'static str,
) -> Option<ArtifactMeta> {
    let meta = RT
        .block_on(async { store.head(path).await })
        .map_err(|source| {
            debug!(
                artifact,
                path = %path,
                error = %source,
                "remote artifact metadata unavailable for validation sidecar token"
            );
            source
        });
    let Ok(meta) = meta else {
        return None;
    };
    ArtifactMeta::from_parts(artifact, meta.e_tag.as_deref(), meta.size)
}

fn remote_artifact_ranges(size: u64, concurrency: usize) -> Vec<Range<u64>> {
    let chunk_count = size.div_ceil(RANGE_GET_CHUNK_TARGET_BYTES);
    let range_count = chunk_count.min(concurrency as u64).max(1);
    let range_size = size.div_ceil(range_count);
    (0..range_count)
        .map(|index| {
            let start = index * range_size;
            let end = ((index + 1) * range_size).min(size);
            start..end
        })
        .collect()
}

fn validation_sidecar_matches(
    cache: &RemoteArtifactCache,
    fabric_name: &str,
    adapter_version: &str,
    inputs: &ValidationSidecarInputs,
) -> bool {
    let (Some(manifest), Some(graph), Some(catchments), Some(snaps)) = (
        inputs.manifest.as_ref(),
        inputs.graph.as_ref(),
        inputs.catchments.as_ref(),
        inputs.snaps.as_ref(),
    ) else {
        return false;
    };
    cache
        .read_validation_sidecar(fabric_name, adapter_version)
        .is_some_and(|sidecar| {
            sidecar.matches(
                &inputs.hfx_format_version,
                manifest,
                graph,
                catchments,
                snaps,
            )
        })
}

fn validation_sidecar_for_current_metadata(
    inputs: ValidationSidecarInputs,
) -> Option<ValidationSidecar> {
    Some(ValidationSidecar::current(
        inputs.hfx_format_version,
        inputs.manifest?,
        inputs.graph?,
        inputs.catchments?,
        inputs.snaps?,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::fmt;
    use std::future::Future;
    use std::io::Cursor;
    use std::ops::Range;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard};

    use arrow::array::{
        BinaryBuilder, Float32Builder, Int16Builder, Int64Array, Int64Builder, ListBuilder,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result,
    };
    use parquet::arrow::ArrowWriter;
    use tracing::field::{Field as TracingField, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};
    use tracing_core::span::Current;
    use url::Url;

    use super::{
        DatasetSession, read_remote_artifact, remote_artifact_path, remote_artifact_ranges,
    };
    use crate::error::SessionError;
    use crate::parquet_cache::{ParquetFooterCache, ParquetRowGroupCache};
    use crate::reader::catchment_store::{
        GEOMETRY_DECODE_TEST_LOCK, read_id_level_scan_count_for_test,
        reset_geometry_decode_counts_for_test, reset_read_id_level_scan_count_for_test,
    };
    use crate::runtime::RT;
    use crate::testutil::DatasetBuilder;
    use hfx_core::{BoundingBox, SnapId, UnitId};

    static CACHE_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Debug, Default)]
    struct StageNestingState {
        next_id: u64,
        stages_by_id: HashMap<u64, String>,
        stack: Vec<u64>,
        nested_index_in_validation: Vec<(String, String)>,
    }

    #[derive(Debug, Clone, Default)]
    struct StageNestingSubscriber {
        state: Arc<Mutex<StageNestingState>>,
    }

    struct StageFieldVisitor {
        stage: Option<String>,
    }

    impl Visit for StageFieldVisitor {
        fn record_str(&mut self, field: &TracingField, value: &str) {
            if field.name() == "stage" {
                self.stage = Some(value.to_owned());
            }
        }

        fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
            if field.name() == "stage" {
                self.stage = Some(format!("{value:?}"));
            }
        }
    }

    impl Subscriber for StageNestingSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, span: &Attributes<'_>) -> Id {
            let mut visitor = StageFieldVisitor { stage: None };
            span.record(&mut visitor);

            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.next_id += 1;
            let id = state.next_id;
            if let Some(stage) = visitor.stage {
                if matches!(stage.as_str(), "catchment_id_index" | "snap_id_index") {
                    let mut nested = Vec::new();
                    for parent_id in &state.stack {
                        if let Some(parent_stage) = state.stages_by_id.get(parent_id)
                            && matches!(
                                parent_stage.as_str(),
                                "validate_graph_catchments" | "validate_snap_refs"
                            )
                        {
                            nested.push((parent_stage.clone(), stage.clone()));
                        }
                    }
                    state.nested_index_in_validation.extend(nested);
                }
                state.stages_by_id.insert(id, stage);
            }
            Id::from_u64(id)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {}

        fn enter(&self, span: &Id) {
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .stack
                .push(span.into_u64());
        }

        fn exit(&self, span: &Id) {
            let popped = self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .stack
                .pop();
            assert_eq!(popped, Some(span.into_u64()));
        }

        fn current_span(&self) -> Current {
            Current::none()
        }
    }

    struct CacheEnv {
        _guard: MutexGuard<'static, ()>,
        previous: Option<OsString>,
    }

    impl CacheEnv {
        fn set(path: &Path) -> Self {
            let guard = CACHE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let previous = std::env::var_os("HFX_CACHE_DIR");
            // SAFETY: these tests serialize all HFX_CACHE_DIR mutations with
            // CACHE_ENV_LOCK and restore the prior value before unlocking.
            unsafe {
                std::env::set_var("HFX_CACHE_DIR", path);
            }
            Self {
                _guard: guard,
                previous,
            }
        }
    }

    impl Drop for CacheEnv {
        fn drop(&mut self) {
            // SAFETY: CACHE_ENV_LOCK is still held while the prior environment
            // value is restored.
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var("HFX_CACHE_DIR", value),
                    None => std::env::remove_var("HFX_CACHE_DIR"),
                }
            }
        }
    }

    #[derive(Debug, Default)]
    struct StoreCounters {
        head_calls: AtomicUsize,
        full_get_calls: AtomicUsize,
        ranged_get_calls: AtomicUsize,
        get_ranges_calls: AtomicUsize,
        last_ranges: Mutex<Vec<Range<u64>>>,
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

        fn full_get_calls(&self) -> usize {
            self.counters.full_get_calls.load(Ordering::SeqCst)
        }

        fn ranged_get_calls(&self) -> usize {
            self.counters.ranged_get_calls.load(Ordering::SeqCst)
        }

        fn get_ranges_calls(&self) -> usize {
            self.counters.get_ranges_calls.load(Ordering::SeqCst)
        }

        fn last_ranges(&self) -> Vec<Range<u64>> {
            self.counters
                .last_ranges
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
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
            } else if let Some(range) = &options.range {
                self.counters
                    .ranged_get_calls
                    .fetch_add(1, Ordering::SeqCst);
                if let GetRange::Bounded(range) = range {
                    self.counters
                        .last_ranges
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(range.clone());
                }
            } else {
                self.counters.full_get_calls.fetch_add(1, Ordering::SeqCst);
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
            *self
                .counters
                .last_ranges
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = ranges.to_vec();
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

    fn manifest_bytes_with_rasters(snap: bool, rasters: bool) -> String {
        let snap_paths = if snap { &["snap.parquet"][..] } else { &[] };
        manifest_bytes_with_snap_paths_and_rasters(snap_paths, rasters)
    }

    fn manifest_bytes_with_snap_paths_and_rasters(snap_paths: &[&str], rasters: bool) -> String {
        let mut manifest = serde_json::json!({
            "format_version": "0.2.1",
            "fabric_name": "testfabric",
            "crs": "EPSG:4326",
            "topology": "tree",
            "bbox": [-10.0, -5.0, 10.0, 5.0],
            "unit_count": 2,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "test-v1",
            "auxiliary": []
        });
        for (index, snap_path) in snap_paths.iter().enumerate() {
            manifest["auxiliary"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "schema": "hfx.aux.snap.v1",
                    "artifacts": { "snap": snap_path },
                    "metadata": {
                        "name": format!("test-snap-{index}"),
                        "description": "Synthetic snap targets.",
                        "references_levels": [0],
                        "weight_semantics": "higher is preferred"
                    }
                }));
        }
        if rasters {
            manifest["auxiliary"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "schema": "hfx.aux.d8_raster.v1",
                    "artifacts": { "flow_dir": "flow_dir.tif", "flow_acc": "flow_acc.tif" },
                    "metadata": { "flow_dir_encoding": "esri" }
                }));
        }
        manifest.to_string()
    }

    fn graph_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("level", DataType::Int16, false),
            Field::new(
                "upstream_ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
        ]));

        let id_arr = Int64Array::from(vec![1_i64, 2]);
        let level_arr = arrow::array::Int16Array::from(vec![0_i16, 0]);
        let mut list_builder = ListBuilder::new(Int64Builder::new());
        list_builder.append(true);
        list_builder.values().append_value(1);
        list_builder.append(true);
        let upstream_arr = list_builder.finish();
        let minx_arr = arrow::array::Float32Array::from(vec![1.0_f32, 2.0]);
        let miny_arr = arrow::array::Float32Array::from(vec![0.0_f32, 0.0]);
        let maxx_arr = arrow::array::Float32Array::from(vec![1.5_f32, 2.5]);
        let maxy_arr = arrow::array::Float32Array::from(vec![0.5_f32, 0.5]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_arr),
                Arc::new(level_arr),
                Arc::new(upstream_arr),
                Arc::new(minx_arr),
                Arc::new(miny_arr),
                Arc::new(maxx_arr),
                Arc::new(maxy_arr),
            ],
        )
        .unwrap();

        let cursor = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.into_inner().unwrap().into_inner()
    }

    fn minimal_wkb_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<u8> {
        let mut w = Vec::new();
        w.push(1u8);
        w.extend_from_slice(&3u32.to_le_bytes());
        w.extend_from_slice(&1u32.to_le_bytes());
        w.extend_from_slice(&5u32.to_le_bytes());
        for (x, y) in [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ] {
            w.extend_from_slice(&x.to_le_bytes());
            w.extend_from_slice(&y.to_le_bytes());
        }
        w
    }

    fn minimal_wkb_linestring(x1: f64, y1: f64, x2: f64, y2: f64) -> Vec<u8> {
        let mut w = Vec::new();
        w.push(1u8);
        w.extend_from_slice(&2u32.to_le_bytes());
        w.extend_from_slice(&2u32.to_le_bytes());
        for (x, y) in [(x1, y1), (x2, y2)] {
            w.extend_from_slice(&x.to_le_bytes());
            w.extend_from_slice(&y.to_le_bytes());
        }
        w
    }

    fn catchments_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("level", DataType::Int16, false),
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let mut id_b = Int64Builder::new();
        let mut level_b = Int16Builder::new();
        let mut area_b = Float32Builder::new();
        let mut up_area_b = Float32Builder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();
        for i in 1..=2_i64 {
            let minx = i as f32;
            let maxx = minx + 0.5;
            id_b.append_value(i);
            level_b.append_value(0);
            area_b.append_value(1.0);
            up_area_b.append_null();
            minx_b.append_value(minx);
            miny_b.append_value(0.0);
            maxx_b.append_value(maxx);
            maxy_b.append_value(0.5);
            geom_b.append_value(minimal_wkb_polygon(minx as f64, 0.0, maxx as f64, 0.5));
        }

        parquet_bytes(
            schema,
            vec![
                Arc::new(id_b.finish()),
                Arc::new(level_b.finish()),
                Arc::new(area_b.finish()),
                Arc::new(up_area_b.finish()),
                Arc::new(minx_b.finish()),
                Arc::new(miny_b.finish()),
                Arc::new(maxx_b.finish()),
                Arc::new(maxy_b.finish()),
                Arc::new(geom_b.finish()),
            ],
        )
    }

    fn snap_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("unit_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("stem_role", DataType::Utf8, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let mut id_b = Int64Builder::new();
        let mut unit_id_b = Int64Builder::new();
        let mut weight_b = Float32Builder::new();
        let mut stem_role_b = arrow::array::StringBuilder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();
        id_b.append_value(1);
        unit_id_b.append_value(1);
        weight_b.append_value(1.0);
        stem_role_b.append_value("mainstem");
        minx_b.append_value(1.1);
        miny_b.append_value(0.1);
        maxx_b.append_value(1.4);
        maxy_b.append_value(0.4);
        geom_b.append_value(minimal_wkb_linestring(1.1, 0.25, 1.4, 0.25));

        parquet_bytes(
            schema,
            vec![
                Arc::new(id_b.finish()),
                Arc::new(unit_id_b.finish()),
                Arc::new(weight_b.finish()),
                Arc::new(stem_role_b.finish()),
                Arc::new(minx_b.finish()),
                Arc::new(miny_b.finish()),
                Arc::new(maxx_b.finish()),
                Arc::new(maxy_b.finish()),
                Arc::new(geom_b.finish()),
            ],
        )
    }

    fn parquet_bytes(schema: Arc<Schema>, columns: Vec<Arc<dyn arrow::array::Array>>) -> Vec<u8> {
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let cursor = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.into_inner().unwrap().into_inner()
    }

    #[test]
    fn remote_artifact_ranges_cover_object_with_configured_parallelism() {
        let ranges = remote_artifact_ranges(10 * 1024 * 1024, 2);

        assert_eq!(ranges, vec![0..5_242_880, 5_242_880..10_485_760]);
    }

    #[test]
    fn read_remote_artifact_uses_single_get_for_small_objects() {
        let path = ObjectPath::from("dataset/root/manifest.json");
        let base_store = Arc::new(InMemory::new());
        let payload = b"{\"format_version\":\"0.1\"}".to_vec();
        RT.block_on(async {
            base_store
                .put(&path, PutPayload::from(payload.clone()))
                .await
                .unwrap();
        });
        let counting_store = CountingStore::new(base_store);

        let bytes = read_remote_artifact(&counting_store, path, "manifest.json").unwrap();

        assert_eq!(bytes.as_ref(), payload.as_slice());
        assert_eq!(counting_store.head_calls(), 1);
        assert_eq!(counting_store.full_get_calls(), 1);
        assert_eq!(counting_store.ranged_get_calls(), 0);
        assert_eq!(counting_store.get_ranges_calls(), 0);
    }

    #[test]
    fn read_remote_artifact_uses_ranges_for_large_objects() {
        let path = ObjectPath::from("dataset/root/graph.parquet");
        let base_store = Arc::new(InMemory::new());
        let payload = (0..10 * 1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        RT.block_on(async {
            base_store
                .put(&path, PutPayload::from(payload.clone()))
                .await
                .unwrap();
        });
        let counting_store = CountingStore::new(base_store);

        let bytes = read_remote_artifact(&counting_store, path, "graph.parquet").unwrap();

        assert_eq!(bytes.as_ref(), payload.as_slice());
        assert_eq!(counting_store.head_calls(), 1);
        assert_eq!(counting_store.full_get_calls(), 0);
        assert_eq!(counting_store.get_ranges_calls(), 0);
        assert!(counting_store.ranged_get_calls() > 1);
        let ranges = counting_store.last_ranges();
        assert!(ranges.len() > 1);
        assert!(ranges.len() <= 16);
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, payload.len() as u64);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
    }

    #[test]
    fn remote_artifact_path_preserves_nested_relative_paths() {
        let root = ObjectPath::from("grit/2.0.0");

        let path = remote_artifact_path(&root, "aux/snap_segments.parquet");

        assert_eq!(path.as_ref(), "grit/2.0.0/aux/snap_segments.parquet");
    }

    fn put_remote_manifest_and_graph(store: &Arc<InMemory>, root: &ObjectPath, snap: bool) {
        put_remote_manifest_graph_with_rasters(store, root, snap, false);
    }

    fn put_remote_manifest_and_graph_with_snap_paths(
        store: &Arc<InMemory>,
        root: &ObjectPath,
        snap_paths: &[&str],
    ) {
        RT.block_on(async {
            store
                .put(
                    &root.clone().join("manifest.json"),
                    PutPayload::from(manifest_bytes_with_snap_paths_and_rasters(
                        snap_paths, false,
                    )),
                )
                .await
                .unwrap();
            store
                .put(
                    &root.clone().join("graph.parquet"),
                    PutPayload::from(graph_bytes()),
                )
                .await
                .unwrap();
        });
    }

    fn put_remote_manifest_graph_with_rasters(
        store: &Arc<InMemory>,
        root: &ObjectPath,
        snap: bool,
        rasters: bool,
    ) {
        RT.block_on(async {
            store
                .put(
                    &root.clone().join("manifest.json"),
                    PutPayload::from(manifest_bytes_with_rasters(snap, rasters)),
                )
                .await
                .unwrap();
            store
                .put(
                    &root.clone().join("graph.parquet"),
                    PutPayload::from(graph_bytes()),
                )
                .await
                .unwrap();
        });
    }

    #[test]
    fn open_remote_fetches_manifest_graph_and_opens_catchments() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&store, &root, false);
        put_remote_catchments(&store, &root);
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let session = DatasetSession::open_remote(store, &root, &url, None).unwrap();

        assert_eq!(session.catchments().total_rows(), 2);
        let bbox = BoundingBox::new(0.75, 0.0, 1.75, 1.0).unwrap();
        assert_eq!(session.catchments().query_by_bbox(&bbox).unwrap().len(), 1);
        assert!(
            cache_dir
                .path()
                .join("testfabric")
                .join("test-v1")
                .join("manifest.json")
                .is_file()
        );
        assert!(
            cache_dir
                .path()
                .join("testfabric")
                .join("test-v1")
                .join("graph.parquet")
                .is_file()
        );
        assert!(
            cache_dir
                .path()
                .join("testfabric")
                .join("test-v1")
                .join("catchments.idindex.arrow")
                .is_file()
        );
        assert!(
            cache_dir
                .path()
                .join("testfabric")
                .join("test-v1")
                .join("validated.json")
                .is_file()
        );
    }

    #[test]
    fn local_open_does_not_allocate_parquet_caches() {
        let (_dir, root) = DatasetBuilder::new(2).build();
        let session = DatasetSession::open(root.to_str().unwrap()).unwrap();

        assert!(session.parquet_cache.is_none());
        assert!(session.footer_cache.is_none());
    }

    #[test]
    fn remote_default_open_enables_parquet_caches() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&store, &root, false);
        put_remote_catchments(&store, &root);
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let session =
            DatasetSession::open_remote_with_default_caches(store, &root, &url, None).unwrap();

        assert!(session.parquet_cache.is_some());
        assert!(session.footer_cache.is_some());
    }

    #[test]
    fn explicit_none_caches_disable_remote_parquet_caches() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&store, &root, false);
        put_remote_catchments(&store, &root);
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let session =
            DatasetSession::open_remote_with_stats(store, &root, &url, None, None, None).unwrap();

        assert!(session.parquet_cache.is_none());
        assert!(session.footer_cache.is_none());
    }

    #[test]
    fn open_does_not_decode_catchment_geometry_during_validation() {
        let _decode_guard = GEOMETRY_DECODE_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        reset_geometry_decode_counts_for_test();
        let (_dir, root) = DatasetBuilder::new(2).build();

        let session = DatasetSession::open_path(&root).unwrap();

        let counts = [1_i64, 2]
            .into_iter()
            .map(|id| {
                session
                    .catchments()
                    .geometry_decode_count_for_test(UnitId::new(id).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            counts,
            vec![0, 0],
            "open validation should not decode full catchment geometry"
        );
    }

    #[test]
    fn repeated_remote_query_with_cache_avoids_second_range_reads() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let base_store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&base_store, &root, false);
        put_remote_catchments(&base_store, &root);
        let counting_store = Arc::new(CountingStore::new(base_store));
        let object_store = Arc::clone(&counting_store) as Arc<dyn ObjectStore>;
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();
        let row_group_cache = Some(ParquetRowGroupCache::new(1024 * 1024));
        let footer_cache = Some(ParquetFooterCache::new());
        let session = DatasetSession::open_remote_with_stats(
            object_store,
            &root,
            &url,
            row_group_cache,
            footer_cache,
            None,
        )
        .unwrap();
        let bbox = BoundingBox::new(0.75, 0.0, 1.75, 1.0).unwrap();

        assert_eq!(session.catchments().query_by_bbox(&bbox).unwrap().len(), 1);
        let ranged_after_first = counting_store.ranged_get_calls();
        let get_ranges_after_first = counting_store.get_ranges_calls();
        assert_eq!(session.catchments().query_by_bbox(&bbox).unwrap().len(), 1);

        assert_eq!(counting_store.ranged_get_calls(), ranged_after_first);
        assert_eq!(counting_store.get_ranges_calls(), get_ranges_after_first);
    }

    #[test]
    fn open_remote_uses_cached_manifest_and_graph_when_remote_is_empty() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&store, &root, false);
        put_remote_catchments(&store, &root);
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        DatasetSession::open_remote(store, &root, &url, None).unwrap();

        let catchments_only_store = Arc::new(InMemory::new());
        put_remote_catchments(&catchments_only_store, &root);
        let session =
            DatasetSession::open_remote(catchments_only_store, &root, &url, None).unwrap();

        assert_eq!(session.graph().len(), 2);
        assert_eq!(session.catchments().total_rows(), 2);
    }

    #[test]
    fn open_remote_does_not_use_cache_entry_from_different_source() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let root_a = ObjectPath::from("dataset/a");
        let url_a = Url::parse("s3://shed-test/dataset/a").unwrap();
        let store_a = Arc::new(InMemory::new());
        put_remote_manifest_and_graph(&store_a, &root_a, false);
        put_remote_catchments(&store_a, &root_a);

        DatasetSession::open_remote(store_a, &root_a, &url_a, None).unwrap();

        let root_b = ObjectPath::from("dataset/b");
        let url_b = Url::parse("s3://shed-test/dataset/b").unwrap();
        let empty_store = Arc::new(InMemory::new());
        let unmapped_err =
            DatasetSession::open_remote(empty_store, &root_b, &url_b, None).unwrap_err();
        assert!(matches!(
            unmapped_err,
            SessionError::RemoteArtifactRead {
                artifact: "manifest.json",
                ..
            }
        ));

        let store_b = Arc::new(InMemory::new());
        put_remote_manifest_and_graph(&store_b, &root_b, false);
        put_remote_catchments(&store_b, &root_b);
        DatasetSession::open_remote(store_b, &root_b, &url_b, None).unwrap();

        let catchments_only_store = Arc::new(InMemory::new());
        put_remote_catchments(&catchments_only_store, &root_b);
        DatasetSession::open_remote(catchments_only_store, &root_b, &url_b, None).unwrap();
    }

    #[test]
    fn open_remote_reports_missing_manifest() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let err = DatasetSession::open_remote(store, &root, &url, None).unwrap_err();

        assert!(matches!(
            err,
            SessionError::RemoteArtifactRead {
                artifact: "manifest.json",
                ..
            }
        ));
    }

    #[test]
    fn open_remote_with_snap_opens_and_queries_snap_store() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&store, &root, true);
        put_remote_catchments(&store, &root);
        RT.block_on(async {
            store
                .put(
                    &root.clone().join("snap.parquet"),
                    PutPayload::from(snap_bytes()),
                )
                .await
                .unwrap();
        });
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let session = DatasetSession::open_remote(store, &root, &url, None).unwrap();
        let snap = session.snap().expect("snap store should be present");
        let bbox = BoundingBox::new(1.0, 0.0, 1.5, 0.5).unwrap();
        let results = snap.query_by_bbox(&bbox).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id(), SnapId::new(1).unwrap());
    }

    fn put_remote_catchments(store: &Arc<InMemory>, root: &ObjectPath) {
        RT.block_on(async {
            store
                .put(
                    &root.clone().join("catchments.parquet"),
                    PutPayload::from(catchments_bytes()),
                )
                .await
                .unwrap();
        });
    }

    fn put_remote_snaps(store: &Arc<InMemory>, root: &ObjectPath, snap_paths: &[&str]) {
        RT.block_on(async {
            for snap_path in snap_paths {
                let object_path = ObjectPath::from(format!("{}/{}", root.as_ref(), snap_path));
                store
                    .put(&object_path, PutPayload::from(snap_bytes()))
                    .await
                    .unwrap();
            }
        });
    }

    #[test]
    fn second_remote_open_uses_persistent_indexes_and_validation_sidecar() {
        let _decode_guard = GEOMETRY_DECODE_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let base_store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&base_store, &root, true);
        put_remote_catchments(&base_store, &root);
        RT.block_on(async {
            base_store
                .put(
                    &root.clone().join("snap.parquet"),
                    PutPayload::from(snap_bytes()),
                )
                .await
                .unwrap();
        });
        let counting_store = Arc::new(CountingStore::new(base_store));
        let object_store = Arc::clone(&counting_store) as Arc<dyn ObjectStore>;
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        DatasetSession::open_remote(object_store.clone(), &root, &url, None).unwrap();
        let first_ranged_gets = counting_store.ranged_get_calls();
        assert!(
            cache_dir
                .path()
                .join("testfabric")
                .join("test-v1")
                .join("catchments.idindex.arrow")
                .is_file()
        );
        assert!(
            cache_dir
                .path()
                .join("testfabric")
                .join("test-v1")
                .join("snap.idindex.arrow")
                .is_file()
        );
        assert!(
            cache_dir
                .path()
                .join("testfabric")
                .join("test-v1")
                .join("validated.json")
                .is_file()
        );

        reset_geometry_decode_counts_for_test();
        reset_read_id_level_scan_count_for_test();
        let second_session = DatasetSession::open_remote(object_store, &root, &url, None).unwrap();
        let second_ranged_gets = counting_store.ranged_get_calls() - first_ranged_gets;
        let read_id_level_scans = read_id_level_scan_count_for_test();
        let counts = [1_i64, 2]
            .into_iter()
            .map(|id| {
                second_session
                    .catchments()
                    .geometry_decode_count_for_test(UnitId::new(id).unwrap())
            })
            .collect::<Vec<_>>();

        assert!(
            second_ranged_gets < first_ranged_gets,
            "second open should avoid parquet ID scans after persistent index hits"
        );
        assert_eq!(
            counts,
            vec![0, 0],
            "sidecar-hit open should not decode full catchment geometry"
        );
        assert_eq!(
            read_id_level_scans, 0,
            "sidecar-hit open should not scan catchment id/level rows"
        );
    }

    #[test]
    fn second_remote_open_with_two_snaps_uses_validation_sidecar() {
        let _decode_guard = GEOMETRY_DECODE_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let base_store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        let snap_paths = ["aux/snap-a.parquet", "aux/snap-b.parquet"];
        put_remote_manifest_and_graph_with_snap_paths(&base_store, &root, &snap_paths);
        put_remote_catchments(&base_store, &root);
        put_remote_snaps(&base_store, &root, &snap_paths);
        let object_store = Arc::clone(&base_store) as Arc<dyn ObjectStore>;
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        DatasetSession::open_remote(object_store.clone(), &root, &url, None).unwrap();

        reset_read_id_level_scan_count_for_test();
        super::reset_snap_validation_scan_count_for_test();
        DatasetSession::open_remote(object_store, &root, &url, None).unwrap();

        assert_eq!(
            read_id_level_scan_count_for_test(),
            0,
            "valid two-snap token should skip catchment id/level validation"
        );
        assert_eq!(
            super::snap_validation_scan_count_for_test(),
            0,
            "valid two-snap token should skip snap validation scans"
        );

        let sidecar_path = cache_dir
            .path()
            .join("testfabric")
            .join("test-v1")
            .join("validated.json");
        let sidecar: serde_json::Value =
            serde_json::from_slice(&std::fs::read(sidecar_path).unwrap()).unwrap();
        let snaps = sidecar["snaps"].as_array().unwrap();
        let snap_paths_in_token = snaps
            .iter()
            .map(|snap| snap["path"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(snap_paths_in_token, snap_paths);
    }

    #[test]
    fn remote_open_does_not_nest_id_index_stages_inside_validation_stages() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let base_store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&base_store, &root, true);
        put_remote_catchments(&base_store, &root);
        RT.block_on(async {
            base_store
                .put(
                    &root.clone().join("snap.parquet"),
                    PutPayload::from(snap_bytes()),
                )
                .await
                .unwrap();
        });
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let subscriber = StageNestingSubscriber::default();
        let state = subscriber.state.clone();
        let dispatch = tracing::Dispatch::new(subscriber);

        tracing::dispatcher::with_default(&dispatch, || {
            DatasetSession::open_remote(base_store.clone(), &root, &url, None).unwrap();
            DatasetSession::open_remote(base_store, &root, &url, None).unwrap();
        });

        let nested = state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .nested_index_in_validation
            .clone();
        assert!(
            nested.is_empty(),
            "ID-index stages must not be nested inside validation stages: {nested:?}"
        );
    }
}
