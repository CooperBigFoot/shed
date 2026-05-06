//! Dataset session — loads an HFX dataset for repeated queries.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use geo::Rect;
use hfx_core::{AtomId, DrainageGraph, Manifest, RasterAvailability, SnapAvailability, Topology};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{debug, info, instrument};
use url::Url;

use crate::cache::RemoteArtifactCache;
use crate::cog::{LocalizedRasterWindow, RasterWindowRequest};
use crate::error::SessionError;
use crate::parquet_cache::ParquetRowGroupCache;
use crate::raster_cache::RemoteRasterCache;
use crate::reader;
use crate::reader::catchment_store::CatchmentStore;
use crate::reader::snap_store::SnapStore;
use crate::runtime::RT;
use crate::source::DatasetSource;
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
    pub(crate) fn artifact(self) -> &'static str {
        match self {
            Self::FlowDir => "flow_dir.tif",
            Self::FlowAcc => "flow_acc.tif",
        }
    }

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
    graph: hfx_core::DrainageGraph,
    catchments: CatchmentStore,
    snap: Option<SnapStore>,
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
        Self::open_with_cache(input, None)
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
        match DatasetSource::parse(input)? {
            DatasetSource::Local(root) => Self::open_path(&root),
            DatasetSource::Remote {
                store,
                http_stats,
                root,
                url,
            } => Self::open_remote_with_stats(store, &root, &url, cache, http_stats),
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
    /// | [`SessionError::AtomCountMismatch`] | Row count in catchments.parquet differs from manifest |
    /// | Manifest/graph/Parquet errors | Propagated from sub-readers |
    #[instrument(skip_all, fields(root = %root.display()))]
    pub fn open_path(root: &Path) -> Result<Self, SessionError> {
        if !root.is_dir() {
            return Err(SessionError::RootNotFound {
                path: root.display().to_string(),
            });
        }

        for artifact in ["manifest.json", "graph.arrow", "catchments.parquet"] {
            let p = root.join(artifact);
            if !p.exists() {
                return Err(SessionError::required_missing(
                    artifact,
                    p.display().to_string(),
                ));
            }
        }

        let manifest = reader::manifest::read_manifest(&root.join("manifest.json"))?;

        if manifest.snap() == SnapAvailability::Present {
            let p = root.join("snap.parquet");
            if !p.exists() {
                return Err(SessionError::optional_missing(
                    "snap.parquet",
                    p.display().to_string(),
                ));
            }
        }

        if matches!(manifest.rasters(), RasterAvailability::Present(_)) {
            let p = root.join("flow_dir.tif");
            if !p.exists() {
                return Err(SessionError::optional_missing(
                    "flow_dir.tif",
                    p.display().to_string(),
                ));
            }
            let p = root.join("flow_acc.tif");
            if !p.exists() {
                return Err(SessionError::optional_missing(
                    "flow_acc.tif",
                    p.display().to_string(),
                ));
            }
        }

        let graph = reader::graph::load_graph(&root.join("graph.arrow"))?;

        let catchments = CatchmentStore::open(&root.join("catchments.parquet"))?;

        let catchment_id_set = {
            let _guard = StageGuard::enter(Stage::ValidateGraphCatchments);
            validate_graph_catchments(&manifest, &graph, &catchments)?
        };

        let snap = if manifest.snap() == SnapAvailability::Present {
            Some(SnapStore::open(&root.join("snap.parquet"))?)
        } else {
            None
        };

        // If snap is present, verify all snap catchment_id references exist
        if let Some(ref snap_store) = snap {
            let _guard = StageGuard::enter(Stage::ValidateSnapRefs);
            validate_snap_refs(snap_store, &catchment_id_set)?;
        }

        let raster_paths = if matches!(manifest.rasters(), RasterAvailability::Present(_)) {
            Some(RasterPaths {
                flow_dir: raster_uri_string(&root.join("flow_dir.tif")),
                flow_acc: raster_uri_string(&root.join("flow_acc.tif")),
            })
        } else {
            None
        };

        info!(
            fabric = manifest.fabric_name(),
            atoms = manifest.atom_count().get(),
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
            graph,
            catchments,
            snap,
            raster_paths,
            raster_cache: None,
            remote_store: None,
            remote_root: None,
            http_stats: None,
            parquet_cache: None,
        })
    }

    fn open_remote(
        store: Arc<dyn ObjectStore>,
        root: &ObjectPath,
        url: &Url,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
    ) -> Result<Self, SessionError> {
        Self::open_remote_with_stats(store, root, url, parquet_cache, None)
    }

    fn open_remote_with_stats(
        store: Arc<dyn ObjectStore>,
        root: &ObjectPath,
        url: &Url,
        parquet_cache: Option<Arc<ParquetRowGroupCache>>,
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
        let (manifest, graph) = if let Some(cached) = cache.read_entry_for_source(url, root)? {
            debug!(
                fabric = cached.manifest.fabric_name(),
                atoms = cached.manifest.atom_count().get(),
                graph_atoms = cached.graph.len(),
                "remote manifest and graph parsed from cache"
            );
            (cached.manifest, cached.graph)
        } else {
            let manifest_path = remote_artifact_path(root, "manifest.json");
            let graph_path = remote_artifact_path(root, "graph.arrow");

            let t = std::time::Instant::now();
            let manifest_bytes = {
                let _guard = StageGuard::enter(Stage::ManifestFetch);
                record_path(manifest_path.as_ref());
                let bytes = read_remote_artifact(store.as_ref(), manifest_path, "manifest.json")?;
                record_bytes(bytes.len() as u64);
                bytes
            };
            info!(
                bytes = manifest_bytes.len(),
                duration_ms = t.elapsed().as_millis(),
                "fetched manifest"
            );
            let manifest = reader::manifest::read_manifest_from_bytes(&manifest_bytes)?;

            let t = std::time::Instant::now();
            let (graph_bytes, graph) = {
                let _guard = StageGuard::enter(Stage::GraphFetch);
                record_path(graph_path.as_ref());
                let bytes = read_remote_artifact(store.as_ref(), graph_path, "graph.arrow")?;
                record_bytes(bytes.len() as u64);
                let graph = reader::graph::load_graph_from_bytes(bytes.clone())?;
                (bytes, graph)
            };
            info!(
                bytes = graph_bytes.len(),
                atoms = graph.len(),
                duration_ms = t.elapsed().as_millis(),
                "fetched graph"
            );
            cache.write_manifest_graph(url, root, &manifest, &manifest_bytes, &graph_bytes)?;

            debug!(
                fabric = manifest.fabric_name(),
                atoms = manifest.atom_count().get(),
                graph_atoms = graph.len(),
                "remote manifest and graph parsed"
            );
            (manifest, graph)
        };

        let fabric_name = manifest.fabric_name().to_string();
        let adapter_version = manifest.adapter_version().to_string();

        let catchments_path = remote_artifact_path(root, "catchments.parquet");
        let catchments = CatchmentStore::open_remote_with_cache(
            store.clone(),
            catchments_path.clone(),
            catchments_path.as_ref().to_string(),
            fabric_name.clone(),
            adapter_version.clone(),
            parquet_cache.clone(),
        )?;
        let t = std::time::Instant::now();
        let catchment_id_set = {
            let _guard = StageGuard::enter(Stage::ValidateGraphCatchments);
            validate_graph_catchments(&manifest, &graph, &catchments)?
        };
        info!(
            rows = catchment_id_set.len(),
            duration_ms = t.elapsed().as_millis(),
            "indexed catchments"
        );

        let snap = if manifest.snap() == SnapAvailability::Present {
            let snap_path = remote_artifact_path(root, "snap.parquet");
            Some(SnapStore::open_remote_with_cache(
                store.clone(),
                snap_path.clone(),
                snap_path.as_ref().to_string(),
                fabric_name.clone(),
                adapter_version.clone(),
                parquet_cache.clone(),
            )?)
        } else {
            None
        };
        if let Some(ref snap_store) = snap {
            let _guard = StageGuard::enter(Stage::ValidateSnapRefs);
            validate_snap_refs(snap_store, &catchment_id_set)?;
        }

        let raster_paths = if matches!(manifest.rasters(), RasterAvailability::Present(_)) {
            Some(RasterPaths {
                flow_dir: remote_artifact_url_string(url, "flow_dir.tif"),
                flow_acc: remote_artifact_url_string(url, "flow_acc.tif"),
            })
        } else {
            None
        };

        info!(
            fabric = manifest.fabric_name(),
            atoms = manifest.atom_count().get(),
            topology = %manifest.topology(),
            elapsed_ms = session_start.elapsed().as_millis(),
            "remote dataset session opened"
        );

        Ok(DatasetSession {
            root: PathBuf::from(url.as_str()),
            fabric_cache_key: (fabric_name, adapter_version),
            manifest,
            graph,
            catchments,
            snap,
            raster_paths,
            raster_cache: Some(raster_cache),
            remote_store: Some(store),
            remote_root: Some(root.clone()),
            http_stats,
            parquet_cache,
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

    /// Return the graph topology declared in the manifest.
    pub fn topology(&self) -> Topology {
        self.manifest.topology()
    }

    /// Return a reference to the in-memory drainage graph.
    pub fn graph(&self) -> &hfx_core::DrainageGraph {
        &self.graph
    }

    /// Return a reference to the catchment store for on-demand queries.
    pub fn catchments(&self) -> &CatchmentStore {
        &self.catchments
    }

    /// Return a reference to the snap store, if present.
    pub fn snap(&self) -> Option<&SnapStore> {
        self.snap.as_ref()
    }

    /// Return the validated raster paths, if rasters are present.
    pub fn raster_paths(&self) -> Option<&RasterPaths> {
        self.raster_paths.as_ref()
    }

    /// Return object-store request counters when network benchmarking is enabled.
    pub fn http_stats(&self) -> Option<HttpStatsSnapshot> {
        self.http_stats.as_ref().map(HttpStatsHandle::snapshot)
    }

    /// Return a local filesystem path for a raster window needed by refinement.
    ///
    /// Local sessions return the full raster path because GDAL already performs
    /// local windowed reads. Remote sessions read only the intersecting COG byte
    /// ranges and materialize a small cache-local GeoTIFF.
    pub(crate) fn localize_raster_window(
        &self,
        kind: RasterKind,
        bbox: Rect<f64>,
    ) -> Result<LocalizedRasterWindow, SessionError> {
        let raster_paths = self.raster_paths.as_ref().ok_or_else(|| {
            SessionError::integrity(
                "raster localization requested but manifest declares no rasters",
            )
        })?;

        if let (Some(cache), Some(store), Some(root)) = (
            self.raster_cache.as_ref(),
            self.remote_store.as_ref(),
            self.remote_root.as_ref(),
        ) {
            let remote_path = remote_artifact_path(root, kind.artifact());
            let request = RasterWindowRequest::new(kind, bbox);
            let (fabric_name, adapter_version) = &self.fabric_cache_key;
            return RT
                .block_on(cache.get_or_fetch_window(
                    store.as_ref(),
                    &remote_path,
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

        let path = match kind {
            RasterKind::FlowDir => raster_paths.flow_dir().to_path_buf(),
            RasterKind::FlowAcc => raster_paths.flow_acc().to_path_buf(),
        };
        Ok(LocalizedRasterWindow::cached(path))
    }

    /// Return the dataset root directory path.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn remote_artifact_path(root: &ObjectPath, artifact: &'static str) -> ObjectPath {
    root.clone().join(artifact)
}

fn raster_uri_string(path: &Path) -> String {
    path.display().to_string()
}

fn remote_artifact_url_string(url: &Url, artifact: &'static str) -> String {
    format!("{}/{}", url.as_str().trim_end_matches('/'), artifact)
}

fn validate_graph_catchments(
    manifest: &Manifest,
    graph: &DrainageGraph,
    catchments: &CatchmentStore,
) -> Result<std::collections::HashSet<AtomId>, SessionError> {
    let expected = manifest.atom_count().get();
    let actual = catchments.total_rows();
    if expected != actual {
        return Err(SessionError::AtomCountMismatch {
            manifest_count: expected,
            actual_count: actual,
        });
    }

    debug!("verifying graph ↔ catchment referential integrity");
    let catchment_ids = catchments.read_all_ids()?;
    let catchment_id_set: std::collections::HashSet<AtomId> =
        catchment_ids.iter().copied().collect();

    for row in graph.rows() {
        if !catchment_id_set.contains(&row.id()) {
            return Err(SessionError::integrity(format!(
                "graph atom {} has no corresponding catchment row",
                row.id().get(),
            )));
        }
        for &upstream_id in row.upstream_ids() {
            if !catchment_id_set.contains(&upstream_id) {
                return Err(SessionError::integrity(format!(
                    "graph atom {} references upstream atom {} which has no catchment row",
                    row.id().get(),
                    upstream_id.get(),
                )));
            }
        }
    }

    for &catchment_id in &catchment_ids {
        if graph.get(catchment_id).is_none() {
            return Err(SessionError::integrity(format!(
                "catchment atom {} has no corresponding graph row",
                catchment_id.get(),
            )));
        }
    }

    debug!(
        graph_atoms = graph.len(),
        catchment_atoms = catchment_ids.len(),
        "integrity checks passed"
    );

    Ok(catchment_id_set)
}

fn validate_snap_refs(
    snap_store: &SnapStore,
    catchment_id_set: &std::collections::HashSet<AtomId>,
) -> Result<(), SessionError> {
    let snap_catchment_ids = snap_store.read_all_catchment_ids()?;
    for &snap_cid in &snap_catchment_ids {
        if !catchment_id_set.contains(&snap_cid) {
            return Err(SessionError::integrity(format!(
                "snap target references catchment {} which has no catchment row",
                snap_cid.get(),
            )));
        }
    }
    debug!(
        snap_refs = snap_catchment_ids.len(),
        "snap catchment_id integrity verified"
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
        let result = store.get(&path).await.map_err(|source| {
            SessionError::remote_artifact_read(artifact, &path_display, source)
        })?;

        result
            .bytes()
            .await
            .map_err(|source| SessionError::remote_artifact_read(artifact, path_display, source))
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::{Arc, Mutex, MutexGuard};

    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Float32Builder, Int64Array, Int64Builder, ListBuilder,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt, PutPayload};
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use super::DatasetSession;
    use crate::error::SessionError;
    use crate::runtime::RT;
    use hfx_core::{BoundingBox, SnapId};

    static CACHE_ENV_LOCK: Mutex<()> = Mutex::new(());

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

    fn manifest_bytes_with_rasters(snap: bool, rasters: bool) -> String {
        let mut manifest = serde_json::json!({
            "format_version": "0.1",
            "fabric_name": "testfabric",
            "crs": "EPSG:4326",
            "topology": "tree",
            "terminal_sink_id": 0,
            "bbox": [-10.0, -5.0, 10.0, 5.0],
            "atom_count": 2,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "test-v1"
        });
        if snap {
            manifest["has_snap"] = serde_json::json!(true);
        }
        if rasters {
            manifest["has_rasters"] = serde_json::json!(true);
            manifest["flow_dir_encoding"] = serde_json::json!("esri");
        }
        manifest.to_string()
    }

    fn graph_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "upstream_ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]));

        let id_arr = Int64Array::from(vec![1_i64, 2]);
        let mut list_builder = ListBuilder::new(Int64Builder::new());
        list_builder.append(true);
        list_builder.values().append_value(1);
        list_builder.append(true);
        let upstream_arr = list_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_arr), Arc::new(upstream_arr)],
        )
        .unwrap();

        let cursor = Cursor::new(Vec::new());
        let mut writer = FileWriter::try_new(cursor, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
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
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let mut id_b = Int64Builder::new();
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
        let mut catchment_id_b = Int64Builder::new();
        let mut weight_b = Float32Builder::new();
        let mut mainstem_b = BooleanBuilder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();
        id_b.append_value(1);
        catchment_id_b.append_value(1);
        weight_b.append_value(1.0);
        mainstem_b.append_value(true);
        minx_b.append_value(1.1);
        miny_b.append_value(0.1);
        maxx_b.append_value(1.4);
        maxy_b.append_value(0.4);
        geom_b.append_value(minimal_wkb_linestring(1.1, 0.25, 1.4, 0.25));

        parquet_bytes(
            schema,
            vec![
                Arc::new(id_b.finish()),
                Arc::new(catchment_id_b.finish()),
                Arc::new(weight_b.finish()),
                Arc::new(mainstem_b.finish()),
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

    fn put_remote_manifest_and_graph(store: &Arc<InMemory>, root: &ObjectPath, snap: bool) {
        put_remote_manifest_graph_with_rasters(store, root, snap, false);
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
                    &root.clone().join("graph.arrow"),
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
                .join("graph.arrow")
                .is_file()
        );
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
}
