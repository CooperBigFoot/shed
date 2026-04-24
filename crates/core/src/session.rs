//! Dataset session — loads an HFX dataset for repeated queries.

use std::path::{Path, PathBuf};

use hfx_core::{Manifest, RasterAvailability, SnapAvailability, Topology};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{debug, info, instrument};
use url::Url;

use crate::cache::RemoteArtifactCache;
use crate::error::SessionError;
use crate::reader;
use crate::reader::catchment_store::CatchmentStore;
use crate::reader::snap_store::SnapStore;
use crate::runtime::RT;
use crate::source::DatasetSource;

/// Validated paths to the optional raster pair.
///
/// Stores paths only — no reading, no GDAL.
#[derive(Debug, Clone)]
pub struct RasterPaths {
    flow_dir: PathBuf,
    flow_acc: PathBuf,
}

impl RasterPaths {
    /// Return the path to the flow direction raster.
    pub fn flow_dir(&self) -> &Path {
        &self.flow_dir
    }

    /// Return the path to the flow accumulation raster.
    pub fn flow_acc(&self) -> &Path {
        &self.flow_acc
    }
}

/// A loaded HFX dataset, ready for repeated queries.
///
/// Created via [`DatasetSession::open`]. Holds the manifest and drainage
/// graph in memory. Catchment and snap data are read on demand via
/// row-group bbox pruning.
#[derive(Debug)]
pub struct DatasetSession {
    root: PathBuf,
    manifest: Manifest,
    graph: hfx_core::DrainageGraph,
    catchments: CatchmentStore,
    snap: Option<SnapStore>,
    raster_paths: Option<RasterPaths>,
}

impl DatasetSession {
    /// Open an HFX dataset source and return a ready-to-query session.
    ///
    /// Local paths and `file://` URLs are opened from the local filesystem.
    /// Remote sources are parsed but not yet readable in this phase.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | Source parsing errors | The dataset source string is malformed or unsupported |
    /// | [`SessionError::RemoteDatasetNotSupported`] | The source is remote |
    /// | Local session errors | Propagated from [`DatasetSession::open_path`] |
    #[instrument(skip_all, fields(input = %input))]
    pub fn open(input: &str) -> Result<Self, SessionError> {
        match DatasetSource::parse(input)? {
            DatasetSource::Local(root) => Self::open_path(&root),
            DatasetSource::Remote { store, root, url } => {
                Self::open_remote(store.as_ref(), &root, &url)
            }
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

        let expected = manifest.atom_count().get();
        let actual = catchments.total_rows();
        if expected != actual {
            return Err(SessionError::AtomCountMismatch {
                manifest_count: expected,
                actual_count: actual,
            });
        }

        // --- Referential integrity: graph ↔ catchments ---
        debug!("verifying graph ↔ catchment referential integrity");
        let catchment_ids = catchments.read_all_ids()?;
        let catchment_id_set: std::collections::HashSet<hfx_core::AtomId> =
            catchment_ids.iter().copied().collect();

        // Every graph atom must have a catchment row
        for row in graph.rows() {
            if !catchment_id_set.contains(&row.id()) {
                return Err(SessionError::integrity(format!(
                    "graph atom {} has no corresponding catchment row",
                    row.id().get(),
                )));
            }
            // Every upstream reference must point to an existing catchment
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

        // Every catchment must have a graph row
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

        let snap = if manifest.snap() == SnapAvailability::Present {
            Some(SnapStore::open(&root.join("snap.parquet"))?)
        } else {
            None
        };

        // If snap is present, verify all snap catchment_id references exist
        if let Some(ref snap_store) = snap {
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
        }

        let raster_paths = if matches!(manifest.rasters(), RasterAvailability::Present(_)) {
            Some(RasterPaths {
                flow_dir: root.join("flow_dir.tif"),
                flow_acc: root.join("flow_acc.tif"),
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
            manifest,
            graph,
            catchments,
            snap,
            raster_paths,
        })
    }

    fn open_remote(
        store: &dyn ObjectStore,
        root: &ObjectPath,
        url: &Url,
    ) -> Result<Self, SessionError> {
        let cache = RemoteArtifactCache::configured()?;
        if let Some(cached) = cache.read_entry_for_source(url, root)? {
            debug!(
                fabric = cached.manifest.fabric_name(),
                atoms = cached.manifest.atom_count().get(),
                graph_atoms = cached.graph.len(),
                "remote manifest and graph parsed from cache"
            );

            return Err(SessionError::RemoteDatasetNotSupported {
                url: url.as_str().to_string(),
            });
        }

        let manifest_path = remote_artifact_path(root, "manifest.json");
        let graph_path = remote_artifact_path(root, "graph.arrow");

        let manifest_bytes = read_remote_artifact(store, manifest_path, "manifest.json")?;
        let manifest = reader::manifest::read_manifest_from_bytes(&manifest_bytes)?;

        let graph_bytes = read_remote_artifact(store, graph_path, "graph.arrow")?;
        let graph = reader::graph::load_graph_from_bytes(graph_bytes.clone())?;
        cache.write_manifest_graph(url, root, &manifest, &manifest_bytes, &graph_bytes)?;

        debug!(
            fabric = manifest.fabric_name(),
            atoms = manifest.atom_count().get(),
            graph_atoms = graph.len(),
            "remote manifest and graph parsed"
        );

        Err(SessionError::RemoteDatasetNotSupported {
            url: url.as_str().to_string(),
        })
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

    /// Return the dataset root directory path.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn remote_artifact_path(root: &ObjectPath, artifact: &'static str) -> ObjectPath {
    root.clone().join(artifact)
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

    use arrow::array::{Int64Array, Int64Builder, ListBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt, PutPayload};
    use url::Url;

    use super::DatasetSession;
    use crate::error::SessionError;
    use crate::runtime::RT;

    static CACHE_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct CacheEnv {
        _guard: MutexGuard<'static, ()>,
        previous: Option<OsString>,
    }

    impl CacheEnv {
        fn set(path: &Path) -> Self {
            let guard = CACHE_ENV_LOCK.lock().unwrap();
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

    fn manifest_bytes() -> String {
        serde_json::json!({
            "format_version": "0.1",
            "fabric_name": "testfabric",
            "crs": "EPSG:4326",
            "topology": "tree",
            "terminal_sink_id": 0,
            "bbox": [-10.0, -5.0, 10.0, 5.0],
            "atom_count": 2,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "test-v1"
        })
        .to_string()
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

    fn put_remote_manifest_and_graph(store: &Arc<InMemory>, root: &ObjectPath) {
        RT.block_on(async {
            store
                .put(
                    &root.clone().join("manifest.json"),
                    PutPayload::from(manifest_bytes()),
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
    fn open_remote_fetches_manifest_and_graph_before_not_supported() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        put_remote_manifest_and_graph(&store, &root);
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let err = DatasetSession::open_remote(store.as_ref(), &root, &url).unwrap_err();

        assert!(matches!(
            err,
            SessionError::RemoteDatasetNotSupported { .. }
        ));
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
        put_remote_manifest_and_graph(&store, &root);
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let first_err = DatasetSession::open_remote(store.as_ref(), &root, &url).unwrap_err();
        assert!(matches!(
            first_err,
            SessionError::RemoteDatasetNotSupported { .. }
        ));

        let empty_store = Arc::new(InMemory::new());
        let second_err =
            DatasetSession::open_remote(empty_store.as_ref(), &root, &url).unwrap_err();

        assert!(matches!(
            second_err,
            SessionError::RemoteDatasetNotSupported { .. }
        ));
    }

    #[test]
    fn open_remote_does_not_use_cache_entry_from_different_source() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let root_a = ObjectPath::from("dataset/a");
        let url_a = Url::parse("s3://shed-test/dataset/a").unwrap();
        let store_a = Arc::new(InMemory::new());
        put_remote_manifest_and_graph(&store_a, &root_a);

        let first_err = DatasetSession::open_remote(store_a.as_ref(), &root_a, &url_a).unwrap_err();
        assert!(matches!(
            first_err,
            SessionError::RemoteDatasetNotSupported { .. }
        ));

        let root_b = ObjectPath::from("dataset/b");
        let url_b = Url::parse("s3://shed-test/dataset/b").unwrap();
        let empty_store = Arc::new(InMemory::new());
        let unmapped_err =
            DatasetSession::open_remote(empty_store.as_ref(), &root_b, &url_b).unwrap_err();
        assert!(matches!(
            unmapped_err,
            SessionError::RemoteArtifactRead {
                artifact: "manifest.json",
                ..
            }
        ));

        let store_b = Arc::new(InMemory::new());
        put_remote_manifest_and_graph(&store_b, &root_b);
        let mapped_err =
            DatasetSession::open_remote(store_b.as_ref(), &root_b, &url_b).unwrap_err();
        assert!(matches!(
            mapped_err,
            SessionError::RemoteDatasetNotSupported { .. }
        ));

        let cached_b_err =
            DatasetSession::open_remote(empty_store.as_ref(), &root_b, &url_b).unwrap_err();
        assert!(matches!(
            cached_b_err,
            SessionError::RemoteDatasetNotSupported { .. }
        ));
    }

    #[test]
    fn open_remote_reports_missing_manifest() {
        let cache_dir = tempfile::TempDir::new().unwrap();
        let _cache_env = CacheEnv::set(cache_dir.path());
        let store = Arc::new(InMemory::new());
        let root = ObjectPath::from("dataset/root");
        let url = Url::parse("s3://shed-test/dataset/root").unwrap();

        let err = DatasetSession::open_remote(store.as_ref(), &root, &url).unwrap_err();

        assert!(matches!(
            err,
            SessionError::RemoteArtifactRead {
                artifact: "manifest.json",
                ..
            }
        ));
    }
}
