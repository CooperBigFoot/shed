//! Dataset session — loads an HFX dataset for repeated queries.

use std::path::{Path, PathBuf};

use hfx_core::{Manifest, RasterAvailability, SnapAvailability, Topology};
use tracing::{debug, info, instrument};

use crate::error::SessionError;
use crate::reader::catchment_store::CatchmentStore;
use crate::reader::snap_store::SnapStore;
use crate::reader;

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
    /// Open an HFX dataset rooted at `root` and return a ready-to-query session.
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
    #[instrument(skip_all, fields(root = %root.as_ref().display()))]
    pub fn open(root: impl AsRef<Path>) -> Result<Self, SessionError> {
        let root = root.as_ref();

        if !root.is_dir() {
            return Err(SessionError::RootNotFound { path: root.display().to_string() });
        }

        for artifact in ["manifest.json", "graph.arrow", "catchments.parquet"] {
            let p = root.join(artifact);
            if !p.exists() {
                return Err(SessionError::required_missing(artifact, p.display().to_string()));
            }
        }

        let manifest = reader::manifest::read_manifest(&root.join("manifest.json"))?;

        if manifest.snap() == SnapAvailability::Present {
            let p = root.join("snap.parquet");
            if !p.exists() {
                return Err(SessionError::optional_missing("snap.parquet", p.display().to_string()));
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
