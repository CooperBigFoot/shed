//! Remote artifact cache for parsed HFX dataset metadata.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use hfx_core::{DrainageGraph, Manifest};
use object_store::path::Path as ObjectPath;
use tracing::{debug, warn};
use url::Url;

use crate::error::SessionError;
use crate::reader;
use crate::reader::id_index::IdIndex;

const CACHE_ENV: &str = "HFX_CACHE_DIR";
const CACHE_NAMESPACE: &str = "hfx";

/// Cached remote artifacts that have already been parsed successfully.
#[derive(Debug)]
pub(crate) struct CachedRemoteArtifacts {
    pub(crate) manifest: Manifest,
    pub(crate) graph: DrainageGraph,
}

/// Cache for the remote artifacts needed before parquet loading is supported.
#[derive(Debug, Clone)]
pub(crate) struct RemoteArtifactCache {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct ValidationSidecar {
    pub(crate) catchments_etag: String,
    pub(crate) catchments_size: u64,
    pub(crate) snap_etag: Option<String>,
    pub(crate) snap_size: Option<u64>,
    pub(crate) validated_at: u64,
    pub(crate) shed_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactMeta {
    pub(crate) etag: String,
    pub(crate) size: u64,
}

impl ArtifactMeta {
    pub(crate) fn from_parts(etag: Option<&str>, size: u64) -> Option<Self> {
        etag.map(|etag| Self {
            etag: etag.to_owned(),
            size,
        })
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct SourceIndexEntry {
    source: String,
    remote_root: String,
    fabric_name: String,
    adapter_version: String,
}

impl RemoteArtifactCache {
    /// Return the configured cache rooted at `HFX_CACHE_DIR` or `~/.cache/hfx`.
    pub(crate) fn configured() -> Result<Self, SessionError> {
        let root = match std::env::var_os(CACHE_ENV) {
            Some(path) => PathBuf::from(path),
            None => dirs::cache_dir()
                .map(|path| path.join(CACHE_NAMESPACE))
                .ok_or(SessionError::CacheRootUnavailable)?,
        };

        Ok(Self { root })
    }

    /// Return the filesystem root used by this cache.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn artifact_cache_dir(&self, fabric_name: &str, adapter_version: &str) -> PathBuf {
        self.root.join(fabric_name).join(adapter_version)
    }

    pub(crate) fn id_index_path(
        &self,
        fabric_name: &str,
        adapter_version: &str,
        artifact: &str,
    ) -> PathBuf {
        IdIndex::cache_path(
            &self
                .artifact_cache_dir(fabric_name, adapter_version)
                .join(artifact),
        )
    }

    pub(crate) fn validation_sidecar_path(
        &self,
        fabric_name: &str,
        adapter_version: &str,
    ) -> PathBuf {
        self.artifact_cache_dir(fabric_name, adapter_version)
            .join("validated.json")
    }

    /// Return the parsed cache entry mapped to this exact remote source.
    pub(crate) fn read_entry_for_source(
        &self,
        url: &Url,
        remote_root: &ObjectPath,
    ) -> Result<Option<CachedRemoteArtifacts>, SessionError> {
        let index_path = self.source_index_path(url, remote_root);
        if !index_path.is_file() {
            return Ok(None);
        }

        let bytes = std::fs::read(&index_path)
            .map_err(|source| SessionError::cache_io("read", &index_path, source))?;
        let Ok(index) = serde_json::from_slice::<SourceIndexEntry>(&bytes) else {
            debug!(
                path = %index_path.display(),
                "remote cache source index is not valid JSON; falling back to remote fetch"
            );
            return Ok(None);
        };

        if index.source != url.as_str() || index.remote_root != remote_root.as_ref() {
            debug!(
                path = %index_path.display(),
                "remote cache source index key collision; falling back to remote fetch"
            );
            return Ok(None);
        }

        let cache_dir = self.artifact_cache_dir(&index.fabric_name, &index.adapter_version);

        Ok(read_entry(&cache_dir))
    }

    /// Write parsed manifest and graph bytes and map them to this remote source.
    pub(crate) fn write_manifest_graph(
        &self,
        url: &Url,
        remote_root: &ObjectPath,
        manifest: &Manifest,
        manifest_bytes: &[u8],
        graph_bytes: &Bytes,
    ) -> Result<(), SessionError> {
        let cache_dir = self.artifact_cache_dir(manifest.fabric_name(), manifest.adapter_version());

        std::fs::create_dir_all(&cache_dir)
            .map_err(|source| SessionError::cache_io("create_dir_all", &cache_dir, source))?;
        write_cache_file(&cache_dir.join("manifest.json"), manifest_bytes)?;
        write_cache_file(&cache_dir.join("graph.arrow"), graph_bytes.as_ref())?;
        self.write_source_index(url, remote_root, manifest)?;

        debug!(
            fabric = manifest.fabric_name(),
            adapter_version = manifest.adapter_version(),
            path = %cache_dir.display(),
            "remote manifest and graph cached"
        );

        Ok(())
    }

    fn write_source_index(
        &self,
        url: &Url,
        remote_root: &ObjectPath,
        manifest: &Manifest,
    ) -> Result<(), SessionError> {
        let index_dir = self.root.join(".sources");
        std::fs::create_dir_all(&index_dir)
            .map_err(|source| SessionError::cache_io("create_dir_all", &index_dir, source))?;

        let index = SourceIndexEntry {
            source: url.as_str().to_string(),
            remote_root: remote_root.as_ref().to_string(),
            fabric_name: manifest.fabric_name().to_string(),
            adapter_version: manifest.adapter_version().to_string(),
        };
        let bytes = serde_json::to_vec(&index).map_err(|source| {
            SessionError::cache_json(
                "serialize source index",
                &self.source_index_path(url, remote_root),
                source,
            )
        })?;
        write_cache_file(&self.source_index_path(url, remote_root), &bytes)
    }

    fn source_index_path(&self, url: &Url, remote_root: &ObjectPath) -> PathBuf {
        let key = format!("{}\n{}", url.as_str(), remote_root.as_ref());
        self.root
            .join(".sources")
            .join(format!("{:016x}.json", fnv1a64(key.as_bytes())))
    }

    pub(crate) fn read_validation_sidecar(
        &self,
        fabric_name: &str,
        adapter_version: &str,
    ) -> Option<ValidationSidecar> {
        let path = self.validation_sidecar_path(fabric_name, adapter_version);
        let bytes = std::fs::read(&path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    pub(crate) fn write_validation_sidecar_best_effort(
        &self,
        fabric_name: &str,
        adapter_version: &str,
        sidecar: &ValidationSidecar,
    ) {
        let path = self.validation_sidecar_path(fabric_name, adapter_version);
        let result = (|| -> Result<(), SessionError> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|source| SessionError::cache_io("create_dir_all", parent, source))?;
            }
            let bytes = serde_json::to_vec(sidecar).map_err(|source| {
                SessionError::cache_json("serialize validation sidecar", &path, source)
            })?;
            write_cache_file(&path, &bytes)
        })();
        if let Err(error) = result {
            warn!(path = %path.display(), error = %error, "failed to write validation sidecar");
        }
    }
}

impl ValidationSidecar {
    pub(crate) fn current(catchments: ArtifactMeta, snap: Option<ArtifactMeta>) -> Self {
        Self {
            catchments_etag: catchments.etag,
            catchments_size: catchments.size,
            snap_etag: snap.as_ref().map(|meta| meta.etag.clone()),
            snap_size: snap.map(|meta| meta.size),
            validated_at: validated_at_unix_seconds(),
            shed_version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }

    pub(crate) fn matches(&self, catchments: &ArtifactMeta, snap: Option<&ArtifactMeta>) -> bool {
        self.catchments_etag == catchments.etag
            && self.catchments_size == catchments.size
            && self.snap_etag.as_deref() == snap.map(|meta| meta.etag.as_str())
            && self.snap_size == snap.map(|meta| meta.size)
            && self.shed_version == env!("CARGO_PKG_VERSION")
    }
}

fn read_entry(path: &Path) -> Option<CachedRemoteArtifacts> {
    let manifest_path = path.join("manifest.json");
    let graph_path = path.join("graph.arrow");
    if !manifest_path.is_file() || !graph_path.is_file() {
        return None;
    }

    let manifest_bytes = std::fs::read(&manifest_path).ok()?;
    let manifest = reader::manifest::read_manifest_from_bytes(&manifest_bytes).ok()?;
    if !cache_key_matches(path, &manifest) {
        return None;
    }

    let graph_bytes = Bytes::from(std::fs::read(&graph_path).ok()?);
    let graph = reader::graph::load_graph_from_bytes(graph_bytes).ok()?;

    Some(CachedRemoteArtifacts { manifest, graph })
}

fn cache_key_matches(path: &Path, manifest: &Manifest) -> bool {
    let adapter_version = path.file_name().and_then(|name| name.to_str());
    let fabric_name = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str());

    fabric_name == Some(manifest.fabric_name())
        && adapter_version == Some(manifest.adapter_version())
}

fn write_cache_file(path: &Path, bytes: &[u8]) -> Result<(), SessionError> {
    std::fs::write(path, bytes).map_err(|source| SessionError::cache_io("write", path, source))
}

fn validated_at_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::{ArtifactMeta, RemoteArtifactCache, ValidationSidecar};

    #[test]
    fn id_index_path_uses_artifact_cache_dir() {
        let cache = RemoteArtifactCache {
            root: "/tmp/cache".into(),
        };

        assert_eq!(
            cache.id_index_path("fabric", "adapter", "catchments.parquet"),
            std::path::Path::new("/tmp/cache/fabric/adapter/catchments.idindex.arrow")
        );
    }

    #[test]
    fn validation_sidecar_matches_exact_metadata_and_version() {
        let catchments = ArtifactMeta {
            etag: "catch-etag".into(),
            size: 123,
        };
        let snap = ArtifactMeta {
            etag: "snap-etag".into(),
            size: 456,
        };
        let sidecar = ValidationSidecar::current(catchments.clone(), Some(snap.clone()));

        assert!(sidecar.matches(&catchments, Some(&snap)));
        assert!(!sidecar.matches(
            &ArtifactMeta {
                etag: "other".into(),
                size: 123,
            },
            Some(&snap)
        ));
        assert!(!sidecar.matches(&catchments, None));
    }

    #[test]
    fn validation_sidecar_matches_absent_snap() {
        let catchments = ArtifactMeta {
            etag: "catch-etag".into(),
            size: 123,
        };
        let sidecar = ValidationSidecar::current(catchments.clone(), None);

        assert!(sidecar.matches(&catchments, None));
    }
}
