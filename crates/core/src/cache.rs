//! Remote artifact cache for parsed HFX dataset metadata.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use hfx_core::{DrainageGraph, Manifest};
use object_store::path::Path as ObjectPath;
use tracing::debug;
use url::Url;

use crate::error::SessionError;
use crate::reader;

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

        let cache_dir = self
            .root
            .join(index.fabric_name)
            .join(index.adapter_version);

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
        let cache_dir = self
            .root
            .join(manifest.fabric_name())
            .join(manifest.adapter_version());

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

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
