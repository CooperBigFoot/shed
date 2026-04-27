//! Local cache for remote raster artifacts.
//!
//! TODO(R3): add bounded LRU eviction and a cross-process file lock.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures_util::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tempfile::NamedTempFile;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Notify;

use crate::cache::fnv1a64;
use crate::cog::{
    LocalizedRasterWindow, RasterWindowRequest, fetch_window_to_path, prepare_window,
};
use crate::error::CacheError;

/// Cache for remote raster files fetched from object storage.
#[derive(Debug)]
pub(crate) struct RemoteRasterCache {
    root: PathBuf,
    in_flight: DashMap<PathBuf, Arc<Notify>>,
}

impl RemoteRasterCache {
    /// Create a remote raster cache rooted at `root`.
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            in_flight: DashMap::new(),
        }
    }

    /// Return a cached local GeoTIFF for a remote COG pixel window.
    pub(crate) async fn get_or_fetch_window(
        &self,
        store: &dyn ObjectStore,
        remote_path: &ObjectPath,
        request: &RasterWindowRequest,
        fabric_name: &str,
        adapter_version: &str,
    ) -> Result<LocalizedRasterWindow, CacheError> {
        let prepared = prepare_window(store, remote_path, request).await?;
        let canonical = self.canonical_window_path(
            remote_path,
            request,
            &prepared.cache_fragment(),
            fabric_name,
            adapter_version,
        );
        if cache_hit(&canonical).await? {
            return Ok(LocalizedRasterWindow::cached(canonical));
        }

        loop {
            match self.in_flight.entry(canonical.clone()) {
                Entry::Occupied(entry) => {
                    let notify = Arc::clone(entry.get());
                    drop(entry);
                    notify.notified().await;
                    if cache_hit(&canonical).await? {
                        return Ok(LocalizedRasterWindow::cached(canonical));
                    }
                }
                Entry::Vacant(entry) => {
                    let notify = Arc::new(Notify::new());
                    entry.insert(Arc::clone(&notify));
                    let result =
                        fetch_window_to_path(store, remote_path, prepared, &canonical).await;
                    self.in_flight.remove(&canonical);
                    notify.notify_waiters();
                    return result;
                }
            }
        }
    }

    fn canonical_path(
        &self,
        remote_path: &ObjectPath,
        fabric_name: &str,
        adapter_version: &str,
    ) -> PathBuf {
        self.root
            .join(fabric_name)
            .join(adapter_version)
            .join("rasters")
            .join(flat_key(remote_path))
    }

    fn canonical_window_path(
        &self,
        remote_path: &ObjectPath,
        request: &RasterWindowRequest,
        window_fragment: &str,
        fabric_name: &str,
        adapter_version: &str,
    ) -> PathBuf {
        let remote_hash = fnv1a64(remote_path.as_ref().as_bytes());
        let key = format!(
            "{}.{}.{}.tif",
            request.kind().cache_name(),
            remote_hash,
            window_fragment
        );
        self.root
            .join(fabric_name)
            .join(adapter_version)
            .join("raster-windows")
            .join(key)
    }

    async fn fetch_to_path(
        &self,
        store: &dyn ObjectStore,
        remote_path: &ObjectPath,
        canonical: &Path,
    ) -> Result<(), CacheError> {
        let parent = canonical.parent().ok_or_else(|| CacheError::Io {
            op: "parent",
            path: canonical.to_path_buf(),
            source: std::io::Error::new(ErrorKind::InvalidInput, "cache path has no parent"),
        })?;

        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| CacheError::Io {
                op: "create_dir_all",
                path: parent.to_path_buf(),
                source,
            })?;

        let temp = NamedTempFile::new_in(parent).map_err(|source| CacheError::Io {
            op: "create_temp",
            path: parent.to_path_buf(),
            source,
        })?;
        let temp_path = temp.path().to_path_buf();

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&temp_path)
            .await
            .map_err(|source| CacheError::Io {
                op: "open_temp",
                path: temp_path.clone(),
                source,
            })?;
        let mut writer = BufWriter::new(file);

        let object = store
            .get(remote_path)
            .await
            .map_err(|source| CacheError::ObjectStore {
                path: remote_path.clone(),
                source,
            })?;
        let mut stream = object.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|source| CacheError::ObjectStore {
                path: remote_path.clone(),
                source,
            })?;
            writer
                .write_all(&chunk)
                .await
                .map_err(|source| CacheError::Io {
                    op: "write",
                    path: temp_path.clone(),
                    source,
                })?;
        }

        writer.flush().await.map_err(|source| CacheError::Io {
            op: "flush",
            path: temp_path.clone(),
            source,
        })?;
        // sync_all before persist is intentional: the temp file contents should
        // be durable before the canonical cache path is published.
        writer
            .get_ref()
            .sync_all()
            .await
            .map_err(|source| CacheError::Io {
                op: "sync_all",
                path: temp_path,
                source,
            })?;
        drop(writer);

        match temp.persist_noclobber(canonical) {
            Ok(_) => Ok(()),
            Err(error) if error.error.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(source) => Err(CacheError::Persist { source }),
        }
    }
}

async fn cache_hit(path: &Path) -> Result<bool, CacheError> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.len() > 0),
        Err(source) if source.kind() == ErrorKind::NotFound => Ok(false),
        Err(source) => Err(CacheError::Io {
            op: "metadata",
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn flat_key(remote_path: &ObjectPath) -> String {
    let path = remote_path.as_ref();
    let flat = path.replace('/', "__");
    format!("{flat}.{:016x}", fnv1a64(path.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::RemoteRasterCache;
    use object_store::path::Path as ObjectPath;

    const FABRIC: &str = "test-fabric";
    const ADAPTER: &str = "0.1.0";

    #[test]
    fn flat_key_resists_separator_replacement_collision() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let cache = RemoteRasterCache::new(temp.path().to_path_buf());
        let with_separator = cache.canonical_path(&ObjectPath::from("a/b.tif"), FABRIC, ADAPTER);
        let with_literal = cache.canonical_path(&ObjectPath::from("a__b.tif"), FABRIC, ADAPTER);

        assert_ne!(with_separator, with_literal);
        assert_eq!(
            with_separator.parent(),
            Some(
                temp.path()
                    .join(FABRIC)
                    .join(ADAPTER)
                    .join("rasters")
                    .as_path()
            )
        );
    }
}
