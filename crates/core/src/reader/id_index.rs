//! Internal Arrow-backed ID index cache format.
//!
//! This phase defines the format before stores or sessions are wired to it.
use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use hfx_core::AtomId;
use serde::{Deserialize, Serialize};

use crate::error::SessionError;

const CACHE_EXTENSION: &str = "idindex.arrow";
const MAGIC: &[u8; 4] = b"IIDX";
const MARKER_LEN: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdIndex {
    pub(crate) ids: Vec<AtomId>,
    pub(crate) id_row_groups: Option<HashMap<AtomId, usize>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Footer {
    file_etag: Option<String>,
    file_size: u64,
    built_by_shed_version: String,
    validated_at: u64,
    body_crc32: u32,
}

impl IdIndex {
    pub(crate) fn cache_path(artifact_path: &Path) -> PathBuf {
        artifact_path.with_extension(CACHE_EXTENSION)
    }

    pub(crate) fn write_to_path(
        &self,
        path: &Path,
        file_size: u64,
        file_etag: Option<&str>,
    ) -> Result<(), SessionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|source| SessionError::cache_io("create_dir_all", parent, source))?;
        }

        let body = self.encode_body(path)?;
        let footer = Footer {
            file_etag: file_etag.map(str::to_owned),
            file_size,
            built_by_shed_version: env!("CARGO_PKG_VERSION").to_owned(),
            validated_at: validated_at_unix_seconds(),
            body_crc32: crc32(&body),
        };
        let footer_json = serde_json::to_vec(&footer).map_err(|source| {
            SessionError::cache_json("serialize id index footer", path, source)
        })?;

        let mut bytes = Vec::with_capacity(body.len() + MARKER_LEN + footer_json.len());
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(&(footer_json.len() as u64).to_le_bytes());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&footer_json);

        std::fs::write(path, bytes).map_err(|source| SessionError::cache_io("write", path, source))
    }

    pub(crate) fn load_from_path(
        path: &Path,
        expected_size: u64,
        expected_etag: Option<&str>,
    ) -> Result<Option<Self>, SessionError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(SessionError::cache_io("read", path, source)),
        };

        let Some((body, footer_json)) = split_body_and_footer(&bytes) else {
            return Ok(None);
        };

        let footer: Footer = match serde_json::from_slice(footer_json) {
            Ok(footer) => footer,
            Err(_) => return Ok(None),
        };

        if footer.file_size != expected_size || footer.file_etag.as_deref() != expected_etag {
            return Ok(None);
        }

        if footer.built_by_shed_version != env!("CARGO_PKG_VERSION") {
            return Ok(None);
        }

        if crc32(body) != footer.body_crc32 {
            return Ok(None);
        }

        decode_body(body)
    }

    fn encode_body(&self, path: &Path) -> Result<Vec<u8>, SessionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("row_group", DataType::Int32, true),
        ]));
        let ids = Int64Array::from_iter_values(self.ids.iter().map(|id| id.get()));
        let row_group_values: Result<Vec<Option<i32>>, _> = self
            .ids
            .iter()
            .map(|id| {
                self.id_row_groups
                    .as_ref()
                    .and_then(|groups| groups.get(id))
                    .map(|row_group| i32::try_from(*row_group))
                    .transpose()
            })
            .collect();
        let row_groups = match row_group_values {
            Ok(values) => Int32Array::from(values),
            Err(_) => {
                return Err(SessionError::integrity(
                    "id index row group does not fit in Int32",
                ));
            }
        };

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(row_groups)])
            .map_err(|source| arrow_error_as_cache_io("build arrow batch", path, source))?;

        let mut body = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut body, &schema)
                .map_err(|source| arrow_error_as_cache_io("open arrow writer", path, source))?;
            writer
                .write(&batch)
                .map_err(|source| arrow_error_as_cache_io("write arrow batch", path, source))?;
            writer
                .finish()
                .map_err(|source| arrow_error_as_cache_io("finish arrow stream", path, source))?;
        }

        Ok(body)
    }
}

fn decode_body(body: &[u8]) -> Result<Option<IdIndex>, SessionError> {
    let reader = match StreamReader::try_new(Cursor::new(body), None) {
        Ok(reader) => reader,
        Err(_) => return Ok(None),
    };

    let schema = reader.schema();
    let Ok(id_field) = schema.field_with_name("id") else {
        return Ok(None);
    };
    let Ok(row_group_field) = schema.field_with_name("row_group") else {
        return Ok(None);
    };
    if id_field.data_type() != &DataType::Int64 || row_group_field.data_type() != &DataType::Int32 {
        return Ok(None);
    }

    let mut ids = Vec::new();
    let mut rows = Vec::new();
    for batch_result in reader {
        let batch = match batch_result {
            Ok(batch) => batch,
            Err(_) => return Ok(None),
        };
        let Some(id_col) = batch.column_by_name("id") else {
            return Ok(None);
        };
        let Some(id_col) = id_col.as_any().downcast_ref::<Int64Array>() else {
            return Ok(None);
        };
        let Some(row_group_col) = batch.column_by_name("row_group") else {
            return Ok(None);
        };
        let Some(row_group_col) = row_group_col.as_any().downcast_ref::<Int32Array>() else {
            return Ok(None);
        };

        for row in 0..batch.num_rows() {
            if id_col.is_null(row) {
                return Ok(None);
            }
            let id = match AtomId::new(id_col.value(row)) {
                Ok(id) => id,
                Err(_) => return Ok(None),
            };
            let row_group = if row_group_col.is_null(row) {
                None
            } else {
                let raw = row_group_col.value(row);
                if raw < 0 {
                    return Ok(None);
                }
                Some(raw as usize)
            };

            ids.push(id);
            rows.push(row_group);
        }
    }

    let row_group_count = rows.iter().filter(|value| value.is_some()).count();
    let id_row_groups = match row_group_count {
        0 => None,
        count if count == ids.len() => {
            let mut groups = HashMap::with_capacity(ids.len());
            for (id, row_group) in ids.iter().copied().zip(rows.into_iter().flatten()) {
                if groups.insert(id, row_group).is_some() {
                    return Ok(None);
                }
            }
            Some(groups)
        }
        _ => return Ok(None),
    };

    Ok(Some(IdIndex { ids, id_row_groups }))
}

fn split_body_and_footer(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    if bytes.len() < MARKER_LEN {
        return None;
    }

    for magic_start in (8..=bytes.len() - MAGIC.len()).rev() {
        if &bytes[magic_start..magic_start + MAGIC.len()] != MAGIC {
            continue;
        }

        let length_start = magic_start - 8;
        let footer_start = magic_start + MAGIC.len();
        let footer_len = u64::from_le_bytes(bytes[length_start..magic_start].try_into().ok()?);
        let footer_end = footer_start.checked_add(usize::try_from(footer_len).ok()?)?;
        if footer_end == bytes.len() {
            return Some((&bytes[..length_start], &bytes[footer_start..footer_end]));
        }
    }

    None
}

fn validated_at_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn arrow_error_as_cache_io(
    operation: &'static str,
    path: &Path,
    source: arrow::error::ArrowError,
) -> SessionError {
    SessionError::cache_io(operation, path, std::io::Error::other(source.to_string()))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;

    use hfx_core::AtomId;
    use tempfile::tempdir;

    use super::IdIndex;

    fn aid(raw: i64) -> AtomId {
        AtomId::new(raw).unwrap()
    }

    fn write_index(path: &Path, index: &IdIndex) {
        index
            .write_to_path(path, 12_345, Some("etag-a"))
            .expect("write id index");
    }

    #[test]
    fn round_trip_with_row_groups_some() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("catchments.idindex.arrow");
        let ids = vec![aid(10), aid(20), aid(30)];
        let id_row_groups = HashMap::from([(aid(10), 0), (aid(20), 1), (aid(30), 1)]);
        let index = IdIndex {
            ids: ids.clone(),
            id_row_groups: Some(id_row_groups.clone()),
        };

        write_index(&path, &index);
        let loaded = IdIndex::load_from_path(&path, 12_345, Some("etag-a"))
            .unwrap()
            .unwrap();

        assert_eq!(loaded.ids, ids);
        assert_eq!(loaded.id_row_groups, Some(id_row_groups));
    }

    #[test]
    fn round_trip_snap_shape_with_row_groups_none() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("snap.idindex.arrow");
        let ids = vec![aid(5), aid(6), aid(7)];
        let index = IdIndex {
            ids: ids.clone(),
            id_row_groups: None,
        };

        write_index(&path, &index);
        let loaded = IdIndex::load_from_path(&path, 12_345, Some("etag-a"))
            .unwrap()
            .unwrap();

        assert_eq!(loaded.ids, ids);
        assert_eq!(loaded.id_row_groups, None);
    }

    #[test]
    fn etag_mismatch_invalidates_index() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("catchments.idindex.arrow");
        let index = IdIndex {
            ids: vec![aid(1)],
            id_row_groups: Some(HashMap::from([(aid(1), 0)])),
        };

        write_index(&path, &index);

        assert!(
            IdIndex::load_from_path(&path, 12_345, Some("etag-b"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn size_mismatch_invalidates_index() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("catchments.idindex.arrow");
        let index = IdIndex {
            ids: vec![aid(1)],
            id_row_groups: Some(HashMap::from([(aid(1), 0)])),
        };

        write_index(&path, &index);

        assert!(
            IdIndex::load_from_path(&path, 54_321, Some("etag-a"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn corrupt_body_invalidates_index() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("catchments.idindex.arrow");
        let index = IdIndex {
            ids: vec![aid(1)],
            id_row_groups: Some(HashMap::from([(aid(1), 0)])),
        };

        write_index(&path, &index);
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        assert!(
            IdIndex::load_from_path(&path, 12_345, Some("etag-a"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn corrupt_magic_invalidates_index() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("catchments.idindex.arrow");
        let index = IdIndex {
            ids: vec![aid(1)],
            id_row_groups: Some(HashMap::from([(aid(1), 0)])),
        };

        write_index(&path, &index);
        let mut bytes = fs::read(&path).unwrap();
        let magic_start = bytes
            .windows(super::MAGIC.len())
            .rposition(|window| window == super::MAGIC)
            .unwrap();
        bytes[magic_start] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        assert!(
            IdIndex::load_from_path(&path, 12_345, Some("etag-a"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn version_mismatch_invalidates_index() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("catchments.idindex.arrow");
        let index = IdIndex {
            ids: vec![aid(1)],
            id_row_groups: Some(HashMap::from([(aid(1), 0)])),
        };

        write_index(&path, &index);
        let mut bytes = fs::read(&path).unwrap();
        let current = env!("CARGO_PKG_VERSION").as_bytes();
        let version_start = bytes
            .windows(current.len())
            .position(|window| window == current)
            .unwrap();
        bytes[version_start] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        assert!(
            IdIndex::load_from_path(&path, 12_345, Some("etag-a"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cache_path_uses_idindex_arrow_extension() {
        assert_eq!(
            IdIndex::cache_path(Path::new("/tmp/catchments.parquet")),
            Path::new("/tmp/catchments.idindex.arrow")
        );
    }
}
