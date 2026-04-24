//! Manifest reader — parses manifest.json into an hfx_core::Manifest.

use std::path::Path;
use std::str::FromStr;

use hfx_core::{
    AtomCount, BoundingBox, Crs, FlowDirEncoding, FormatVersion, Manifest, ManifestBuilder,
    Topology,
};
use tracing::instrument;

use crate::error::SessionError;

/// Raw serde struct for deserializing manifest.json.
///
/// All fields are `Option<T>` to allow field-level error reporting rather than
/// failing at the serde layer on missing required fields.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct RawManifest {
    pub format_version: Option<String>,
    pub fabric_name: Option<String>,
    pub fabric_version: Option<String>,
    pub fabric_level: Option<u32>,
    pub crs: Option<String>,
    pub has_up_area: Option<bool>,
    pub has_rasters: Option<bool>,
    pub has_snap: Option<bool>,
    pub flow_dir_encoding: Option<String>,
    pub terminal_sink_id: Option<i64>,
    pub topology: Option<String>,
    pub region: Option<String>,
    pub bbox: Option<Vec<f64>>,
    pub atom_count: Option<u64>,
    pub created_at: Option<String>,
    pub adapter_version: Option<String>,
}

/// Reads and validates `manifest.json` at `path`, returning a typed [`Manifest`].
///
/// # Errors
///
/// | Variant | Condition |
/// |---|---|
/// | [`SessionError::Io`] | File cannot be read |
/// | [`SessionError::ManifestJsonParse`] | Bytes are not valid JSON or do not match the expected shape |
/// | [`SessionError::ManifestFieldMissing`] | A required field is absent from the JSON object |
/// | [`SessionError::ManifestFieldInvalid`] | A field is present but its value is invalid |
/// | [`SessionError::ManifestDomain`] | Parsed fields pass individual checks but `ManifestBuilder` rejects the combination |
#[instrument(skip_all, fields(path = %path.display()))]
pub fn read_manifest(path: &Path) -> Result<Manifest, SessionError> {
    let bytes = std::fs::read(path).map_err(|e| SessionError::io("manifest.json", e))?;

    read_manifest_from_bytes(&bytes)
}

/// Reads and validates `manifest.json` bytes, returning a typed [`Manifest`].
///
/// # Errors
///
/// | Variant | Condition |
/// |---|---|
/// | [`SessionError::ManifestJsonParse`] | Bytes are not valid JSON or do not match the expected shape |
/// | [`SessionError::ManifestFieldMissing`] | A required field is absent from the JSON object |
/// | [`SessionError::ManifestFieldInvalid`] | A field is present but its value is invalid |
/// | [`SessionError::ManifestDomain`] | Parsed fields pass individual checks but `ManifestBuilder` rejects the combination |
#[instrument(skip_all, fields(byte_len = bytes.len()))]
pub fn read_manifest_from_bytes(bytes: &[u8]) -> Result<Manifest, SessionError> {
    let raw = serde_json::from_slice::<RawManifest>(bytes)
        .map_err(|source| SessionError::ManifestJsonParse { source })?;

    build_manifest(raw)
}

/// Converts a [`RawManifest`] into a validated [`Manifest`].
fn build_manifest(raw: RawManifest) -> Result<Manifest, SessionError> {
    // --- Required fields ---

    let format_version_str = raw
        .format_version
        .ok_or(SessionError::ManifestFieldMissing {
            field: "format_version",
        })?;
    let format_version = FormatVersion::from_str(&format_version_str).map_err(|_| {
        SessionError::manifest_field_invalid(
            "format_version",
            format!(
                "unsupported version {:?}, expected \"0.1\"",
                format_version_str
            ),
        )
    })?;

    let fabric_name = raw.fabric_name.ok_or(SessionError::ManifestFieldMissing {
        field: "fabric_name",
    })?;

    let crs_str = raw
        .crs
        .ok_or(SessionError::ManifestFieldMissing { field: "crs" })?;
    let crs = Crs::from_str(&crs_str).map_err(|_| {
        SessionError::manifest_field_invalid(
            "crs",
            format!("unsupported CRS {:?}, expected \"EPSG:4326\"", crs_str),
        )
    })?;

    let topology_str = raw
        .topology
        .ok_or(SessionError::ManifestFieldMissing { field: "topology" })?;
    let topology = Topology::from_str(&topology_str).map_err(|_| {
        SessionError::manifest_field_invalid(
            "topology",
            format!(
                "unsupported topology {:?}, expected \"tree\" or \"dag\"",
                topology_str
            ),
        )
    })?;

    let terminal_sink_id = raw
        .terminal_sink_id
        .ok_or(SessionError::ManifestFieldMissing {
            field: "terminal_sink_id",
        })?;

    let bbox_raw = raw
        .bbox
        .ok_or(SessionError::ManifestFieldMissing { field: "bbox" })?;
    if bbox_raw.len() != 4 {
        return Err(SessionError::manifest_field_invalid(
            "bbox",
            format!(
                "expected 4 elements [minx, miny, maxx, maxy], got {}",
                bbox_raw.len()
            ),
        ));
    }
    let bbox = BoundingBox::new(
        bbox_raw[0] as f32,
        bbox_raw[1] as f32,
        bbox_raw[2] as f32,
        bbox_raw[3] as f32,
    )
    .map_err(|e| SessionError::manifest_field_invalid("bbox", e.to_string()))?;

    let atom_count_raw = raw.atom_count.ok_or(SessionError::ManifestFieldMissing {
        field: "atom_count",
    })?;
    let atom_count = AtomCount::new(atom_count_raw)
        .map_err(|e| SessionError::manifest_field_invalid("atom_count", e.to_string()))?;

    let created_at = raw.created_at.ok_or(SessionError::ManifestFieldMissing {
        field: "created_at",
    })?;

    let adapter_version = raw
        .adapter_version
        .ok_or(SessionError::ManifestFieldMissing {
            field: "adapter_version",
        })?;

    // --- Conditional: flow_dir_encoding required when has_rasters is true ---

    let has_rasters = raw.has_rasters.unwrap_or(false);
    let flow_dir_encoding = if has_rasters {
        let encoding_str = raw.flow_dir_encoding.ok_or_else(|| {
            SessionError::manifest_field_invalid(
                "flow_dir_encoding",
                "required when has_rasters is true but was not provided",
            )
        })?;
        let encoding = FlowDirEncoding::from_str(&encoding_str).map_err(|_| {
            SessionError::manifest_field_invalid(
                "flow_dir_encoding",
                format!(
                    "unsupported encoding {:?}, expected \"esri\" or \"taudem\"",
                    encoding_str
                ),
            )
        })?;
        Some(encoding)
    } else {
        None
    };

    // --- Build ---

    let builder = ManifestBuilder::new(
        format_version,
        fabric_name,
        crs,
        topology,
        terminal_sink_id,
        bbox,
        atom_count,
        created_at,
        adapter_version,
    )
    .map_err(|source| SessionError::ManifestDomain { source })?;

    let builder = if raw.has_up_area.unwrap_or(false) {
        builder.with_up_area()
    } else {
        builder
    };
    let builder = if let Some(encoding) = flow_dir_encoding {
        builder.with_rasters(encoding)
    } else {
        builder
    };
    let builder = if raw.has_snap.unwrap_or(false) {
        builder.with_snap()
    } else {
        builder
    };
    let builder = if let Some(v) = raw.fabric_version {
        builder.with_fabric_version(v)
    } else {
        builder
    };
    let builder = if let Some(v) = raw.fabric_level {
        builder.with_fabric_level(v)
    } else {
        builder
    };
    let builder = if let Some(v) = raw.region {
        builder.with_region(v)
    } else {
        builder
    };

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::error::SessionError;

    /// Write a JSON value to `manifest.json` inside `dir` and return the path.
    fn write_manifest(dir: &TempDir, value: &serde_json::Value) -> std::path::PathBuf {
        let path = dir.path().join("manifest.json");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(value.to_string().as_bytes()).unwrap();
        path
    }

    fn minimal_json() -> serde_json::Value {
        json!({
            "format_version": "0.1",
            "fabric_name": "testfabric",
            "crs": "EPSG:4326",
            "topology": "tree",
            "terminal_sink_id": 0,
            "bbox": [-10.0, -5.0, 10.0, 5.0],
            "atom_count": 100,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "hfx-adapter-v1"
        })
    }

    #[test]
    fn test_valid_minimal_manifest() {
        let dir = TempDir::new().unwrap();
        let path = write_manifest(&dir, &minimal_json());

        let manifest = read_manifest(&path).unwrap();

        assert_eq!(manifest.format_version(), FormatVersion::V0_1);
        assert_eq!(manifest.fabric_name(), "testfabric");
        assert_eq!(manifest.crs(), Crs::Epsg4326);
        assert_eq!(manifest.topology(), Topology::Tree);
        assert_eq!(manifest.terminal_sink_id(), 0);
        assert_eq!(manifest.atom_count().get(), 100);
        assert_eq!(manifest.created_at(), "2026-01-01T00:00:00Z");
        assert_eq!(manifest.adapter_version(), "hfx-adapter-v1");
        assert_eq!(manifest.fabric_version(), None);
        assert_eq!(manifest.fabric_level(), None);
        assert_eq!(manifest.region(), None);
        assert_eq!(
            manifest.up_area(),
            hfx_core::UpAreaAvailability::NotAvailable
        );
        assert_eq!(manifest.rasters(), hfx_core::RasterAvailability::Absent);
        assert_eq!(manifest.snap(), hfx_core::SnapAvailability::Absent);
    }

    #[test]
    fn test_valid_minimal_manifest_from_bytes() {
        let bytes = minimal_json().to_string();

        let manifest = read_manifest_from_bytes(bytes.as_bytes()).unwrap();

        assert_eq!(manifest.format_version(), FormatVersion::V0_1);
        assert_eq!(manifest.fabric_name(), "testfabric");
        assert_eq!(manifest.atom_count().get(), 100);
    }

    #[test]
    fn test_valid_full_manifest() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        let obj = value.as_object_mut().unwrap();
        obj.insert("has_up_area".into(), json!(true));
        obj.insert("has_rasters".into(), json!(true));
        obj.insert("flow_dir_encoding".into(), json!("esri"));
        obj.insert("has_snap".into(), json!(true));
        obj.insert("fabric_version".into(), json!("v2024"));
        obj.insert("fabric_level".into(), json!(8u32));
        obj.insert("region".into(), json!("North America"));
        let path = write_manifest(&dir, &value);

        let manifest = read_manifest(&path).unwrap();

        assert_eq!(
            manifest.up_area(),
            hfx_core::UpAreaAvailability::Precomputed
        );
        assert_eq!(
            manifest.rasters(),
            hfx_core::RasterAvailability::Present(FlowDirEncoding::Esri)
        );
        assert_eq!(manifest.snap(), hfx_core::SnapAvailability::Present);
        assert_eq!(manifest.fabric_version(), Some("v2024"));
        assert_eq!(manifest.fabric_level(), Some(8));
        assert_eq!(manifest.region(), Some("North America"));
    }

    #[test]
    fn test_missing_required_field() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        value.as_object_mut().unwrap().remove("format_version");
        let path = write_manifest(&dir, &value);

        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(
                err,
                SessionError::ManifestFieldMissing {
                    field: "format_version"
                }
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_invalid_json() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, b"{broken").unwrap();

        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::ManifestJsonParse { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_unsupported_topology() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        value["topology"] = json!("graph");
        let path = write_manifest(&dir, &value);

        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(
                err,
                SessionError::ManifestFieldInvalid {
                    field: "topology",
                    ..
                }
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_unsupported_crs() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        value["crs"] = json!("EPSG:32632");
        let path = write_manifest(&dir, &value);

        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::ManifestFieldInvalid { field: "crs", .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_has_rasters_requires_flow_dir_encoding() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        value
            .as_object_mut()
            .unwrap()
            .insert("has_rasters".into(), json!(true));
        // Deliberately omit flow_dir_encoding
        let path = write_manifest(&dir, &value);

        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(
                err,
                SessionError::ManifestFieldInvalid {
                    field: "flow_dir_encoding",
                    ..
                }
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_degenerate_bbox() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        // minx == maxx → degenerate on x axis
        value["bbox"] = json!([10.0, -5.0, 10.0, 5.0]);
        let path = write_manifest(&dir, &value);

        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(
                err,
                SessionError::ManifestFieldInvalid { field: "bbox", .. }
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_file_not_found() {
        let path = std::path::Path::new("/nonexistent/path/to/manifest.json");
        let err = read_manifest(path).unwrap_err();
        assert!(
            matches!(err, SessionError::Io { .. }),
            "unexpected error: {err}"
        );
    }
}
