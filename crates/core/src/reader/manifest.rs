//! Manifest reader — parses manifest.json into an hfx_core::Manifest plus
//! shed-side auxiliary declarations.
//!
//! HFX v0.2.1 hard-cut: only `format_version == "0.2.1"` and `crs ==
//! "EPSG:4326"` are accepted. The version check runs first so a v0.1 manifest
//! is rejected with a typed [`SessionError::UnsupportedFormatVersion`] before
//! any required-field parsing. Presence of snap/raster data is expressed
//! through `auxiliary[]` declarations.

use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

use hfx_core::{
    AuxiliaryDecl, AuxiliarySchemaId, BlessedAuxSchema, BoundingBox, Crs, FlowDirEncoding,
    FormatVersion, Manifest, ManifestBuilder, Topology, UnitCount,
};
use tracing::instrument;

use crate::error::SessionError;

/// The only HFX on-disk format version this engine reads.
const SUPPORTED_FORMAT_VERSION: &str = "0.2.1";
/// The only CRS this engine reads.
const SUPPORTED_CRS: &str = "EPSG:4326";

/// Parsed metadata for a blessed `hfx.aux.d8_raster.v1` declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct D8RasterDecl {
    /// Relative path (dataset-root-relative) to the flow-direction raster.
    pub flow_dir: String,
    /// Relative path (dataset-root-relative) to the flow-accumulation raster.
    pub flow_acc: String,
    /// Declared flow-direction encoding convention.
    pub flow_dir_encoding: FlowDirEncoding,
}

/// Parsed metadata for a blessed `hfx.aux.snap.v1` declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapDecl {
    /// Kebab-case name, unique across snap declarations in the dataset.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Relative path (dataset-root-relative) to the snap-feature Parquet file.
    pub snap: String,
    /// Non-empty list of HFX levels this snap file may reference.
    pub references_levels: Vec<i16>,
    /// Producer documentation for how `weight` values should be interpreted.
    pub weight_semantics: String,
}

/// A generic (non-blessed) auxiliary declaration retained as a raw handle.
///
/// shed performs structural checks only on these (path resolution + presence);
/// it does NOT parse their metadata semantically. This is the reverse-DNS /
/// provisional handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericAuxDecl {
    /// The raw schema ID string.
    pub schema: String,
    /// Artifact key → resolved dataset-root-relative path.
    pub artifacts: BTreeMap<String, String>,
    /// Raw metadata retained without semantic parsing.
    pub metadata: serde_json::Value,
}

/// shed-side classified auxiliary declarations parsed from `manifest.auxiliary[]`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuxDeclarations {
    /// Blessed D8 raster declarations.
    pub d8_rasters: Vec<D8RasterDecl>,
    /// Blessed snap declarations.
    pub snaps: Vec<SnapDecl>,
    /// Provisional / third-party declarations retained as raw handles.
    pub generic: Vec<GenericAuxDecl>,
}

/// A parsed manifest plus its classified auxiliary declarations.
#[derive(Debug, Clone)]
pub struct ParsedManifest {
    /// The validated core manifest.
    pub manifest: Manifest,
    /// shed-side classified auxiliary declarations.
    pub aux: AuxDeclarations,
}

/// Raw serde struct for deserializing manifest.json.
///
/// All fields are `Option<T>` so that field-level error reporting (rather than
/// a serde-layer failure) drives missing-required-field diagnostics.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct RawManifest {
    pub format_version: Option<String>,
    pub fabric_name: Option<String>,
    pub fabric_version: Option<String>,
    pub crs: Option<String>,
    pub has_up_area: Option<bool>,
    pub topology: Option<String>,
    pub region: Option<String>,
    pub bbox: Option<Vec<f64>>,
    pub unit_count: Option<u64>,
    pub created_at: Option<String>,
    pub adapter_version: Option<String>,
    #[serde(default)]
    pub auxiliary: Vec<RawAuxiliary>,
}

/// Raw serde struct for one `auxiliary[]` entry.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct RawAuxiliary {
    pub schema: Option<String>,
    #[serde(default)]
    pub artifacts: BTreeMap<String, String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Reads and validates `manifest.json` at `path`, returning a [`ParsedManifest`].
///
/// # Errors
///
/// | Variant | Condition |
/// |---|---|
/// | [`SessionError::Io`] | File cannot be read |
/// | [`SessionError::UnsupportedFormatVersion`] | `format_version` is not `"0.2.1"` |
/// | [`SessionError::UnsupportedCrs`] | `crs` is not `"EPSG:4326"` |
/// | [`SessionError::ManifestJsonParse`] | Bytes are not valid JSON or do not match the expected shape |
/// | [`SessionError::ManifestFieldMissing`] | A required field is absent |
/// | [`SessionError::ManifestFieldInvalid`] | A field is present but its value is invalid |
/// | [`SessionError::AuxiliaryDeclParse`] | An `auxiliary[]` entry is malformed |
#[instrument(skip_all, fields(path = %path.display()))]
pub fn read_manifest(path: &Path) -> Result<ParsedManifest, SessionError> {
    let bytes = std::fs::read(path).map_err(|e| SessionError::io("manifest.json", e))?;
    read_manifest_from_bytes(&bytes)
}

/// Reads and validates `manifest.json` bytes, returning a [`ParsedManifest`].
///
/// # Errors
///
/// See [`read_manifest`].
#[instrument(skip_all, fields(byte_len = bytes.len()))]
pub fn read_manifest_from_bytes(bytes: &[u8]) -> Result<ParsedManifest, SessionError> {
    let raw = serde_json::from_slice::<RawManifest>(bytes)
        .map_err(|source| SessionError::ManifestJsonParse { source })?;

    build_manifest(raw)
}

/// Converts a [`RawManifest`] into a validated [`ParsedManifest`].
fn build_manifest(raw: RawManifest) -> Result<ParsedManifest, SessionError> {
    // --- Format version is checked FIRST, before any required-field parsing. ---
    let format_version_str = raw
        .format_version
        .ok_or(SessionError::ManifestFieldMissing {
            field: "format_version",
        })?;
    if format_version_str != SUPPORTED_FORMAT_VERSION {
        return Err(SessionError::UnsupportedFormatVersion {
            found: format_version_str,
            expected: SUPPORTED_FORMAT_VERSION,
        });
    }
    let format_version = FormatVersion::V0_2_1;

    let fabric_name = raw.fabric_name.ok_or(SessionError::ManifestFieldMissing {
        field: "fabric_name",
    })?;

    let crs_str = raw
        .crs
        .ok_or(SessionError::ManifestFieldMissing { field: "crs" })?;
    if crs_str != SUPPORTED_CRS {
        return Err(SessionError::UnsupportedCrs {
            found: crs_str,
            expected: SUPPORTED_CRS,
        });
    }
    let crs = Crs::Epsg4326;

    let topology_str = raw
        .topology
        .ok_or(SessionError::ManifestFieldMissing { field: "topology" })?;
    let topology = Topology::from_str(&topology_str).map_err(|_| {
        SessionError::manifest_field_invalid(
            "topology",
            format!("unsupported topology {topology_str:?}, expected \"tree\" or \"dag\""),
        )
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

    let unit_count_raw = raw.unit_count.ok_or(SessionError::ManifestFieldMissing {
        field: "unit_count",
    })?;
    let unit_count = UnitCount::new(unit_count_raw)
        .map_err(|e| SessionError::manifest_field_invalid("unit_count", e.to_string()))?;

    let created_at = raw.created_at.ok_or(SessionError::ManifestFieldMissing {
        field: "created_at",
    })?;

    let adapter_version = raw
        .adapter_version
        .ok_or(SessionError::ManifestFieldMissing {
            field: "adapter_version",
        })?;

    // --- Auxiliary declarations ---
    let mut aux = AuxDeclarations::default();
    let mut aux_decls: Vec<AuxiliaryDecl> = Vec::with_capacity(raw.auxiliary.len());
    for entry in raw.auxiliary {
        let (decl, classified) = parse_auxiliary(entry)?;
        aux_decls.push(decl);
        match classified {
            ClassifiedAux::D8(d8) => aux.d8_rasters.push(d8),
            ClassifiedAux::Snap(snap) => aux.snaps.push(snap),
            ClassifiedAux::Generic(g) => aux.generic.push(g),
        }
    }

    // --- Build core manifest ---
    let mut builder = ManifestBuilder::new(
        format_version,
        fabric_name,
        crs,
        topology,
        bbox,
        unit_count,
        created_at,
        adapter_version,
    )
    .map_err(|source| SessionError::ManifestDomain { source })?;

    if raw.has_up_area.unwrap_or(false) {
        builder = builder.with_up_area();
    }
    if let Some(v) = raw.fabric_version {
        builder = builder.with_fabric_version(v);
    }
    if let Some(v) = raw.region {
        builder = builder.with_region(v);
    }
    for decl in aux_decls {
        builder = builder.with_auxiliary(decl);
    }

    Ok(ParsedManifest {
        manifest: builder.build(),
        aux,
    })
}

/// Classified, metadata-parsed auxiliary variant.
enum ClassifiedAux {
    D8(D8RasterDecl),
    Snap(SnapDecl),
    Generic(GenericAuxDecl),
}

/// Parse one `auxiliary[]` entry into both an [`AuxiliaryDecl`] for the core
/// manifest and a shed-side [`ClassifiedAux`] carrying parsed metadata.
fn parse_auxiliary(raw: RawAuxiliary) -> Result<(AuxiliaryDecl, ClassifiedAux), SessionError> {
    let schema_str = raw.schema.ok_or_else(|| SessionError::AuxiliaryDeclParse {
        schema: "<missing>".to_string(),
        reason: "auxiliary entry is missing required \"schema\" field".to_string(),
    })?;

    let schema_id =
        AuxiliarySchemaId::parse(&schema_str).map_err(|e| SessionError::AuxiliaryDeclParse {
            schema: schema_str.clone(),
            reason: e.to_string(),
        })?;

    if raw.artifacts.is_empty() {
        return Err(SessionError::AuxiliaryDeclParse {
            schema: schema_str,
            reason: "auxiliary \"artifacts\" mapping must be non-empty".to_string(),
        });
    }

    let decl = AuxiliaryDecl::new(schema_id.clone(), raw.artifacts.clone()).map_err(|e| {
        SessionError::AuxiliaryDeclParse {
            schema: schema_str.clone(),
            reason: e.to_string(),
        }
    })?;

    let classified = match &schema_id {
        AuxiliarySchemaId::Blessed(BlessedAuxSchema::D8RasterV1) => ClassifiedAux::D8(
            parse_d8_metadata(&schema_str, &raw.artifacts, &raw.metadata)?,
        ),
        AuxiliarySchemaId::Blessed(BlessedAuxSchema::SnapV1) => ClassifiedAux::Snap(
            parse_snap_metadata(&schema_str, &raw.artifacts, &raw.metadata)?,
        ),
        AuxiliarySchemaId::Provisional(_) | AuxiliarySchemaId::ThirdParty(_) => {
            // Generic handle: raw path + metadata only, no semantic parsing.
            ClassifiedAux::Generic(GenericAuxDecl {
                schema: schema_str,
                artifacts: raw.artifacts,
                metadata: raw.metadata,
            })
        }
    };

    Ok((decl, classified))
}

/// Parse the metadata block for an `hfx.aux.d8_raster.v1` declaration.
fn parse_d8_metadata(
    schema: &str,
    artifacts: &BTreeMap<String, String>,
    metadata: &serde_json::Value,
) -> Result<D8RasterDecl, SessionError> {
    let flow_dir = require_artifact(schema, artifacts, "flow_dir")?;
    let flow_acc = require_artifact(schema, artifacts, "flow_acc")?;

    let encoding_str = metadata
        .get("flow_dir_encoding")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SessionError::AuxiliaryDeclParse {
            schema: schema.to_string(),
            reason: "metadata.flow_dir_encoding must be a string".to_string(),
        })?;
    let flow_dir_encoding =
        FlowDirEncoding::from_str(encoding_str).map_err(|_| SessionError::AuxiliaryDeclParse {
            schema: schema.to_string(),
            reason: format!(
                "metadata.flow_dir_encoding {encoding_str:?} must be \"esri\" or \"taudem\""
            ),
        })?;

    Ok(D8RasterDecl {
        flow_dir,
        flow_acc,
        flow_dir_encoding,
    })
}

/// Parse the metadata block for an `hfx.aux.snap.v1` declaration.
fn parse_snap_metadata(
    schema: &str,
    artifacts: &BTreeMap<String, String>,
    metadata: &serde_json::Value,
) -> Result<SnapDecl, SessionError> {
    let snap = require_artifact(schema, artifacts, "snap")?;

    let meta_obj = metadata
        .as_object()
        .ok_or_else(|| SessionError::SnapAuxMetadataInvalid {
            name: "<unknown>".to_string(),
            reason: "metadata block must be an object".to_string(),
        })?;

    let name = meta_obj
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SessionError::SnapAuxMetadataInvalid {
            name: "<unknown>".to_string(),
            reason: "metadata.name must be a non-empty string".to_string(),
        })?
        .to_string();
    if name.is_empty() {
        return Err(SessionError::SnapAuxMetadataInvalid {
            name: "<unknown>".to_string(),
            reason: "metadata.name must be a non-empty string".to_string(),
        });
    }

    let description = meta_obj
        .get("description")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SessionError::SnapAuxMetadataInvalid {
            name: name.clone(),
            reason: "metadata.description must be a string".to_string(),
        })?
        .to_string();

    let weight_semantics = meta_obj
        .get("weight_semantics")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SessionError::SnapAuxMetadataInvalid {
            name: name.clone(),
            reason: "metadata.weight_semantics must be a string".to_string(),
        })?
        .to_string();

    let levels_raw = meta_obj
        .get("references_levels")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| SessionError::SnapAuxMetadataInvalid {
            name: name.clone(),
            reason: "metadata.references_levels must be a non-empty array".to_string(),
        })?;
    if levels_raw.is_empty() {
        return Err(SessionError::SnapAuxMetadataInvalid {
            name: name.clone(),
            reason: "metadata.references_levels must be non-empty".to_string(),
        });
    }
    let mut references_levels = Vec::with_capacity(levels_raw.len());
    for v in levels_raw {
        let n = v
            .as_i64()
            .ok_or_else(|| SessionError::SnapAuxMetadataInvalid {
                name: name.clone(),
                reason: "metadata.references_levels entries must be integers".to_string(),
            })?;
        if !(0..=i64::from(i16::MAX)).contains(&n) {
            return Err(SessionError::SnapAuxMetadataInvalid {
                name: name.clone(),
                reason: format!(
                    "metadata.references_levels entry {n} out of range [0, {}]",
                    i16::MAX
                ),
            });
        }
        references_levels.push(n as i16);
    }

    Ok(SnapDecl {
        name,
        description,
        snap,
        references_levels,
        weight_semantics,
    })
}

/// Return the artifact path for `key`, erroring if it is absent.
fn require_artifact(
    schema: &str,
    artifacts: &BTreeMap<String, String>,
    key: &str,
) -> Result<String, SessionError> {
    artifacts
        .get(key)
        .cloned()
        .ok_or_else(|| SessionError::AuxiliaryDeclParse {
            schema: schema.to_string(),
            reason: format!("missing required artifact key {key:?}"),
        })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::error::SessionError;

    fn write_manifest(dir: &TempDir, value: &serde_json::Value) -> std::path::PathBuf {
        let path = dir.path().join("manifest.json");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(value.to_string().as_bytes()).unwrap();
        path
    }

    fn minimal_json() -> serde_json::Value {
        json!({
            "format_version": "0.2.1",
            "fabric_name": "testfabric",
            "crs": "EPSG:4326",
            "topology": "tree",
            "bbox": [-10.0, -5.0, 10.0, 5.0],
            "unit_count": 100,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "hfx-adapter-v1"
        })
    }

    #[test]
    fn test_valid_minimal_manifest() {
        let dir = TempDir::new().unwrap();
        let path = write_manifest(&dir, &minimal_json());

        let parsed = read_manifest(&path).unwrap();
        let manifest = parsed.manifest;

        assert_eq!(manifest.format_version(), FormatVersion::V0_2_1);
        assert_eq!(manifest.fabric_name(), "testfabric");
        assert_eq!(manifest.crs(), Crs::Epsg4326);
        assert_eq!(manifest.topology(), Topology::Tree);
        assert_eq!(manifest.unit_count().get(), 100);
        assert_eq!(manifest.created_at(), "2026-01-01T00:00:00Z");
        assert_eq!(manifest.adapter_version(), "hfx-adapter-v1");
        assert!(parsed.aux.snaps.is_empty());
        assert!(parsed.aux.d8_rasters.is_empty());
    }

    #[test]
    fn test_v01_format_version_rejected_before_missing_fields() {
        let dir = TempDir::new().unwrap();
        // v0.1 manifest: also omits unit_count, but version check must fire first.
        let value = json!({
            "format_version": "0.1",
            "fabric_name": "testfabric"
        });
        let path = write_manifest(&dir, &value);
        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::UnsupportedFormatVersion { ref found, .. } if found == "0.1"),
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
            matches!(err, SessionError::UnsupportedCrs { .. }),
            "got {err}"
        );
    }

    #[test]
    fn test_d8_and_snap_auxiliary_parsed() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        value["auxiliary"] = json!([
            {
                "schema": "hfx.aux.d8_raster.v1",
                "artifacts": { "flow_dir": "flow_dir.tif", "flow_acc": "flow_acc.tif" },
                "metadata": { "flow_dir_encoding": "esri" }
            },
            {
                "schema": "hfx.aux.snap.v1",
                "artifacts": { "snap": "snap/segment_stems.parquet" },
                "metadata": {
                    "name": "segment-stems",
                    "description": "Segment stems.",
                    "references_levels": [0],
                    "weight_semantics": "higher is stronger"
                }
            }
        ]);
        let path = write_manifest(&dir, &value);
        let parsed = read_manifest(&path).unwrap();
        assert_eq!(parsed.aux.d8_rasters.len(), 1);
        assert_eq!(parsed.aux.d8_rasters[0].flow_dir, "flow_dir.tif");
        assert_eq!(
            parsed.aux.d8_rasters[0].flow_dir_encoding,
            FlowDirEncoding::Esri
        );
        assert_eq!(parsed.aux.snaps.len(), 1);
        assert_eq!(parsed.aux.snaps[0].name, "segment-stems");
        assert_eq!(parsed.aux.snaps[0].references_levels, vec![0]);
    }

    #[test]
    fn test_missing_required_field() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        value.as_object_mut().unwrap().remove("unit_count");
        let path = write_manifest(&dir, &value);
        let err = read_manifest(&path).unwrap_err();
        assert!(
            matches!(
                err,
                SessionError::ManifestFieldMissing {
                    field: "unit_count"
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
            "got {err}"
        );
    }

    #[test]
    fn test_generic_aux_retained_as_handle() {
        let dir = TempDir::new().unwrap();
        let mut value = minimal_json();
        value["auxiliary"] = json!([
            {
                "schema": "org.example.custom.v1",
                "artifacts": { "data": "extra/custom.bin" },
                "metadata": { "anything": 42 }
            }
        ]);
        let path = write_manifest(&dir, &value);
        let parsed = read_manifest(&path).unwrap();
        assert_eq!(parsed.aux.generic.len(), 1);
        assert_eq!(parsed.aux.generic[0].schema, "org.example.custom.v1");
    }
}
