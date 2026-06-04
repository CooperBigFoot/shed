//! Export identity values for basin GeoParquet rows.

use std::fmt;
use std::str::FromStr;

use hfx_core::{Manifest, UnitId};

use crate::export::ExportError;

const MAX_BASIN_ID_BYTES: usize = 128;

/// Path-safe caller-owned basin identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BasinId(String);

impl BasinId {
    /// Parse a basin identifier from caller input.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | value violates the documented allowlist | [`ExportError::InvalidBasinId`] |
    pub fn parse(value: impl Into<String>) -> Result<Self, ExportError> {
        let value = value.into();
        validate_basin_id(&value)?;
        Ok(Self(value))
    }

    /// Return the identifier string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Build the narrow single-fabric default basin ID from a typed terminal unit.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | decimal ID violates the basin ID allowlist | [`ExportError::InvalidBasinId`] |
    pub fn from_terminal_unit_id(unit_id: UnitId) -> Result<Self, ExportError> {
        Self::from_terminal_unit_id_raw(unit_id.get())
    }

    /// Build the narrow single-fabric default basin ID from a raw terminal unit.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | `terminal_unit_id < 0` | [`ExportError::NegativeDefaultBasinId`] |
    /// | decimal ID violates the basin ID allowlist | [`ExportError::InvalidBasinId`] |
    pub fn from_terminal_unit_id_raw(terminal_unit_id: i64) -> Result<Self, ExportError> {
        if terminal_unit_id < 0 {
            return Err(ExportError::NegativeDefaultBasinId { terminal_unit_id });
        }
        Self::parse(terminal_unit_id.to_string())
    }
}

impl fmt::Display for BasinId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BasinId {
    type Err = ExportError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Free-form delineation label stored as a column value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DelineationLabel(String);

impl DelineationLabel {
    /// Parse a caller-supplied delineation label.
    ///
    /// # Errors
    ///
    /// This constructor currently accepts any non-empty label. Empty labels are
    /// rejected with [`ExportError::InvalidBasinId`] until writer-specific
    /// duplicate-row validation lands.
    pub fn parse(value: impl Into<String>) -> Result<Self, ExportError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ExportError::InvalidBasinId {
                value,
                reason: "delineation label must not be empty",
            });
        }
        Ok(Self(value))
    }

    /// Return the label string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Build the default `{fabric_name}/{fabric_version}/{method}` label.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | fabric identity has no fabric version | [`ExportError::MissingFabricVersion`] |
    pub fn from_fabric_identity(
        identity: &FabricIdentity,
        method: &ExportMethod,
    ) -> Result<Self, ExportError> {
        let version =
            identity
                .fabric_version()
                .ok_or_else(|| ExportError::MissingFabricVersion {
                    fabric_name: identity.fabric_name().to_owned(),
                })?;
        Ok(Self(format!(
            "{}/{}/{}",
            identity.fabric_name(),
            version,
            method.as_str()
        )))
    }
}

impl fmt::Display for DelineationLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DelineationLabel {
    type Err = ExportError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Source fabric identity used to construct default delineation labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricIdentity {
    fabric_name: String,
    fabric_version: Option<String>,
    adapter_version: String,
}

impl FabricIdentity {
    /// Copy fabric identity fields from an HFX manifest.
    pub fn from_manifest(manifest: &Manifest) -> Self {
        Self {
            fabric_name: manifest.fabric_name().to_owned(),
            fabric_version: manifest.fabric_version().map(str::to_owned),
            adapter_version: manifest.adapter_version().to_owned(),
        }
    }

    /// Construct fabric identity fields directly for tests and explicit callers.
    pub fn new(
        fabric_name: impl Into<String>,
        fabric_version: Option<String>,
        adapter_version: impl Into<String>,
    ) -> Self {
        Self {
            fabric_name: fabric_name.into(),
            fabric_version,
            adapter_version: adapter_version.into(),
        }
    }

    /// Return the source fabric name.
    pub fn fabric_name(&self) -> &str {
        &self.fabric_name
    }

    /// Return the source fabric data version used by default labels.
    pub fn fabric_version(&self) -> Option<&str> {
        self.fabric_version.as_deref()
    }

    /// Return the adapter version used only as provenance.
    pub fn adapter_version(&self) -> &str {
        &self.adapter_version
    }
}

/// Export method label component for default delineation values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExportMethod(String);

impl ExportMethod {
    /// Parse a caller-supplied export method label.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | value is empty | [`ExportError::InvalidBasinId`] |
    pub fn parse(value: impl Into<String>) -> Result<Self, ExportError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ExportError::InvalidBasinId {
                value,
                reason: "export method must not be empty",
            });
        }
        Ok(Self(value))
    }

    /// Return the default best-effort D8 method label.
    pub fn d8_best_effort() -> Self {
        Self("d8-best-effort".to_owned())
    }

    /// Return the default required/applied D8 method label.
    pub fn d8_required() -> Self {
        Self("d8-required".to_owned())
    }

    /// Return the default no-refinement method label.
    pub fn no_refine() -> Self {
        Self("no-refine".to_owned())
    }

    /// Return the stable label used in `delineation`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExportMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Caller/outlet context included in export diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportOrigin {
    description: String,
}

impl ExportOrigin {
    /// Create a diagnostic origin description.
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
        }
    }

    /// Return the diagnostic origin description.
    pub fn as_str(&self) -> &str {
        &self.description
    }
}

impl fmt::Display for ExportOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn validate_basin_id(value: &str) -> Result<(), ExportError> {
    let reason = if value.is_empty() {
        Some("basin_id must not be empty")
    } else if value.len() > MAX_BASIN_ID_BYTES {
        Some("basin_id must be at most 128 bytes")
    } else if value == "." || value == ".." {
        Some("basin_id must not be . or ..")
    } else if value.ends_with('.') {
        Some("basin_id must not end with a dot")
    } else if value.ends_with(' ') {
        Some("basin_id must not end with a space")
    } else if is_windows_device_name(value) {
        Some("basin_id must not be a Windows device name")
    } else if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        Some("basin_id must match ^[A-Za-z0-9._-]+$")
    } else {
        None
    };

    match reason {
        Some(reason) => Err(ExportError::InvalidBasinId {
            value: value.to_owned(),
            reason,
        }),
        None => Ok(()),
    }
}

fn is_windows_device_name(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || upper
            .strip_prefix("COM")
            .and_then(single_ascii_digit_1_to_9)
            .is_some()
        || upper
            .strip_prefix("LPT")
            .and_then(single_ascii_digit_1_to_9)
            .is_some()
}

fn single_ascii_digit_1_to_9(value: &str) -> Option<()> {
    match value.as_bytes() {
        [b'1'..=b'9'] => Some(()),
        _ => None,
    }
}

#[cfg(test)]
mod export_identity_tests {
    use hfx_core::{BoundingBox, Crs, FormatVersion, ManifestBuilder, Topology, UnitCount, UnitId};

    use super::*;

    #[test]
    fn export_identity_basin_id_accepts_allowlist_values() {
        for value in ["abc", "ABC", "a.b_c-1", "Z9._-"] {
            let id = BasinId::parse(value).unwrap();
            assert_eq!(id.as_str(), value);
        }
    }

    #[test]
    fn export_identity_basin_id_rejects_unsafe_and_reserved_values() {
        for value in [
            "", ".", "..", "CON", "prn", "AUX", "nul", "COM1", "com9", "LPT1", "lpt9", "a/b",
            "a\\b", "é",
        ] {
            assert!(matches!(
                BasinId::parse(value),
                Err(ExportError::InvalidBasinId { .. })
            ));
        }
    }

    #[test]
    fn export_identity_basin_id_rejects_equals() {
        assert!(matches!(
            BasinId::parse("basin=1"),
            Err(ExportError::InvalidBasinId { .. })
        ));
    }

    #[test]
    fn export_identity_basin_id_rejects_trailing_dot() {
        assert!(matches!(
            BasinId::parse("basin."),
            Err(ExportError::InvalidBasinId { .. })
        ));
    }

    #[test]
    fn export_identity_caller_supplied_valid_id() {
        let id: BasinId = "caller.owned_01".parse().unwrap();
        assert_eq!(id.to_string(), "caller.owned_01");
    }

    #[test]
    fn export_identity_missing_fabric_version_default_label_error() {
        let identity = FabricIdentity::new("grit", None, "adapter-v1");
        let err =
            DelineationLabel::from_fabric_identity(&identity, &ExportMethod::d8_best_effort())
                .unwrap_err();
        assert!(matches!(
            err,
            ExportError::MissingFabricVersion { fabric_name } if fabric_name == "grit"
        ));
    }

    #[test]
    fn export_identity_adapter_version_is_provenance_only() {
        let first = FabricIdentity::new("grit", Some("2024.1".to_owned()), "adapter-a");
        let second = FabricIdentity::new("grit", Some("2024.1".to_owned()), "adapter-b");
        let first_label =
            DelineationLabel::from_fabric_identity(&first, &ExportMethod::d8_best_effort())
                .unwrap();
        let second_label =
            DelineationLabel::from_fabric_identity(&second, &ExportMethod::d8_best_effort())
                .unwrap();

        assert_eq!(first_label, second_label);
        assert_eq!(first.adapter_version(), "adapter-a");
        assert_eq!(second.adapter_version(), "adapter-b");
    }

    #[test]
    fn export_identity_default_terminal_unit_id_formatting() {
        let id = BasinId::from_terminal_unit_id(UnitId::new(42).unwrap()).unwrap();
        assert_eq!(id.as_str(), "42");
    }

    #[test]
    fn export_identity_negative_terminal_unit_default_rejection() {
        let err = BasinId::from_terminal_unit_id_raw(-7).unwrap_err();
        assert!(matches!(
            err,
            ExportError::NegativeDefaultBasinId {
                terminal_unit_id: -7
            }
        ));
    }

    #[test]
    fn export_identity_same_basin_id_allowed_with_distinct_delineation_label() {
        let basin = BasinId::parse("rhine-basel").unwrap();
        let grit = DelineationLabel::parse("grit/2.0.0/d8-best-effort").unwrap();
        let merit = DelineationLabel::parse("merit/2024.1/d8-carved").unwrap();

        assert_eq!(basin.as_str(), "rhine-basel");
        assert_ne!(grit, merit);
    }

    #[test]
    fn export_identity_fabric_identity_copies_manifest_fields() {
        let manifest = ManifestBuilder::new(
            FormatVersion::V0_2_1,
            "grit",
            Crs::Epsg4326,
            Topology::Tree,
            BoundingBox::new(-1.0, -1.0, 1.0, 1.0).unwrap(),
            UnitCount::new(1).unwrap(),
            "2026-06-04T00:00:00Z",
            "adapter-v1",
        )
        .unwrap()
        .with_fabric_version("2.0.0")
        .build();
        let identity = FabricIdentity::from_manifest(&manifest);

        assert_eq!(identity.fabric_name(), "grit");
        assert_eq!(identity.fabric_version(), Some("2.0.0"));
        assert_eq!(identity.adapter_version(), "adapter-v1");
    }
}
