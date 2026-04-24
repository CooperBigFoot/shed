//! Error types for dataset session operations.

/// Errors that can occur while opening or reading an HFX dataset session.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// Fired when the supplied path exists but is not a directory, or does not
    /// exist at all.
    #[error("dataset root not found or not a directory: {path}")]
    RootNotFound {
        /// The path that was checked.
        path: String,
    },

    /// Fired when a file that must be present for a valid HFX dataset is
    /// absent from the dataset root.
    #[error("required artifact {artifact:?} not found at {path}")]
    RequiredArtifactMissing {
        /// Short name of the artifact (e.g. `"catchments.parquet"`).
        artifact: &'static str,
        /// Absolute path that was checked.
        path: String,
    },

    /// Fired when the manifest declares an optional artifact as present but
    /// the corresponding file is missing from disk.
    #[error("optional artifact {artifact:?} declared in manifest but missing at {path}")]
    OptionalArtifactMissing {
        /// Short name of the artifact (e.g. `"snap.parquet"`).
        artifact: &'static str,
        /// Absolute path that was checked.
        path: String,
    },

    /// Fired when a file I/O operation fails while reading an artifact.
    #[error("I/O error reading artifact {artifact:?}: {source}")]
    Io {
        /// Short name of the artifact being read.
        artifact: &'static str,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// Fired when `manifest.json` exists but contains invalid JSON.
    #[error("failed to parse manifest.json as JSON: {source}")]
    ManifestJsonParse {
        /// Underlying serde_json error.
        source: serde_json::Error,
    },

    /// Fired when a required top-level field is absent from the parsed
    /// manifest JSON object.
    #[error("required manifest field {field:?} is missing")]
    ManifestFieldMissing {
        /// The field name that was absent.
        field: &'static str,
    },

    /// Fired when a manifest field is present but its value does not satisfy
    /// the HFX contract (wrong type, out of range, unrecognised enum variant,
    /// etc.).
    #[error("manifest field {field:?} has an invalid value: {reason}")]
    ManifestFieldInvalid {
        /// The field name that failed validation.
        field: &'static str,
        /// Human-readable description of why the value was rejected.
        reason: String,
    },

    /// Fired when the parsed manifest fields are individually valid but
    /// `hfx_core::ManifestBuilder::new` or `.build()` rejects the combination
    /// (e.g. terminal_sink_id != 0, uppercase fabric name).
    #[error("manifest domain validation failed: {source}")]
    ManifestDomain {
        /// Underlying domain error from `hfx_core`.
        source: hfx_core::ManifestError,
    },

    /// Fired when `graph.arrow` exists but cannot be decoded as Arrow IPC.
    #[error("failed to parse graph.arrow as Arrow IPC: {source}")]
    GraphArrowParse {
        /// Underlying Arrow error.
        source: arrow::error::ArrowError,
    },

    /// Fired when the Arrow IPC record batch has the wrong schema — a required
    /// column is missing or has the wrong data type.
    #[error("graph.arrow schema error: {reason}")]
    GraphSchema {
        /// Human-readable description of the schema mismatch.
        reason: String,
    },

    /// Fired when the Arrow record batch is valid but
    /// `hfx_core::DrainageGraph::new` rejects the content (e.g. duplicate
    /// atom IDs, empty graph).
    #[error("graph domain validation failed: {source}")]
    GraphDomain {
        /// Underlying domain error from `hfx_core`.
        source: hfx_core::GraphError,
    },

    /// Fired when a Parquet file exists but cannot be opened or decoded by
    /// the Parquet reader.
    #[error("failed to parse {artifact:?} as Parquet: {source}")]
    ParquetParse {
        /// Short name of the artifact (e.g. `"catchments.parquet"`).
        artifact: &'static str,
        /// Underlying Parquet error.
        source: parquet::errors::ParquetError,
    },

    /// Fired when the Parquet file is structurally valid but its schema does
    /// not match the expected HFX column layout (wrong column name, wrong
    /// physical type, missing required column).
    #[error("schema error in {artifact:?}: {reason}")]
    ParquetSchema {
        /// Short name of the artifact.
        artifact: &'static str,
        /// Human-readable description of the schema mismatch.
        reason: String,
    },

    /// Fired when a specific row in a Parquet artifact fails domain-level
    /// validation after its raw values have been decoded.
    #[error("invalid row {row} in {artifact:?}: {detail}")]
    InvalidRow {
        /// Short name of the artifact.
        artifact: &'static str,
        /// Zero-based row index within the artifact.
        row: usize,
        /// Human-readable description of what was wrong.
        detail: String,
    },

    /// Fired when the manifest declares a specific atom count but the actual
    /// number of rows in `catchments.parquet` differs.
    #[error(
        "atom count mismatch: manifest declares {manifest_count} atoms but file contains {actual_count}"
    )]
    AtomCountMismatch {
        /// The count declared in `manifest.json`.
        manifest_count: u64,
        /// The actual row count found in the Parquet file.
        actual_count: u64,
    },

    /// Fired when reading a specific row group from a Parquet file fails at
    /// runtime (e.g. decompression error, I/O error mid-scan).
    #[error("error reading row group {row_group} of {artifact:?}: {source}")]
    RowGroupReadError {
        /// Short name of the artifact.
        artifact: &'static str,
        /// Zero-based row group index.
        row_group: usize,
        /// Underlying Parquet error.
        source: parquet::errors::ParquetError,
    },

    /// Fired when referential integrity between dataset artifacts is violated —
    /// for example, a graph atom ID that has no corresponding catchment row.
    #[error("integrity violation: {detail}")]
    IntegrityViolation {
        /// Human-readable description of the integrity failure.
        detail: String,
    },

    /// Fired when a dataset source string looks like a URL but is malformed,
    /// or a URL lacks source-specific required pieces.
    #[error("invalid dataset source {input:?}: {reason}")]
    InvalidDatasetSource {
        /// The dataset source string supplied by the caller.
        input: String,
        /// Human-readable description of why parsing failed.
        reason: String,
    },

    /// Fired when a dataset source URL is well-formed but not supported by
    /// the current runtime.
    #[error("unsupported dataset source {input:?}: {reason}")]
    UnsupportedDatasetSource {
        /// The dataset source string supplied by the caller.
        input: String,
        /// Human-readable description of the unsupported capability.
        reason: String,
    },

    /// Fired when a remote dataset URL path cannot be represented as an
    /// object-store path prefix.
    #[error("invalid remote dataset path in {input:?}: {source}")]
    DatasetSourcePath {
        /// The dataset source string supplied by the caller.
        input: String,
        /// Underlying object-store path error.
        source: object_store::path::Error,
    },

    /// Fired when object-store configuration derived from a supported dataset
    /// source cannot be built.
    #[error("failed to configure object store for dataset source {input:?}: {source}")]
    ObjectStoreConfig {
        /// The dataset source string supplied by the caller.
        input: String,
        /// Underlying object-store configuration error.
        source: object_store::Error,
    },

    /// Fired when a remote dataset source parses successfully but the session
    /// reader does not yet support loading remote artifacts.
    #[error("remote dataset opening is not yet supported for {url}")]
    RemoteDatasetNotSupported {
        /// The remote dataset URL supplied by the caller.
        url: String,
    },
}

impl SessionError {
    /// Construct an [`SessionError::Io`] variant.
    pub(crate) fn io(artifact: &'static str, source: std::io::Error) -> Self {
        Self::Io { artifact, source }
    }

    /// Construct a [`SessionError::RequiredArtifactMissing`] variant.
    pub(crate) fn required_missing(artifact: &'static str, path: impl Into<String>) -> Self {
        Self::RequiredArtifactMissing {
            artifact,
            path: path.into(),
        }
    }

    /// Construct a [`SessionError::OptionalArtifactMissing`] variant.
    pub(crate) fn optional_missing(artifact: &'static str, path: impl Into<String>) -> Self {
        Self::OptionalArtifactMissing {
            artifact,
            path: path.into(),
        }
    }

    /// Construct a [`SessionError::ManifestFieldInvalid`] variant.
    pub(crate) fn manifest_field_invalid(field: &'static str, reason: impl Into<String>) -> Self {
        Self::ManifestFieldInvalid {
            field,
            reason: reason.into(),
        }
    }

    /// Construct a [`SessionError::ParquetSchema`] variant.
    pub(crate) fn parquet_schema(artifact: &'static str, reason: impl Into<String>) -> Self {
        Self::ParquetSchema {
            artifact,
            reason: reason.into(),
        }
    }

    /// Construct a [`SessionError::GraphSchema`] variant.
    pub(crate) fn graph_schema(reason: impl Into<String>) -> Self {
        Self::GraphSchema {
            reason: reason.into(),
        }
    }

    /// Construct a [`SessionError::InvalidRow`] variant.
    pub(crate) fn invalid_row(
        artifact: &'static str,
        row: usize,
        detail: impl Into<String>,
    ) -> Self {
        Self::InvalidRow {
            artifact,
            row,
            detail: detail.into(),
        }
    }

    /// Construct a [`SessionError::IntegrityViolation`] variant.
    pub(crate) fn integrity(detail: impl Into<String>) -> Self {
        Self::IntegrityViolation {
            detail: detail.into(),
        }
    }
}
