//! Core library for the shed watershed extraction engine.

// Pre-existing thiserror enums exceed clippy's 128-byte result_large_err
// threshold (surfaced under clippy 1.93.0 once CI could resolve hfx-core).
// Boxing the large Err variants is deferred tech debt; suppress crate-wide.
#![allow(clippy::result_large_err)]

pub mod algo;
#[allow(dead_code)]
pub(crate) mod assembly;
pub(crate) mod cache;
pub(crate) mod cog;
pub mod engine;
pub mod error;
pub mod export;
pub mod parquet_cache;
#[allow(dead_code)]
pub(crate) mod raster_cache;
pub mod reader;
pub mod refinement;
pub mod resolver;
#[allow(dead_code)]
pub(crate) mod runtime;
pub mod session;
pub mod source;
pub mod source_telemetry;
pub mod staged;
pub mod telemetry;

#[cfg(feature = "test-fixtures")]
pub mod test_raster_source;

#[cfg(any(test, feature = "test-fixtures"))]
#[allow(deprecated)]
pub mod testutil;

pub use cog::LocalizedRasterWindow;
pub use engine::{
    DelineationOptions, DelineationResult, DelineationUnitMetadata, Engine, EngineBuilder,
    EngineError, RefinementOutcome,
};
pub use error::SessionError;
pub use export::{
    BasinBbox, BasinCentroid, BasinExportSchemaProfile, BasinId, BasinSpatialSortKey,
    DelineationLabel, ExportError, ExportMethod, ExportOrigin, FabricIdentity, HilbertIndex,
    RowGroupPlan, UnitBundleExportInput, UnitBundleExportOptions, UnitBundleGeoParquetWriter,
    UnitBundleSpatialSortKey,
};
pub use refinement::{
    AppliedRefinementReason, BestEffortSkipReason, ContainedTerminalPolygon, D8RasterHandle,
    D8RasterRefinementStrategy, D8RefinementPantry, RefinementProvenance, RefinementStrategyName,
    TerminalRefinementDecision, TerminalRefinementError, TerminalRefinementInput,
    TerminalRefinementStrategy,
};
pub use resolver::{
    OutletResolutionError, PipTieBreak, ResolutionMethod, ResolvedOutlet, ResolverConfig,
    SearchRadiusMetres, SnapStrategy, resolve_outlet, resolve_outlet_at_level,
};
pub use source::DatasetSource;
pub use staged::{
    DissolvedWatershed, LevelResolvedOutlet, LevelSelection, PreMergeDrainageUnit,
    PreMergeDrainageUnits, RefinementMode, SameLevelUpstreamUnits, SelectedLevel,
    TerminalRefinement,
};
