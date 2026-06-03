//! Core library for the shed watershed extraction engine.

pub mod algo;
#[allow(dead_code)]
pub(crate) mod assembly;
pub(crate) mod cache;
pub(crate) mod cog;
pub mod engine;
pub mod error;
pub mod parquet_cache;
#[allow(dead_code)]
pub(crate) mod raster_cache;
pub mod reader;
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

pub use engine::{
    DelineationOptions, DelineationResult, Engine, EngineBuilder, EngineError, RefinementOutcome,
};
pub use error::SessionError;
pub use resolver::{
    OutletResolutionError, PipTieBreak, ResolutionMethod, ResolvedOutlet, ResolverConfig,
    SearchRadiusMetres, SnapStrategy, resolve_outlet,
};
pub use source::DatasetSource;
pub use staged::{
    DissolvedWatershed, LevelResolvedOutlet, LevelSelection, PreMergeDrainageUnit,
    PreMergeDrainageUnits, RefinementMode, SameLevelUpstreamUnits, SelectedLevel,
    TerminalRefinement,
};
