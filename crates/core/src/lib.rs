//! Core library for the shed watershed extraction engine.

pub mod algo;
#[allow(dead_code)]
pub(crate) mod assembly;
pub mod engine;
pub mod error;
pub mod reader;
pub mod resolver;
pub mod session;

#[cfg(any(test, feature = "test-fixtures"))]
pub mod testutil;

pub use engine::{
    DelineationOptions, DelineationResult, Engine, EngineBuilder, EngineError, RefinementOutcome,
};
pub use error::SessionError;
pub use resolver::{
    OutletResolutionError, PipTieBreak, ResolutionMethod, ResolvedOutlet, ResolverConfig,
    SearchRadiusMetres, SnapStrategy, resolve_outlet,
};
