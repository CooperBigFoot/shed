//! Core library for the shed watershed extraction engine.

pub mod algo;
#[allow(dead_code)]
pub(crate) mod assembly;
pub mod error;
pub mod reader;
pub mod resolver;
pub mod session;

#[cfg(test)]
mod testutil;

pub use error::SessionError;
pub use resolver::{
    OutletResolutionError, PipTieBreak, ResolutionMethod, ResolvedOutlet, ResolverConfig,
    SearchRadiusMetres, resolve_outlet,
};
