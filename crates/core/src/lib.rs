//! Core library for the shed watershed extraction engine.

pub mod algo;
pub mod error;
pub mod reader;
pub mod resolver;
pub mod session;

#[cfg(test)]
mod testutil;

pub use error::SessionError;
pub use resolver::{
    OutletResolutionError, PipTieBreak, ResolvedOutlet, ResolverConfig, ResolutionMethod,
    SearchRadiusMetres, resolve_outlet,
};
