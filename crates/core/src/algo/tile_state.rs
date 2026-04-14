//! Typestate markers for tile masking status.

/// Marker type for tiles that have not been masked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Raw;

/// Marker type for tiles that have been masked with a catchment mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Masked;
