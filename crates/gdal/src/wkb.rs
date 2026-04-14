//! WKB decoding re-exported from `shed-core`.
//!
//! This module re-exports WKB decoding from `shed_core::algo::wkb` for
//! convenience. The implementation is pure Rust with no GDAL dependency.

pub use shed_core::algo::wkb::{WkbDecodeError, decode_wkb, decode_wkb_multi_polygon, decode_wkb_polygon};
