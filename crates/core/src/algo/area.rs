//! Area in square kilometres.

use std::fmt;

/// Area measured in square kilometres.
///
/// Wraps an `f64` value representing area. Does NOT derive `Eq`/`Ord`
/// because `f64` does not support them soundly.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct AreaKm2(f64);

impl AreaKm2 {
    /// Create a new area value.
    pub fn new(km2: f64) -> Self {
        Self(km2)
    }

    /// Return the raw `f64` value.
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

impl fmt::Display for AreaKm2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} km²", self.0)
    }
}

impl From<hfx_core::AreaKm2> for AreaKm2 {
    fn from(a: hfx_core::AreaKm2) -> Self {
        AreaKm2::new(a.get() as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_as_f64() {
        let a = AreaKm2::new(42.5);
        assert_eq!(a.as_f64(), 42.5);
    }

    #[test]
    fn display() {
        let a = AreaKm2::new(100.0);
        assert_eq!(format!("{a}"), "100 km²");
    }

    #[test]
    fn partial_ord() {
        assert!(AreaKm2::new(10.0) < AreaKm2::new(20.0));
        assert!(AreaKm2::new(20.0) > AreaKm2::new(10.0));
    }

    #[test]
    fn equality() {
        assert_eq!(AreaKm2::new(5.0), AreaKm2::new(5.0));
        assert_ne!(AreaKm2::new(5.0), AreaKm2::new(6.0));
    }

    #[test]
    fn from_hfx_core_area_km2() {
        let hfx_area = hfx_core::AreaKm2::new(123.45).unwrap();
        let area: AreaKm2 = hfx_area.into();
        assert!(
            (area.as_f64() - 123.45_f64).abs() < 1e-4,
            "expected ≈123.45, got {}",
            area.as_f64()
        );
    }
}
