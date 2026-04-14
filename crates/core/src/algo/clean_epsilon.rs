//! Epsilon value for topological cleaning operations.

/// Epsilon value for topology cleaning, in degrees.
///
/// Used as the buffer distance when cleaning topological artifacts via
/// buffer-unbuffer operations on polygon geometries.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct CleanEpsilon(f64);

impl CleanEpsilon {
    /// Creates a new epsilon value.
    pub const fn new(value: f64) -> Self {
        Self(value)
    }

    /// Returns the raw `f64` value.
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

/// Default epsilon for topology cleaning, in degrees (~1 m at the equator).
pub const DEFAULT_CLEANING_EPSILON: CleanEpsilon = CleanEpsilon::new(0.00001);

#[cfg(test)]
mod tests {
    use super::{CleanEpsilon, DEFAULT_CLEANING_EPSILON};

    #[test]
    fn construction_and_accessor() {
        let eps = CleanEpsilon::new(0.001);
        assert_eq!(eps.as_f64(), 0.001);
    }

    #[test]
    fn default_value() {
        assert_eq!(DEFAULT_CLEANING_EPSILON.as_f64(), 0.00001);
    }

    #[test]
    fn const_construction() {
        const EPS: CleanEpsilon = CleanEpsilon::new(0.5);
        assert_eq!(EPS.as_f64(), 0.5);
    }

    #[test]
    fn equality() {
        assert_eq!(CleanEpsilon::new(0.1), CleanEpsilon::new(0.1));
        assert_ne!(CleanEpsilon::new(0.1), CleanEpsilon::new(0.2));
    }
}
