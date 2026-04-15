//! Pour-point snap threshold.
//!
//! A newtype wrapping a `u32` pixel count. Source-fabric-specific conversions
//! (e.g. from MERIT pixel area) are intentionally omitted here; only the
//! generic constructors and accessors are provided.

/// Minimum upstream-pixel count used when snapping a pour point to the stream
/// network.
///
/// Wraps a `u32` representing the number of upstream pixels a cell must drain
/// to be considered part of the stream network. The pixel area depends on the
/// source raster and is left to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapThreshold(u32);

impl SnapThreshold {
    /// Default snap threshold aligned with the HFX spec (1,000 upstream cells).
    ///
    /// Used when the caller does not supply an explicit threshold.
    /// See HFX v0.1 §5: "configurable, default: 1,000 cells".
    pub const DEFAULT: Self = Self(1000);

    /// Creates a new threshold from a raw pixel count.
    pub fn new(pixels: u32) -> Self {
        Self(pixels)
    }

    /// Returns the raw pixel count.
    pub fn pixels(self) -> u32 {
        self.0
    }

    /// Returns the pixel count as an `f32`.
    pub fn as_f32(self) -> f32 {
        self.0 as f32
    }
}

#[cfg(test)]
mod tests {
    use super::SnapThreshold;

    #[test]
    fn default_constant() {
        assert_eq!(SnapThreshold::DEFAULT.pixels(), 1000);
        assert_eq!(SnapThreshold::DEFAULT.as_f32(), 1000.0f32);
    }

    #[test]
    fn new_and_pixels_round_trip() {
        assert_eq!(SnapThreshold::new(42).pixels(), 42);
    }

    #[test]
    fn as_f32_conversion() {
        assert_eq!(SnapThreshold::new(500).as_f32(), 500.0f32);
    }

    #[test]
    fn new_zero() {
        assert_eq!(SnapThreshold::new(0).pixels(), 0);
    }

    #[test]
    fn new_max() {
        assert_eq!(SnapThreshold::new(u32::MAX).pixels(), u32::MAX);
    }

    #[test]
    fn ordering() {
        assert!(SnapThreshold::new(100) < SnapThreshold::new(200));
        assert!(SnapThreshold::new(100) < SnapThreshold::DEFAULT);
    }
}
