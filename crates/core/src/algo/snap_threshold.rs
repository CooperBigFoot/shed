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
    /// Default threshold for a single, isolated outlet.
    pub const FALLBACK_SINGLE: Self = Self(500);

    /// Default threshold when processing multiple outlets simultaneously.
    pub const FALLBACK_MULTI: Self = Self(5000);

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
    fn constants() {
        assert_eq!(SnapThreshold::FALLBACK_SINGLE.pixels(), 500);
        assert_eq!(SnapThreshold::FALLBACK_MULTI.pixels(), 5000);
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
    fn fallback_single_value() {
        assert_eq!(SnapThreshold::FALLBACK_SINGLE.pixels(), 500);
        assert_eq!(SnapThreshold::FALLBACK_SINGLE.as_f32(), 500.0f32);
    }

    #[test]
    fn fallback_multi_value() {
        assert_eq!(SnapThreshold::FALLBACK_MULTI.pixels(), 5000);
        assert_eq!(SnapThreshold::FALLBACK_MULTI.as_f32(), 5000.0f32);
    }

    #[test]
    fn ordering() {
        assert!(SnapThreshold::new(100) < SnapThreshold::new(200));
        assert!(SnapThreshold::FALLBACK_SINGLE < SnapThreshold::FALLBACK_MULTI);
    }
}
