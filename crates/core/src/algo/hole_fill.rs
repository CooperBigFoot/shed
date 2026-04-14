//! Remove small interior holes from multi-polygon geometries.

use geo::{Area, LineString, MultiPolygon, Polygon};
use tracing::{debug, instrument};

/// Default fill threshold in pixels — holes smaller than this are removed.
pub const DEFAULT_FILL_THRESHOLD_PX: u32 = 100;

/// Mode for interior hole removal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HoleFillMode {
    /// Remove all interior holes unconditionally.
    RemoveAll,
    /// Remove holes smaller than the given pixel count.
    ///
    /// The area threshold is `threshold_pixels as f64 * pixel_area`.
    /// The caller computes `pixel_area` from [`crate::algo::geo_transform::GeoTransform::pixel_area`].
    BelowThreshold {
        /// Number of pixels below which a hole is removed.
        threshold_pixels: u32,
        /// Area of a single raster pixel in the CRS coordinate units squared.
        pixel_area: f64,
    },
}

/// Remove interior holes from a multi-polygon according to `mode`.
///
/// For each polygon in `geom`, interior rings are filtered based on `mode`:
/// - [`HoleFillMode::RemoveAll`]: all holes are dropped.
/// - [`HoleFillMode::BelowThreshold`]: holes with unsigned area less than
///   `threshold_pixels as f64 * pixel_area` are dropped.
#[instrument(skip(geom))]
pub fn fill_holes(geom: MultiPolygon<f64>, mode: HoleFillMode) -> MultiPolygon<f64> {
    let mut total_holes: usize = 0;

    let polygons: Vec<Polygon<f64>> = geom
        .0
        .into_iter()
        .map(|poly| {
            let (exterior, interiors) = poly.into_inner();
            total_holes += interiors.len();

            let kept: Vec<LineString<f64>> = match mode {
                HoleFillMode::RemoveAll => vec![],
                HoleFillMode::BelowThreshold {
                    threshold_pixels,
                    pixel_area,
                } => {
                    let threshold_area = threshold_pixels as f64 * pixel_area;
                    interiors
                        .into_iter()
                        .filter(|ring| {
                            let area = Polygon::new(ring.clone(), vec![]).unsigned_area();
                            area >= threshold_area
                        })
                        .collect()
                }
            };

            Polygon::new(exterior, kept)
        })
        .collect();

    let kept_total: usize = polygons.iter().map(|p| p.interiors().len()).sum();
    let removed = total_holes - kept_total;

    debug!(removed = removed, total = total_holes, "filled holes");

    MultiPolygon::new(polygons)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unit square exterior ring.
    fn unit_square() -> LineString<f64> {
        LineString::from(vec![
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0),
        ])
    }

    /// Pixel area matching the original MERIT-Hydro 3 arc-sec pixel, used in tests
    /// to preserve the same numeric thresholds as the hydra-shed originals.
    const TEST_PIXEL_AREA: f64 = 0.000_000_694_444;

    /// A tiny hole: 0.001 × 0.001 = 0.000_001 sq-deg, well below threshold.
    ///
    /// threshold = 100 * 0.000_000_694_444 = 0.000_069_4444 sq-deg.
    /// small_hole area = 0.001 * 0.001 = 0.000_001 sq-deg — well below threshold.
    fn small_hole() -> LineString<f64> {
        LineString::from(vec![
            (0.4, 0.4),
            (0.401, 0.4),
            (0.401, 0.401),
            (0.4, 0.401),
            (0.4, 0.4),
        ])
    }

    /// A large hole: 0.8 × 0.8 = 0.64 sq-deg, well above threshold 0.000_069_4444.
    fn large_hole() -> LineString<f64> {
        LineString::from(vec![
            (0.1, 0.1),
            (0.9, 0.1),
            (0.9, 0.9),
            (0.1, 0.9),
            (0.1, 0.1),
        ])
    }

    #[test]
    fn no_holes_unchanged() {
        let poly = Polygon::new(unit_square(), vec![]);
        let mp = MultiPolygon::new(vec![poly]);
        let result = fill_holes(
            mp,
            HoleFillMode::BelowThreshold {
                threshold_pixels: DEFAULT_FILL_THRESHOLD_PX,
                pixel_area: TEST_PIXEL_AREA,
            },
        );
        assert_eq!(result.0.len(), 1);
        assert!(result.0[0].interiors().is_empty());
    }

    #[test]
    fn small_hole_removed() {
        let poly = Polygon::new(unit_square(), vec![small_hole()]);
        let mp = MultiPolygon::new(vec![poly]);
        let result = fill_holes(
            mp,
            HoleFillMode::BelowThreshold {
                threshold_pixels: DEFAULT_FILL_THRESHOLD_PX,
                pixel_area: TEST_PIXEL_AREA,
            },
        );
        assert_eq!(result.0.len(), 1);
        assert!(
            result.0[0].interiors().is_empty(),
            "small hole should have been removed"
        );
    }

    #[test]
    fn large_hole_kept() {
        let poly = Polygon::new(unit_square(), vec![large_hole()]);
        let mp = MultiPolygon::new(vec![poly]);
        let result = fill_holes(
            mp,
            HoleFillMode::BelowThreshold {
                threshold_pixels: DEFAULT_FILL_THRESHOLD_PX,
                pixel_area: TEST_PIXEL_AREA,
            },
        );
        assert_eq!(result.0.len(), 1);
        assert_eq!(
            result.0[0].interiors().len(),
            1,
            "large hole should have been preserved"
        );
    }

    #[test]
    fn threshold_zero_removes_all() {
        let poly = Polygon::new(unit_square(), vec![small_hole(), large_hole()]);
        let mp = MultiPolygon::new(vec![poly]);
        let result = fill_holes(mp, HoleFillMode::RemoveAll);
        assert_eq!(result.0.len(), 1);
        assert!(
            result.0[0].interiors().is_empty(),
            "RemoveAll must remove all holes"
        );
    }

    #[test]
    fn mixed_holes() {
        let poly = Polygon::new(unit_square(), vec![small_hole(), large_hole()]);
        let mp = MultiPolygon::new(vec![poly]);
        let result = fill_holes(
            mp,
            HoleFillMode::BelowThreshold {
                threshold_pixels: DEFAULT_FILL_THRESHOLD_PX,
                pixel_area: TEST_PIXEL_AREA,
            },
        );
        assert_eq!(result.0.len(), 1);
        assert_eq!(
            result.0[0].interiors().len(),
            1,
            "only the large hole should remain"
        );
    }

    #[test]
    fn multiple_polygons() {
        // Second polygon exterior: shifted 2 units right so it does not overlap.
        let exterior2 = LineString::from(vec![
            (2.0, 0.0),
            (3.0, 0.0),
            (3.0, 1.0),
            (2.0, 1.0),
            (2.0, 0.0),
        ]);
        // Small hole inside second polygon.
        let small_hole2 = LineString::from(vec![
            (2.4, 0.4),
            (2.401, 0.4),
            (2.401, 0.401),
            (2.4, 0.401),
            (2.4, 0.4),
        ]);
        // Large hole inside second polygon.
        let large_hole2 = LineString::from(vec![
            (2.1, 0.1),
            (2.9, 0.1),
            (2.9, 0.9),
            (2.1, 0.9),
            (2.1, 0.1),
        ]);

        let poly1 = Polygon::new(unit_square(), vec![small_hole(), large_hole()]);
        let poly2 = Polygon::new(exterior2, vec![small_hole2, large_hole2]);
        let mp = MultiPolygon::new(vec![poly1, poly2]);

        let result = fill_holes(
            mp,
            HoleFillMode::BelowThreshold {
                threshold_pixels: DEFAULT_FILL_THRESHOLD_PX,
                pixel_area: TEST_PIXEL_AREA,
            },
        );

        assert_eq!(result.0.len(), 2);
        assert_eq!(
            result.0[0].interiors().len(),
            1,
            "first polygon: only large hole should remain"
        );
        assert_eq!(
            result.0[1].interiors().len(),
            1,
            "second polygon: only large hole should remain"
        );
    }
}
