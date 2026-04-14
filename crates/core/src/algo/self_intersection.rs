//! Pure-Rust self-intersection detection for multi-polygon geometry.

use geo::Line;
use geo::algorithm::sweep::Intersections;
use geo::{LineString, MultiPolygon};
use tracing::{debug, instrument};

/// Check whether any polygon in `mp` has a proper self-intersection.
///
/// Uses an O(n log n) Bentley-Ottmann sweep via [`Intersections`] to detect
/// ring crossings without invoking GDAL/GEOS. Returns `true` as soon as the
/// first proper intersection is found.
///
/// This is a fast guard to determine whether expensive geometry repair is needed.
#[instrument(skip(mp))]
pub fn has_self_intersections(mp: &MultiPolygon<f64>) -> bool {
    mp.0.iter().enumerate().any(|(poly_idx, polygon)| {
        if ring_has_self_intersection(polygon.exterior()) {
            debug!(poly_idx, ring = "exterior", "self-intersection detected");
            return true;
        }
        polygon
            .interiors()
            .iter()
            .enumerate()
            .any(|(hole_idx, hole)| {
                if ring_has_self_intersection(hole) {
                    debug!(
                        poly_idx,
                        hole_idx,
                        ring = "interior",
                        "self-intersection detected"
                    );
                    return true;
                }
                false
            })
    })
}

/// Check a single ring for proper self-intersections via Bentley-Ottmann sweep.
fn ring_has_self_intersection(ring: &LineString<f64>) -> bool {
    Intersections::<Line<f64>>::from_iter(ring.lines()).any(|(_, _, li)| li.is_proper())
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{LineString, MultiPolygon, Polygon};

    /// Build a simple polygon from coordinate tuples (no holes).
    fn poly(coords: &[(f64, f64)]) -> Polygon<f64> {
        Polygon::new(LineString::from(coords.to_vec()), vec![])
    }

    #[test]
    fn self_intersection_valid_square() {
        let square = poly(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
        let mp = MultiPolygon::new(vec![square]);
        assert!(!has_self_intersections(&mp));
    }

    #[test]
    fn self_intersection_true_bowtie() {
        // Figure-8 / bowtie: edges cross at (0.5, 0.5).
        let bowtie = poly(&[(0.0, 0.0), (1.0, 1.0), (0.0, 1.0), (1.0, 0.0), (0.0, 0.0)]);
        let mp = MultiPolygon::new(vec![bowtie]);
        assert!(has_self_intersections(&mp));
    }

    #[test]
    fn self_intersection_empty() {
        let mp: MultiPolygon<f64> = MultiPolygon::new(vec![]);
        assert!(!has_self_intersections(&mp));
    }

    #[test]
    fn self_intersection_valid_polygon_with_hole() {
        let exterior = LineString::from(vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let hole = LineString::from(vec![
            (2.0, 2.0),
            (8.0, 2.0),
            (8.0, 8.0),
            (2.0, 8.0),
            (2.0, 2.0),
        ]);
        let polygon = Polygon::new(exterior, vec![hole]);
        let mp = MultiPolygon::new(vec![polygon]);
        assert!(!has_self_intersections(&mp));
    }

    #[test]
    fn self_intersection_l_shape() {
        let l_shape = poly(&[
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 1.0),
            (1.0, 1.0),
            (1.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ]);
        let mp = MultiPolygon::new(vec![l_shape]);
        assert!(!has_self_intersections(&mp));
    }

    #[test]
    fn self_intersection_hole_self_intersects() {
        // Valid exterior, but the hole is a bowtie.
        let exterior = LineString::from(vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let bowtie_hole = LineString::from(vec![
            (2.0, 2.0),
            (8.0, 8.0),
            (2.0, 8.0),
            (8.0, 2.0),
            (2.0, 2.0),
        ]);
        let polygon = Polygon::new(exterior, vec![bowtie_hole]);
        let mp = MultiPolygon::new(vec![polygon]);
        assert!(has_self_intersections(&mp));
    }

    #[test]
    fn self_intersection_multi_one_bad() {
        let square = poly(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
        let bowtie = poly(&[(0.0, 0.0), (1.0, 1.0), (0.0, 1.0), (1.0, 0.0), (0.0, 0.0)]);
        let mp = MultiPolygon::new(vec![square, bowtie]);
        assert!(has_self_intersections(&mp));
    }
}
