//! Extract the largest polygon by unsigned area from a multi-polygon.

use geo::{Area, MultiPolygon, Polygon};
use tracing::{debug, instrument};

/// Return the polygon with the greatest unsigned area, or `None` if empty.
///
/// Iterates over all polygons in `mp`, computes `unsigned_area()` for each,
/// and returns a clone of the one with the maximum area.
#[instrument(skip(mp))]
pub fn largest_polygon(mp: &MultiPolygon<f64>) -> Option<Polygon<f64>> {
    let winner = mp.0.iter().max_by(|a, b| {
        a.unsigned_area()
            .partial_cmp(&b.unsigned_area())
            .unwrap_or(std::cmp::Ordering::Equal)
    })?;

    let winning_area = winner.unsigned_area();
    debug!(area = winning_area, "selected largest polygon");

    Some(winner.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{LineString, Polygon};

    fn rect(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]),
            vec![],
        )
    }

    #[test]
    fn empty_multi_polygon_returns_none() {
        let mp: MultiPolygon<f64> = MultiPolygon::new(vec![]);
        assert!(largest_polygon(&mp).is_none());
    }

    #[test]
    fn single_polygon_returned() {
        let poly = rect(0.0, 0.0, 2.0, 3.0);
        let mp = MultiPolygon::new(vec![poly.clone()]);
        let result = largest_polygon(&mp).unwrap();
        assert_eq!(result, poly);
    }

    #[test]
    fn largest_of_three() {
        let small = rect(0.0, 0.0, 1.0, 1.0); // area = 1
        let medium = rect(0.0, 0.0, 2.0, 2.0); // area = 4
        let large = rect(0.0, 0.0, 3.0, 4.0); // area = 12
        let mp = MultiPolygon::new(vec![small, medium, large.clone()]);
        let result = largest_polygon(&mp).unwrap();
        assert_eq!(result, large);
    }

    #[test]
    fn identical_areas_returns_one() {
        let a = rect(0.0, 0.0, 2.0, 2.0); // area = 4
        let b = rect(5.0, 5.0, 7.0, 7.0); // area = 4
        let mp = MultiPolygon::new(vec![a, b]);
        // Either polygon is acceptable; just verify we get one back.
        assert!(largest_polygon(&mp).is_some());
    }
}
