//! Geodesic watershed area calculation using Karney's algorithm.

use geo::{GeodesicArea, MultiPolygon, Polygon};
use tracing::{debug, info, instrument};

use crate::algo::area::AreaKm2;

/// Conversion factor from square metres to square kilometres.
const M2_TO_KM2: f64 = 1e-6;

/// Errors from geodesic area computation.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WatershedAreaError {
    /// Returned when the input geometry contains no polygons.
    #[error("cannot compute area of empty geometry")]
    EmptyGeometry,

    /// Returned when the geodesic area computation yields a non-finite value.
    #[error("geodesic area returned non-finite value: {raw_m2} m²")]
    NonFiniteArea {
        /// The raw area in square metres that was non-finite.
        raw_m2: f64,
    },
}

/// Compute the geodesic area of a polygon on the WGS84 ellipsoid.
///
/// Uses [`geo::GeodesicArea::geodesic_area_unsigned`] (Karney 2013) for
/// sub-metre accuracy without projection distortion.
///
/// # Errors
///
/// | Condition | Error |
/// |-----------|-------|
/// | Result is non-finite | [`WatershedAreaError::NonFiniteArea`] |
#[instrument(skip(polygon))]
pub fn geodesic_area(polygon: &Polygon<f64>) -> Result<AreaKm2, WatershedAreaError> {
    let area_m2 = polygon.geodesic_area_unsigned();

    if !area_m2.is_finite() {
        return Err(WatershedAreaError::NonFiniteArea { raw_m2: area_m2 });
    }

    let area_km2 = area_m2 * M2_TO_KM2;
    debug!(area_km2, "geodesic polygon area computed");
    Ok(AreaKm2::new(area_km2))
}

/// Compute the geodesic area of a multi-polygon on the WGS84 ellipsoid.
///
/// Sums the unsigned area of each constituent polygon using
/// [`geo::GeodesicArea::geodesic_area_unsigned`].
///
/// # Errors
///
/// | Condition | Error |
/// |-----------|-------|
/// | Multi-polygon has no polygons | [`WatershedAreaError::EmptyGeometry`] |
/// | Result is non-finite | [`WatershedAreaError::NonFiniteArea`] |
#[instrument(skip(multi_polygon))]
pub fn geodesic_area_multi(
    multi_polygon: &MultiPolygon<f64>,
) -> Result<AreaKm2, WatershedAreaError> {
    if multi_polygon.0.is_empty() {
        return Err(WatershedAreaError::EmptyGeometry);
    }

    let area_m2 = multi_polygon.geodesic_area_unsigned();

    if !area_m2.is_finite() {
        return Err(WatershedAreaError::NonFiniteArea { raw_m2: area_m2 });
    }

    let area_km2 = area_m2 * M2_TO_KM2;
    info!(
        area_km2,
        polygon_count = multi_polygon.0.len(),
        "geodesic multi-polygon area computed"
    );
    Ok(AreaKm2::new(area_km2))
}

#[cfg(test)]
mod tests {
    use geo::{LineString, MultiPolygon, Polygon};

    use super::{WatershedAreaError, geodesic_area, geodesic_area_multi};
    use crate::algo::area::AreaKm2;

    /// Build a geographic rectangle polygon from southwest to northeast corners.
    fn geo_rect(west: f64, south: f64, east: f64, north: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (west, south),
                (east, south),
                (east, north),
                (west, north),
                (west, south),
            ]),
            vec![],
        )
    }

    #[test]
    fn one_degree_square_at_equator() {
        let poly = geo_rect(0.0, 0.0, 1.0, 1.0);
        let result = geodesic_area(&poly).unwrap().as_f64();
        let expected = 12_309.0_f64;
        let rel_err = (result - expected).abs() / expected;
        assert!(
            rel_err < 0.01,
            "area {result:.1} km² deviates {:.2}% from expected {expected} km²",
            rel_err * 100.0
        );
    }

    #[test]
    fn one_degree_square_at_60n() {
        let poly = geo_rect(0.0, 60.0, 1.0, 61.0);
        let result = geodesic_area(&poly).unwrap().as_f64();
        let expected = 6_154.0_f64;
        let rel_err = (result - expected).abs() / expected;
        assert!(
            rel_err < 0.02,
            "area {result:.1} km² deviates {:.2}% from expected {expected} km²",
            rel_err * 100.0
        );
    }

    #[test]
    fn small_watershed_polygon() {
        let poly = geo_rect(10.0, 45.0, 10.01, 45.01);
        let result = geodesic_area(&poly).unwrap().as_f64();
        assert!(
            result > 0.5 && result < 1.5,
            "area {result:.4} km² not in expected range (0.5, 1.5) km²"
        );
    }

    #[test]
    fn polygon_with_hole() {
        let outer = LineString::from(vec![
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0),
        ]);
        let hole = LineString::from(vec![
            (0.25, 0.25),
            (0.75, 0.25),
            (0.75, 0.75),
            (0.25, 0.75),
            (0.25, 0.25),
        ]);
        let poly = Polygon::new(outer, vec![hole]);
        let full_square_area = geodesic_area(&geo_rect(0.0, 0.0, 1.0, 1.0))
            .unwrap()
            .as_f64();
        let holed_area = geodesic_area(&poly).unwrap().as_f64();
        assert!(
            holed_area < full_square_area,
            "holed polygon area {holed_area:.1} km² should be less than full square {full_square_area:.1} km²"
        );
    }

    #[test]
    fn empty_multi_polygon_error() {
        let result = geodesic_area_multi(&MultiPolygon::new(vec![]));
        assert_eq!(result, Err(WatershedAreaError::EmptyGeometry));
    }

    #[test]
    fn multi_polygon_sums_areas() {
        let rect_a = geo_rect(0.0, 0.0, 1.0, 1.0);
        let rect_b = geo_rect(10.0, 0.0, 11.0, 1.0);
        let single_area = geodesic_area(&geo_rect(0.0, 0.0, 1.0, 1.0))
            .unwrap()
            .as_f64();
        let multi = MultiPolygon::new(vec![rect_a, rect_b]);
        let total_area = geodesic_area_multi(&multi).unwrap().as_f64();
        let expected = 2.0 * single_area;
        let rel_err = (total_area - expected).abs() / expected;
        assert!(
            rel_err < 0.01,
            "multi-polygon area {total_area:.1} km² deviates {:.2}% from expected {expected:.1} km²",
            rel_err * 100.0
        );
    }

    #[test]
    fn returns_area_km2_newtype() {
        let poly = geo_rect(0.0, 0.0, 1.0, 1.0);
        let area: AreaKm2 = geodesic_area(&poly).unwrap();
        assert!(area.as_f64() > 0.0, "area should be positive");
    }

    #[test]
    fn non_finite_check_valid_polygon_passes() {
        // Positive test: a well-formed polygon must NOT produce NonFiniteArea.
        let poly = geo_rect(5.0, 5.0, 6.0, 6.0);
        let result = geodesic_area(&poly);
        assert!(
            result.is_ok(),
            "valid polygon should not produce NonFiniteArea, got {result:?}"
        );
        assert!(result.unwrap().as_f64().is_finite());
    }
}
