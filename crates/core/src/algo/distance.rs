//! Geodesic distance calculation on the WGS84 ellipsoid.

use std::fmt;

use geo::{Distance, Geodesic};
use tracing::instrument;

use crate::algo::coord::GeoCoord;

/// Distance measured in metres on the WGS84 ellipsoid.
///
/// Wraps an `f64` value representing a distance. Does NOT derive `Eq`/`Ord`
/// because `f64` does not support them soundly.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct DistanceMetres(f64);

impl DistanceMetres {
    /// Create a new distance value.
    pub fn new(metres: f64) -> Self {
        Self(metres)
    }

    /// Return the raw `f64` value in metres.
    pub fn as_f64(self) -> f64 {
        self.0
    }

    /// Return the distance converted to kilometres.
    pub fn as_km(self) -> f64 {
        self.0 / 1000.0
    }
}

impl fmt::Display for DistanceMetres {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.1} m", self.0)
    }
}

/// Compute the geodesic distance between two geographic coordinates.
///
/// Uses Karney's geodesic algorithm via [`geo::Geodesic`] and the [`geo::Distance`]
/// trait for sub-millimetre accuracy on the WGS84 ellipsoid.
#[instrument]
pub fn geodesic_distance(a: GeoCoord, b: GeoCoord) -> DistanceMetres {
    let pa = geo::Point::new(a.lon, a.lat);
    let pb = geo::Point::new(b.lon, b.lat);
    let metres = Geodesic::distance(pa, pb);
    DistanceMetres::new(metres)
}

#[cfg(test)]
mod tests {
    use super::{DistanceMetres, geodesic_distance};
    use crate::algo::coord::GeoCoord;

    // Test 1: NYC to London great-circle distance ≈ 5,570 km, within 2%.
    #[test]
    fn nyc_to_london() {
        let nyc = GeoCoord::new(-74.006, 40.7128);
        let london = GeoCoord::new(-0.1278, 51.5074);
        let result = geodesic_distance(nyc, london);
        assert!(
            (result.as_km() - 5570.0).abs() / 5570.0 < 0.02,
            "expected ≈5570 km, got {:.1} km",
            result.as_km(),
        );
    }

    // Test 2: coincident points produce exactly 0.0 metres.
    #[test]
    fn coincident_points() {
        let p = GeoCoord::new(10.0, 50.0);
        let result = geodesic_distance(p, p);
        assert_eq!(result.as_f64(), 0.0);
    }

    // Test 3: 0.001° latitude shift at equator ≈ 111 m, within [100, 120) m.
    #[test]
    fn short_snap_distance() {
        let a = GeoCoord::new(0.0, 0.0);
        let b = GeoCoord::new(0.0, 0.001);
        let result = geodesic_distance(a, b);
        assert!(
            result.as_f64() >= 100.0 && result.as_f64() < 120.0,
            "expected 100–120 m, got {:.3} m",
            result.as_f64(),
        );
    }

    // Test 4: distance is symmetric — d(a, b) == d(b, a).
    #[test]
    fn symmetry() {
        let nyc = GeoCoord::new(-74.006, 40.7128);
        let london = GeoCoord::new(-0.1278, 51.5074);
        assert_eq!(
            geodesic_distance(nyc, london),
            geodesic_distance(london, nyc)
        );
    }

    // Test 5: antipodal points ≈ half Earth circumference ≈ 20,003 km, within 1%.
    #[test]
    fn antipodal_points() {
        let a = GeoCoord::new(0.0, 0.0);
        let b = GeoCoord::new(180.0, 0.0);
        let result = geodesic_distance(a, b);
        assert!(
            (result.as_km() - 20_003.0).abs() / 20_003.0 < 0.01,
            "expected ≈20003 km, got {:.1} km",
            result.as_km(),
        );
    }

    // Test 6: as_km converts metres to kilometres correctly.
    #[test]
    fn as_km_conversion() {
        let d = DistanceMetres::new(5000.0);
        assert_eq!(d.as_km(), 5.0);
    }

    // Test 7: Display formats to one decimal place followed by " m".
    #[test]
    fn display_formatting() {
        let d = DistanceMetres::new(1234.5);
        assert_eq!(format!("{d}"), "1234.5 m");
    }

    // Test 8: PartialOrd — smaller distance is less than larger distance.
    #[test]
    fn partial_ord() {
        assert!(DistanceMetres::new(100.0) < DistanceMetres::new(200.0));
    }

    // Test 9: PartialEq — equal values compare equal; differing values do not.
    #[test]
    fn equality() {
        assert_eq!(DistanceMetres::new(5.0), DistanceMetres::new(5.0));
        assert_ne!(DistanceMetres::new(5.0), DistanceMetres::new(6.0));
    }
}
