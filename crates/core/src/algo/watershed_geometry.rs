//! Typestate pipeline for watershed geometry post-processing.

use std::marker::PhantomData;

use geo::algorithm::winding_order::Winding;
use geo::{MultiPolygon, Polygon};

use crate::algo::area::AreaKm2;
use crate::algo::clean_epsilon::CleanEpsilon;
use crate::algo::clean_topology::clean_topology;
use crate::algo::hole_fill::{HoleFillMode, fill_holes};
use crate::algo::largest_polygon::largest_polygon;
use crate::algo::traits::{GeometryRepair, GeometryRepairError};
use crate::algo::watershed_area::{WatershedAreaError, geodesic_area_multi};

/// State marker: geometry has been dissolved but not cleaned.
#[derive(Debug)]
pub struct Dissolved;

/// State marker: topology has been cleaned but holes have not been filled.
#[derive(Debug)]
pub struct TopologyCleaned;

/// State marker: holes have been filled; geometry is ready for extraction.
#[derive(Debug)]
pub struct HolesFilled;

/// A watershed geometry that progresses through post-processing states.
///
/// The typestate parameter `State` enforces the correct processing order:
/// 1. [`Dissolved`] → [`TopologyCleaned`] via [`clean_topology`](WatershedGeometry::clean_topology)
///    or [`repair_topology`](WatershedGeometry::repair_topology)
/// 2. [`TopologyCleaned`] → [`HolesFilled`] via [`fill_holes`](WatershedGeometry::fill_holes)
/// 3. [`HolesFilled`] → extract the final polygon via [`largest_polygon`](WatershedGeometry::largest_polygon)
///    or [`into_inner`](WatershedGeometry::into_inner)
#[derive(Debug)]
pub struct WatershedGeometry<State> {
    inner: MultiPolygon<f64>,
    _state: PhantomData<State>,
}

impl WatershedGeometry<Dissolved> {
    /// Create a new pipeline from a dissolved multi-polygon.
    pub fn from_dissolved(mp: MultiPolygon<f64>) -> Self {
        Self {
            inner: mp,
            _state: PhantomData,
        }
    }

    /// Clean topology using pure-Rust buffer-unbuffer, transitioning to [`TopologyCleaned`].
    pub fn clean_topology(self, epsilon: CleanEpsilon) -> WatershedGeometry<TopologyCleaned> {
        let cleaned = clean_topology(self.inner, epsilon);
        WatershedGeometry {
            inner: cleaned,
            _state: PhantomData,
        }
    }

    /// Repair topology using an external geometry repair implementation (e.g., GDAL),
    /// transitioning to [`TopologyCleaned`].
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`GeometryRepairError::BackendError`] | Backend geometry operation fails |
    /// | [`GeometryRepairError::UnexpectedGeometryType`] | Backend returns a non-polygon type |
    /// | [`GeometryRepairError::StillInvalid`] | Geometry remains invalid after repair |
    pub fn repair_topology(
        self,
        repairer: &dyn GeometryRepair,
        epsilon: CleanEpsilon,
    ) -> Result<WatershedGeometry<TopologyCleaned>, GeometryRepairError> {
        let repaired = repairer.repair(self.inner, epsilon)?;
        Ok(WatershedGeometry {
            inner: repaired,
            _state: PhantomData,
        })
    }

    /// Skip cleaning — geometry is already valid.
    pub fn with_cleaned_topology(self) -> WatershedGeometry<TopologyCleaned> {
        WatershedGeometry {
            inner: self.inner,
            _state: PhantomData,
        }
    }

    /// Access the inner geometry.
    pub fn into_inner(self) -> MultiPolygon<f64> {
        self.inner
    }
}

impl WatershedGeometry<TopologyCleaned> {
    /// Fill interior holes according to `mode`, transitioning to [`HolesFilled`].
    pub fn fill_holes(self, mode: HoleFillMode) -> WatershedGeometry<HolesFilled> {
        let filled = fill_holes(self.inner, mode);
        WatershedGeometry {
            inner: filled,
            _state: PhantomData,
        }
    }

    /// Access the inner geometry.
    pub fn into_inner(self) -> MultiPolygon<f64> {
        self.inner
    }
}

impl WatershedGeometry<HolesFilled> {
    /// Select the polygon with the largest area, normalized to CCW exterior winding.
    pub fn largest_polygon(&self) -> Option<Polygon<f64>> {
        largest_polygon(&self.inner).map(normalize_polygon_winding)
    }

    /// Compute geodesic area of the multi-polygon on the WGS84 ellipsoid.
    ///
    /// Normalizes polygon winding to CCW exteriors / CW holes before computing,
    /// because upstream geometry operations (dissolve, clean_topology) may produce
    /// CW exteriors, which causes Karney's algorithm to compute Earth's complement.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`WatershedAreaError::EmptyGeometry`] | Multi-polygon contains no polygons |
    /// | [`WatershedAreaError::NonFiniteArea`] | Geodesic area computation yields non-finite value |
    pub fn geodesic_area(&self) -> Result<AreaKm2, WatershedAreaError> {
        let normalized = normalize_winding(&self.inner);
        geodesic_area_multi(&normalized)
    }

    /// Access the inner geometry.
    pub fn into_inner(self) -> MultiPolygon<f64> {
        self.inner
    }
}

/// Normalize a polygon to CCW exterior / CW holes.
fn normalize_polygon_winding(poly: Polygon<f64>) -> Polygon<f64> {
    let (mut exterior, interiors) = poly.into_inner();
    exterior.make_ccw_winding();
    let holes: Vec<_> = interiors
        .into_iter()
        .map(|mut h| {
            h.make_cw_winding();
            h
        })
        .collect();
    Polygon::new(exterior, holes)
}

/// Normalize all polygons in a multi-polygon to CCW exterior / CW holes.
fn normalize_winding(mp: &MultiPolygon<f64>) -> MultiPolygon<f64> {
    MultiPolygon::new(
        mp.0.iter()
            .cloned()
            .map(normalize_polygon_winding)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::clean_epsilon::DEFAULT_CLEANING_EPSILON;
    use crate::algo::hole_fill::HoleFillMode;
    use crate::algo::traits::GeometryRepairError;
    use geo::{Area, LineString, MultiPolygon, Polygon};

    fn unit_square() -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            vec![],
        )
    }

    #[test]
    fn full_pipeline() {
        let mp = MultiPolygon::new(vec![unit_square()]);
        let result = WatershedGeometry::from_dissolved(mp)
            .clean_topology(DEFAULT_CLEANING_EPSILON)
            .fill_holes(HoleFillMode::RemoveAll)
            .largest_polygon();
        assert!(result.is_some());
        let poly = result.unwrap();
        let area = poly.unsigned_area();
        // Area should be approximately 1.0 after buffer-unbuffer with tiny epsilon
        assert!((area - 1.0).abs() < 0.05, "area was {area}, expected ~1.0");
    }

    #[test]
    fn into_inner_returns_multi_polygon() {
        let mp = MultiPolygon::new(vec![unit_square()]);
        let result = WatershedGeometry::from_dissolved(mp)
            .clean_topology(DEFAULT_CLEANING_EPSILON)
            .fill_holes(HoleFillMode::RemoveAll)
            .into_inner();
        assert!(!result.0.is_empty());
    }

    #[test]
    fn empty_geometry() {
        let mp = MultiPolygon::new(vec![]);
        let result = WatershedGeometry::from_dissolved(mp)
            .clean_topology(DEFAULT_CLEANING_EPSILON)
            .fill_holes(HoleFillMode::RemoveAll)
            .largest_polygon();
        assert!(result.is_none());
    }

    #[test]
    fn pipeline_preserves_area() {
        let poly = unit_square();
        let original_area = poly.unsigned_area();
        let mp = MultiPolygon::new(vec![poly]);
        let result = WatershedGeometry::from_dissolved(mp)
            .clean_topology(DEFAULT_CLEANING_EPSILON)
            .fill_holes(HoleFillMode::BelowThreshold {
                threshold_pixels: 100,
                pixel_area_deg2: 0.000_000_694_444,
            })
            .largest_polygon()
            .unwrap();
        let ratio = (result.unsigned_area() - original_area).abs() / original_area;
        assert!(
            ratio < 0.01,
            "area changed by {:.2}%, expected < 1%",
            ratio * 100.0
        );
    }

    #[test]
    fn with_cleaned_topology_skips_cleaning() {
        let mp = MultiPolygon::new(vec![unit_square()]);
        // with_cleaned_topology should produce a valid TopologyCleaned without running buffer-unbuffer
        let result = WatershedGeometry::from_dissolved(mp)
            .with_cleaned_topology()
            .fill_holes(HoleFillMode::RemoveAll)
            .largest_polygon();
        assert!(result.is_some());
    }

    #[test]
    fn dissolved_into_inner() {
        let mp = MultiPolygon::new(vec![unit_square()]);
        let inner = WatershedGeometry::from_dissolved(mp.clone()).into_inner();
        assert_eq!(inner.0.len(), mp.0.len());
    }

    #[test]
    fn topology_cleaned_into_inner() {
        let mp = MultiPolygon::new(vec![unit_square()]);
        let inner = WatershedGeometry::from_dissolved(mp)
            .clean_topology(DEFAULT_CLEANING_EPSILON)
            .into_inner();
        assert!(!inner.0.is_empty());
    }

    // ── Mock GeometryRepair for repair_topology tests ─────────────────────────

    /// A passthrough implementation that returns geometry unchanged.
    struct PassthroughRepair;

    impl GeometryRepair for PassthroughRepair {
        fn repair(
            &self,
            geometry: MultiPolygon<f64>,
            _epsilon: CleanEpsilon,
        ) -> Result<MultiPolygon<f64>, GeometryRepairError> {
            Ok(geometry)
        }
    }

    /// An implementation that always fails.
    struct FailingRepair;

    impl GeometryRepair for FailingRepair {
        fn repair(
            &self,
            _geometry: MultiPolygon<f64>,
            _epsilon: CleanEpsilon,
        ) -> Result<MultiPolygon<f64>, GeometryRepairError> {
            Err(GeometryRepairError::StillInvalid)
        }
    }

    #[test]
    fn repair_topology_passthrough() {
        let mp = MultiPolygon::new(vec![unit_square()]);
        let repairer = PassthroughRepair;
        let result = WatershedGeometry::from_dissolved(mp)
            .repair_topology(&repairer, DEFAULT_CLEANING_EPSILON)
            .unwrap()
            .fill_holes(HoleFillMode::RemoveAll)
            .largest_polygon();
        assert!(result.is_some());
    }

    #[test]
    fn repair_topology_propagates_error() {
        let mp = MultiPolygon::new(vec![unit_square()]);
        let repairer = FailingRepair;
        let result = WatershedGeometry::from_dissolved(mp)
            .repair_topology(&repairer, DEFAULT_CLEANING_EPSILON);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), GeometryRepairError::StillInvalid));
    }
}
