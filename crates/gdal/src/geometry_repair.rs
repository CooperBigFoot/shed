//! GDAL/GEOS-backed geometry validation and repair.
//!
//! Applies a buffer/unbuffer round-trip via GEOS (through GDAL) to resolve
//! ring self-intersections. Falls back to `OGR_G_MakeValid` when the
//! buffer round-trip leaves the geometry invalid.

use geo::MultiPolygon;
use tracing::{debug, instrument, warn};

use shed_core::algo::clean_epsilon::CleanEpsilon;
use shed_core::algo::traits::{GeometryRepair, GeometryRepairError};

use crate::convert::{gdal_to_multi_polygon, multi_polygon_to_gdal};
use crate::error::GdalRepairError;

/// GDAL/GEOS-backed implementation of [`GeometryRepair`].
#[derive(Debug, Clone)]
pub struct GdalGeometryRepair;

impl GdalGeometryRepair {
    /// Create a new `GdalGeometryRepair`.
    pub fn new() -> Self {
        Self
    }
}

impl Default for GdalGeometryRepair {
    fn default() -> Self {
        Self::new()
    }
}

impl GeometryRepair for GdalGeometryRepair {
    #[instrument(skip(self, geometry), fields(polygon_count = geometry.0.len()))]
    fn repair(
        &self,
        geometry: MultiPolygon<f64>,
        epsilon: CleanEpsilon,
    ) -> Result<MultiPolygon<f64>, GeometryRepairError> {
        repair_impl(geometry, epsilon).map_err(|e| match e {
            GdalRepairError::Gdal { reason } => GeometryRepairError::BackendError { reason },
            GdalRepairError::UnexpectedGeometryType { geometry_type } => {
                GeometryRepairError::UnexpectedGeometryType { geometry_type }
            }
            GdalRepairError::StillInvalid => GeometryRepairError::StillInvalid,
        })
    }
}

/// Core repair logic, operating with `GdalRepairError` internally.
fn repair_impl(
    mp: MultiPolygon<f64>,
    epsilon: CleanEpsilon,
) -> Result<MultiPolygon<f64>, GdalRepairError> {
    if mp.0.is_empty() {
        return Ok(mp);
    }

    debug!(
        polygon_count = mp.0.len(),
        epsilon = epsilon.as_f64(),
        "starting GDAL geometry repair"
    );

    let gdal_geom = multi_polygon_to_gdal(&mp)?;

    let eps = epsilon.as_f64();
    let buffered_out = gdal_geom.buffer(eps, 8)?;
    let buffered_back = buffered_out.buffer(-eps, 8)?;

    debug!("buffer round-trip complete, checking validity");

    let repaired = if buffered_back.is_valid() {
        buffered_back
    } else {
        warn!("geometry still invalid after buffer round-trip, calling make_valid");
        let made_valid = buffered_back.make_valid(&gdal::cpl::CslStringList::new())?;
        debug!(
            geometry_type = %made_valid.geometry_name(),
            "make_valid returned geometry"
        );
        if !made_valid.is_valid() {
            return Err(GdalRepairError::StillInvalid);
        }
        made_valid
    };

    gdal_to_multi_polygon(&repaired)
}
