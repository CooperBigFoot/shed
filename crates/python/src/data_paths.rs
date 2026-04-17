//! Runtime data-path injection for bundled GDAL/PROJ data.
//!
//! Wheel builds bundle GDAL data files under `pyshed/_data/gdal/` and PROJ
//! data (including `proj.db`) under `pyshed/_data/proj/`. The Python
//! `__init__.py` detects those directories at import time and calls these
//! functions to inform the GDAL/PROJ runtime of the correct search paths,
//! overriding any compiled-in or environment-variable defaults.
//!
//! On source/editable installs the `_data/` directory is absent; `__init__.py`
//! silently skips injection and relies on a system-installed GDAL/PROJ.

use std::ffi::{CString, c_char};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Inject the bundled GDAL data directory into the GDAL runtime.
///
/// Calls `CPLSetConfigOption("GDAL_DATA", path)`. Called by `pyshed/__init__.py`
/// when a bundled `gdal/gdalvrt.xsd` sentinel is found.
#[pyfunction]
pub(crate) fn _set_gdal_data(path: &str) -> PyResult<()> {
    tracing::debug!(gdal_data = %path, "injecting GDAL_DATA");
    gdal::config::set_config_option("GDAL_DATA", path)
        .map_err(|e| PyRuntimeError::new_err(format!("set GDAL_DATA: {e}")))?;
    Ok(())
}

/// Inject the bundled PROJ data directory into the PROJ runtime.
///
/// Calls `OSRSetPROJSearchPaths` with a NULL-terminated pointer array. Called
/// by `pyshed/__init__.py` when a bundled `proj/proj.db` sentinel is found, or
/// when the `PROJ_DATA`/`PROJ_LIB` environment variable is set.
#[pyfunction]
pub(crate) fn _set_proj_data(path: &str) -> PyResult<()> {
    tracing::debug!(proj_data = %path, "injecting PROJ search path");
    let c_path =
        CString::new(path).map_err(|e| PyRuntimeError::new_err(format!("NUL in path: {e}")))?;
    // SAFETY: `ptrs` is a NULL-terminated array of `*const c_char`. `c_path`
    // is kept alive until the end of this function, outliving the FFI call.
    let ptrs: [*const c_char; 2] = [c_path.as_ptr(), std::ptr::null()];
    unsafe { gdal_sys::OSRSetPROJSearchPaths(ptrs.as_ptr()) };
    Ok(())
}

/// Verify that PROJ can resolve its data and perform a coordinate transform.
///
/// Creates an EPSG:4326 → EPSG:3857 transform and projects the origin (0°lon,
/// 0°lat), which must map to a finite Web Mercator coordinate pair. A failure
/// here means `proj.db` is missing or unreachable.
///
/// Used by Phase 3's `CIBW_TEST_COMMAND` to prove PROJ data is accessible
/// inside the bundled wheel before it is published.
#[pyfunction]
pub(crate) fn _self_test_proj() -> PyResult<()> {
    use gdal::spatial_ref::{AxisMappingStrategy, CoordTransform, SpatialRef};

    tracing::debug!("running PROJ self-test EPSG:4326 -> EPSG:3857");

    let mut wgs84 = SpatialRef::from_epsg(4326)
        .map_err(|e| PyRuntimeError::new_err(format!("load EPSG:4326: {e}")))?;
    let mut webmerc = SpatialRef::from_epsg(3857)
        .map_err(|e| PyRuntimeError::new_err(format!("load EPSG:3857: {e}")))?;

    // Force (lon, lat) axis order so xs[0] = longitude and ys[0] = latitude,
    // regardless of how GDAL/PROJ ≥ 6 interprets the authority-compliant order.
    wgs84.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
    webmerc.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);

    let xform = CoordTransform::new(&wgs84, &webmerc)
        .map_err(|e| PyRuntimeError::new_err(format!("build transform: {e}")))?;

    // Origin in EPSG:4326 (lon=0, lat=0) maps to origin (0, 0) in EPSG:3857.
    // Any finite pair is acceptable; non-finite output means proj.db is unreachable.
    let mut xs = [0.0_f64];
    let mut ys = [0.0_f64];
    let mut zs = [0.0_f64];
    xform
        .transform_coords(&mut xs, &mut ys, &mut zs)
        .map_err(|e| PyRuntimeError::new_err(format!("transform (0,0): {e}")))?;

    if !xs[0].is_finite() || !ys[0].is_finite() {
        return Err(PyRuntimeError::new_err(
            "PROJ transform produced non-finite result",
        ));
    }

    Ok(())
}
