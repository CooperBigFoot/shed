//! PyO3 bindings for the shed watershed delineation engine.
//!
//! Exposes [`Engine`] and [`DelineationResult`] to Python, plus a hierarchy
//! of typed exceptions rooted at `ShedError`.
//!
//! The compiled extension module is named `_pyshed` (note the leading
//! underscore). The public `pyshed` package re-exports everything from
//! `pyshed/__init__.py` and handles runtime GDAL/PROJ data injection.

mod config;
mod data_paths;
mod engine;
mod error;
mod geojson;
mod result;

use pyo3::prelude::*;

#[pymodule]
fn _pyshed(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<engine::PyEngine>()?;
    m.add_class::<result::PyDelineationResult>()?;
    m.add("ShedError", m.py().get_type::<error::ShedError>())?;
    m.add("DatasetError", m.py().get_type::<error::DatasetError>())?;
    m.add(
        "ResolutionError",
        m.py().get_type::<error::ResolutionError>(),
    )?;
    m.add("AssemblyError", m.py().get_type::<error::PyAssemblyError>())?;
    m.add_function(wrap_pyfunction!(data_paths::_set_gdal_data, m)?)?;
    m.add_function(wrap_pyfunction!(data_paths::_set_proj_data, m)?)?;
    m.add_function(wrap_pyfunction!(data_paths::_self_test_proj, m)?)?;
    Ok(())
}
