//! PyO3 bindings for the shed watershed delineation engine.
//!
//! Exposes [`Engine`] and [`DelineationResult`] to Python, plus a hierarchy
//! of typed exceptions rooted at `ShedError`.

mod config;
mod engine;
mod error;
mod geojson;
mod result;

use pyo3::prelude::*;

#[pymodule]
fn pyshed(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<engine::PyEngine>()?;
    m.add_class::<result::PyDelineationResult>()?;
    m.add("ShedError", m.py().get_type::<error::ShedError>())?;
    m.add("DatasetError", m.py().get_type::<error::DatasetError>())?;
    m.add(
        "ResolutionError",
        m.py().get_type::<error::ResolutionError>(),
    )?;
    m.add("AssemblyError", m.py().get_type::<error::PyAssemblyError>())?;
    Ok(())
}
