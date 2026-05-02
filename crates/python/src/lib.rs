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
pub(crate) mod kwargs;
mod result;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Map a log level string to `log::LevelFilter`.
///
/// Accepts `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"` (case-insensitive).
fn parse_level_filter(level: &str) -> PyResult<log::LevelFilter> {
    match level.to_ascii_lowercase().as_str() {
        "trace" => Ok(log::LevelFilter::Trace),
        "debug" => Ok(log::LevelFilter::Debug),
        "info" => Ok(log::LevelFilter::Info),
        "warn" => Ok(log::LevelFilter::Warn),
        "error" => Ok(log::LevelFilter::Error),
        other => Err(PyValueError::new_err(format!(
            "unknown log level {other:?}; valid values are: trace, debug, info, warn, error"
        ))),
    }
}

/// Update the Rust-side global log level forwarded to Python's `logging` module.
///
/// `log::set_boxed_logger` can only be called once per process; the bridge is
/// installed in the module-init path below. After that, only the dynamic
/// `log::set_max_level` filter can change. This function adjusts that filter.
///
/// Internal hook — the public `pyshed.set_log_level` wrapper in `__init__.py`
/// calls into this and additionally configures the Python `logging` hierarchy
/// so records are actually emitted.
///
/// Valid levels (case-insensitive): `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`.
#[pyfunction]
fn _set_log_level(level: &str) -> PyResult<()> {
    let filter = parse_level_filter(level)?;
    log::set_max_level(filter);
    Ok(())
}

#[pymodule]
fn _pyshed(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Route Rust `tracing`/`log` events to Python's `logging` module.
    // `let _ =` ensures a duplicate install on re-import does not panic
    // (`log::set_boxed_logger` only succeeds once per process).
    let _ = pyo3_log::Logger::default()
        .filter(log::LevelFilter::Trace)
        .install();
    // Be explicit about the dynamic max-level — the bridge respects this and
    // future readers shouldn't have to grep pyo3-log internals to know we
    // start at Trace.
    log::set_max_level(log::LevelFilter::Trace);

    m.add_class::<engine::PyEngine>()?;
    m.add_class::<result::PyDelineationResult>()?;
    m.add_class::<result::PyAreaOnlyResult>()?;
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
    m.add_function(wrap_pyfunction!(_set_log_level, m)?)?;
    Ok(())
}
