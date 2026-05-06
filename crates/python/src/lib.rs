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

use std::sync::OnceLock;

use log::{LevelFilter, Log, Metadata, Record};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use shed_core::telemetry::jsonl::{JsonlGuard, JsonlLayer};
use tracing_subscriber::prelude::*;

static JSONL_GUARD: OnceLock<JsonlGuard> = OnceLock::new();

struct PythonTracingLogger {
    python: pyo3_log::Logger,
    tracing: tracing_log::LogTracer,
}

impl PythonTracingLogger {
    fn install() -> Result<(), log::SetLoggerError> {
        let logger = Self {
            python: pyo3_log::Logger::default().filter(LevelFilter::Warn),
            tracing: tracing_log::LogTracer::new(),
        };
        log::set_boxed_logger(Box::new(logger))?;
        log::set_max_level(LevelFilter::Warn);
        Ok(())
    }
}

impl Log for PythonTracingLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.python.enabled(metadata) || self.tracing.enabled(metadata)
    }

    fn log(&self, record: &Record<'_>) {
        if self.python.enabled(record.metadata()) {
            self.python.log(record);
        }
        if self.tracing.enabled(record.metadata()) {
            self.tracing.log(record);
        }
    }

    fn flush(&self) {
        self.python.flush();
        self.tracing.flush();
    }
}

/// Map a Python log level string or common alias to `log::LevelFilter`.
///
/// Accepts `"trace"`, `"debug"`, `"info"`, `"warn"`/`"warning"`, and
/// `"error"`/`"critical"` (case-insensitive).
fn parse_level_filter(level: &str) -> PyResult<log::LevelFilter> {
    match level.to_ascii_lowercase().as_str() {
        "trace" => Ok(log::LevelFilter::Trace),
        "debug" => Ok(log::LevelFilter::Debug),
        "info" => Ok(log::LevelFilter::Info),
        "warn" | "warning" => Ok(log::LevelFilter::Warn),
        "error" | "critical" => Ok(log::LevelFilter::Error),
        other => Err(PyValueError::new_err(format!(
            "unknown log level {other:?}; valid values are: trace, debug, info, warn, warning, error, critical"
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
/// Valid levels (case-insensitive): `"trace"`, `"debug"`, `"info"`,
/// `"warn"`/`"warning"`, `"error"`/`"critical"`.
#[pyfunction]
fn _set_log_level(level: &str) -> PyResult<()> {
    let filter = parse_level_filter(level)?;
    log::set_max_level(filter);
    Ok(())
}

fn install_tracing() {
    let Ok(Some((layer, guard))) = JsonlLayer::from_env() else {
        return;
    };
    let subscriber = tracing_subscriber::registry().with(layer);
    if tracing::subscriber::set_global_default(subscriber).is_ok() {
        let _ = JSONL_GUARD.set(guard);
    }
}

#[pyfunction]
fn _install_bench_trace() {
    install_tracing();
}

#[pymodule]
fn _pyshed(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Route Rust `tracing`/`log` events to Python's `logging` module.
    // `let _ =` ensures a duplicate install on re-import does not panic
    // (`log::set_boxed_logger` only succeeds once per process).
    let _ = PythonTracingLogger::install();
    install_tracing();
    // Keep import quiet by default. The public Python wrapper can raise this
    // dynamic max-level on demand through `_set_log_level`.
    log::set_max_level(log::LevelFilter::Warn);

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
    m.add_function(wrap_pyfunction!(_install_bench_trace, m)?)?;
    m.add_function(wrap_pyfunction!(_set_log_level, m)?)?;
    Ok(())
}
