//! Python exception types for pyshed.

use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;

// The first argument to `create_exception!` sets the `__module__` attribute
// that appears in Python tracebacks. We use `pyshed` (not `_pyshed`) so that
// users see `pyshed.DatasetError` rather than `pyshed._pyshed.DatasetError`.
// The exception types are registered in the `_pyshed` compiled extension and
// re-exported by `pyshed/__init__.py`, but their `__module__` stays `pyshed`.
create_exception!(pyshed, ShedError, PyException);
create_exception!(pyshed, DatasetError, ShedError);
create_exception!(pyshed, ResolutionError, ShedError);
create_exception!(pyshed, PyAssemblyError, ShedError);

/// Map a dataset/session error to a Python [`DatasetError`].
pub fn dataset_err(e: impl std::fmt::Display) -> PyErr {
    DatasetError::new_err(e.to_string())
}

/// Map a [`shed_core::EngineError`] to the most specific Python exception.
pub fn engine_err_to_py(e: shed_core::EngineError) -> PyErr {
    use shed_core::EngineError;
    match e {
        EngineError::Resolution { .. } => ResolutionError::new_err(e.to_string()),
        EngineError::Traversal { .. } => ShedError::new_err(e.to_string()),
        EngineError::TerminalCatchmentFetch { .. } => DatasetError::new_err(e.to_string()),
        EngineError::TerminalCatchmentDecode { .. } => DatasetError::new_err(e.to_string()),
        EngineError::Refinement { .. } => ShedError::new_err(e.to_string()),
        EngineError::Assembly { .. } => PyAssemblyError::new_err(e.to_string()),
    }
}
