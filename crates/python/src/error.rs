//! Python exception types for pyshed.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::PyErr;

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
