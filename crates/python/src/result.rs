//! Python-exposed [`DelineationResult`] wrapper.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use shed_core::algo::encode_wkb_multi_polygon;
use shed_core::{DelineationResult, RefinementOutcome};

use crate::geojson::result_to_geojson_feature;

/// Python-visible wrapper around [`DelineationResult`].
#[pyclass(name = "DelineationResult")]
pub struct PyDelineationResult {
    inner: DelineationResult,
}

impl PyDelineationResult {
    /// Wrap a [`DelineationResult`] from the engine.
    pub fn from_result(result: DelineationResult) -> Self {
        Self { inner: result }
    }
}

#[pymethods]
impl PyDelineationResult {
    /// Terminal atom ID that the outlet resolved to.
    #[getter]
    fn terminal_atom_id(&self) -> i64 {
        self.inner.terminal_atom_id().get()
    }

    /// Input outlet coordinate as `(lon, lat)`.
    #[getter]
    fn input_outlet(&self) -> (f64, f64) {
        let c = self.inner.input_outlet();
        (c.lon, c.lat)
    }

    /// Resolved outlet coordinate as `(lon, lat)`.
    #[getter]
    fn resolved_outlet(&self) -> (f64, f64) {
        let c = self.inner.resolved_outlet();
        (c.lon, c.lat)
    }

    /// Refined outlet coordinate as `(lon, lat)`, or `None` if refinement was
    /// not applied.
    #[getter]
    fn refined_outlet(&self) -> Option<(f64, f64)> {
        match self.inner.refinement() {
            RefinementOutcome::Applied { refined_outlet } => {
                Some((refined_outlet.lon, refined_outlet.lat))
            }
            _ => None,
        }
    }

    /// Debug string representation of the resolution method.
    #[getter]
    fn resolution_method(&self) -> String {
        format!("{:?}", self.inner.resolution_method())
    }

    /// All upstream atom IDs (including the terminal atom).
    #[getter]
    fn upstream_atom_ids(&self) -> Vec<i64> {
        self.inner.upstream_atom_ids().iter().map(|id| id.get()).collect()
    }

    /// Geodesic watershed area in km².
    #[getter]
    fn area_km2(&self) -> f64 {
        self.inner.area_km2().as_f64()
    }

    /// Watershed geometry encoded as OGC WKB bytes (little-endian, 2D).
    #[getter]
    fn geometry_wkb<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = encode_wkb_multi_polygon(self.inner.geometry())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Serialize the result as a GeoJSON Feature string.
    fn to_geojson(&self) -> PyResult<String> {
        result_to_geojson_feature(&self.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!(
            "DelineationResult(terminal_atom_id={}, area_km2={:.2}, upstream_count={})",
            self.inner.terminal_atom_id().get(),
            self.inner.area_km2().as_f64(),
            self.inner.upstream_atom_ids().len(),
        )
    }
}
