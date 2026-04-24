//! Python-exposed [`DelineationResult`] wrapper.

use std::sync::OnceLock;

use geo::BoundingRect;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use shed_core::algo::encode_wkb_multi_polygon;
use shed_core::engine::DelineationAreaOnlyResult;
use shed_core::{DelineationResult, RefinementOutcome};

use crate::geojson::result_to_geojson_feature;

/// Python-visible wrapper around [`DelineationResult`].
#[pyclass(name = "DelineationResult")]
pub struct PyDelineationResult {
    inner: DelineationResult,
    geometry_wkb: OnceLock<Vec<u8>>,
}

impl PyDelineationResult {
    /// Wrap a [`DelineationResult`] from the engine.
    pub fn from_result(result: DelineationResult) -> Self {
        Self {
            inner: result,
            geometry_wkb: OnceLock::new(),
        }
    }
}

/// Python-visible wrapper around [`DelineationAreaOnlyResult`].
#[pyclass(name = "AreaOnlyResult")]
pub struct PyAreaOnlyResult {
    inner: DelineationAreaOnlyResult,
}

impl PyAreaOnlyResult {
    /// Wrap a [`DelineationAreaOnlyResult`] from the engine.
    pub fn from_result(result: DelineationAreaOnlyResult) -> Self {
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
        refined_outlet_tuple(self.inner.refinement())
    }

    /// Debug string representation of the resolution method.
    #[getter]
    fn resolution_method(&self) -> String {
        format!("{:?}", self.inner.resolution_method())
    }

    /// All upstream atom IDs (including the terminal atom).
    #[getter]
    fn upstream_atom_ids(&self) -> Vec<i64> {
        self.inner
            .upstream_atom_ids()
            .iter()
            .map(|id| id.get())
            .collect()
    }

    /// Geodesic watershed area in km².
    #[getter]
    fn area_km2(&self) -> f64 {
        self.inner.area_km2().as_f64()
    }

    /// Watershed geometry bounding box as `(minx, miny, maxx, maxy)`.
    #[getter]
    fn geometry_bbox(&self) -> Option<(f64, f64, f64, f64)> {
        self.inner.geometry().bounding_rect().map(|rect| {
            let min = rect.min();
            let max = rect.max();
            (min.x, min.y, max.x, max.y)
        })
    }

    /// Watershed geometry encoded as OGC WKB bytes (little-endian, 2D).
    #[getter]
    fn geometry_wkb<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if let Some(bytes) = self.geometry_wkb.get() {
            return Ok(PyBytes::new(py, bytes));
        }

        let encoded = encode_wkb_multi_polygon(self.inner.geometry())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        match self.geometry_wkb.set(encoded) {
            Ok(()) => match self.geometry_wkb.get() {
                Some(bytes) => Ok(PyBytes::new(py, bytes)),
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "failed to cache geometry WKB",
                )),
            },
            Err(bytes) => match self.geometry_wkb.get() {
                Some(cached) => Ok(PyBytes::new(py, cached)),
                None => Ok(PyBytes::new(py, &bytes)),
            },
        }
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

#[pymethods]
impl PyAreaOnlyResult {
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
        refined_outlet_tuple(self.inner.refinement())
    }

    /// Debug string representation of the resolution method.
    #[getter]
    fn resolution_method(&self) -> String {
        format!("{:?}", self.inner.resolution_method())
    }

    /// All upstream atom IDs (including the terminal atom).
    #[getter]
    fn upstream_atom_ids(&self) -> Vec<i64> {
        self.inner
            .upstream_atom_ids()
            .iter()
            .map(|id| id.get())
            .collect()
    }

    /// Geodesic watershed area in km².
    #[getter]
    fn area_km2(&self) -> f64 {
        self.inner.area_km2().as_f64()
    }

    fn __repr__(&self) -> String {
        format!(
            "AreaOnlyResult(terminal_atom_id={}, area_km2={:.2}, upstream_count={})",
            self.inner.terminal_atom_id().get(),
            self.inner.area_km2().as_f64(),
            self.inner.upstream_atom_ids().len(),
        )
    }
}

fn refined_outlet_tuple(refinement: &RefinementOutcome) -> Option<(f64, f64)> {
    match refinement {
        RefinementOutcome::Applied { refined_outlet } => {
            Some((refined_outlet.lon, refined_outlet.lat))
        }
        _ => None,
    }
}
