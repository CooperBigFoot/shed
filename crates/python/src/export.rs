//! Python-exposed GeoParquet export writers.

use std::fs::File;

use pyo3::prelude::*;
use shed_core::export::{
    BasinExportInput, BasinGeoParquetWriter, BasinId, ExportMethod, ExportOptions, ExportOrigin,
    UnitBundleExportInput, UnitBundleExportOptions, UnitBundleGeoParquetWriter,
};

use crate::engine::{PyEngine, default_export_method};
use crate::error::export_err_to_py;
use crate::result::PyDelineationResult;
use crate::staged::{PyPreMergeDrainageUnits, PyTerminalRefinement};

/// Writer object for merged-basin GeoParquet exports.
#[pyclass(name = "BasinGeoParquetWriter")]
#[derive(Debug, Clone, Default)]
pub struct PyBasinGeoParquetWriter;

/// Writer object for pre-merge unit-bundle GeoParquet exports.
#[pyclass(name = "UnitBundleGeoParquetWriter")]
#[derive(Debug, Clone, Default)]
pub struct PyUnitBundleGeoParquetWriter;

#[pymethods]
impl PyBasinGeoParquetWriter {
    #[new]
    fn new() -> Self {
        Self
    }

    /// Write merged delineation results to one GeoParquet file.
    #[pyo3(signature = (engine, path, results, *, basin_ids=None, method=None, allow_default_basin_id=false))]
    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        py: Python<'_>,
        engine: &PyEngine,
        path: &str,
        results: Vec<PyRef<'_, PyDelineationResult>>,
        basin_ids: Option<Vec<String>>,
        method: Option<String>,
        allow_default_basin_id: bool,
    ) -> PyResult<()> {
        if results.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "basin export requires at least one result",
            ));
        }
        let basin_ids = match basin_ids {
            Some(values) => {
                if values.len() != results.len() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "basin_ids length must match results length",
                    ));
                }
                Some(
                    values
                        .into_iter()
                        .map(BasinId::parse)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(export_err_to_py)?,
                )
            }
            None => {
                if !allow_default_basin_id || results.len() != 1 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "basin_ids are required unless allow_default_basin_id=True with exactly one result",
                    ));
                }
                None
            }
        };

        let method = resolve_method(method, engine)?;
        let origins = (0..results.len())
            .map(|index| ExportOrigin::new(format!("pyshed result[{index}]")))
            .collect::<Vec<_>>();
        let inputs = match &basin_ids {
            Some(ids) => ids
                .iter()
                .zip(results.iter())
                .zip(origins.iter())
                .map(|((basin_id, result), origin)| {
                    BasinExportInput::explicit(
                        basin_id,
                        result.inner(),
                        &engine.fabric_identity,
                        method.clone(),
                        origin,
                    )
                })
                .collect::<Vec<_>>(),
            None => results
                .iter()
                .zip(origins.iter())
                .map(|(result, origin)| {
                    BasinExportInput::default_basin_id(
                        result.inner(),
                        &engine.fabric_identity,
                        method.clone(),
                        origin,
                    )
                })
                .collect::<Vec<_>>(),
        };

        let path = path.to_owned();
        py.allow_threads(move || {
            let file = File::create(&path)
                .map_err(|e| pyo3::exceptions::PyOSError::new_err(e.to_string()))?;
            BasinGeoParquetWriter::new(ExportOptions::default())
                .write(file, &inputs)
                .map_err(export_err_to_py)
        })
    }

    fn __repr__(&self) -> &'static str {
        "BasinGeoParquetWriter()"
    }
}

#[pymethods]
impl PyUnitBundleGeoParquetWriter {
    #[new]
    fn new() -> Self {
        Self
    }

    /// Write pre-merge drainage-unit bundles to one GeoParquet file.
    #[pyo3(signature = (engine, path, bundles, refinements, *, method=None))]
    fn write(
        &self,
        py: Python<'_>,
        engine: &PyEngine,
        path: &str,
        bundles: Vec<PyRef<'_, PyPreMergeDrainageUnits>>,
        refinements: Vec<PyRef<'_, PyTerminalRefinement>>,
        method: Option<String>,
    ) -> PyResult<()> {
        if bundles.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "unit-bundle export requires at least one bundle",
            ));
        }
        if bundles.len() != refinements.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "refinements length must match bundles length",
            ));
        }

        let method = resolve_method(method, engine)?;
        let inputs = bundles
            .iter()
            .zip(refinements.iter())
            .map(|(bundle, refinement)| {
                UnitBundleExportInput::new(
                    &bundle.inner,
                    &engine.fabric_identity,
                    method.clone(),
                    &refinement.inner,
                )
            })
            .collect::<Vec<_>>();

        let path = path.to_owned();
        py.allow_threads(move || {
            let file = File::create(&path)
                .map_err(|e| pyo3::exceptions::PyOSError::new_err(e.to_string()))?;
            UnitBundleGeoParquetWriter::new(UnitBundleExportOptions::default())
                .write(file, &inputs)
                .map_err(export_err_to_py)
        })
    }

    fn __repr__(&self) -> &'static str {
        "UnitBundleGeoParquetWriter()"
    }
}

fn resolve_method(method: Option<String>, engine: &PyEngine) -> PyResult<ExportMethod> {
    match method {
        Some(value) => ExportMethod::parse(value).map_err(export_err_to_py),
        None => default_export_method(&engine.config),
    }
}
