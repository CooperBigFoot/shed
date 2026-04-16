//! Python-exposed [`Engine`] wrapper.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use shed_core::algo::GeoCoord;
use shed_core::session::DatasetSession;
use shed_core::Engine;
use shed_gdal::{GdalGeometryRepair, GdalRasterSource};

use crate::config::EngineConfig;
use crate::error::engine_err_to_py;
use crate::result::PyDelineationResult;

/// Watershed delineation engine exposed to Python.
///
/// Construct with a path to an HFX dataset directory. Optional keyword
/// arguments tune the outlet resolution and geometry-cleaning steps.
#[pyclass(name = "Engine")]
pub struct PyEngine {
    engine: Arc<Engine>,
    config: EngineConfig,
}

#[pymethods]
impl PyEngine {
    /// Open an HFX dataset and build the engine.
    ///
    /// Parameters
    /// ----------
    /// dataset_path:
    ///     Path to the HFX dataset root directory.
    /// snap_radius:
    ///     Optional snap-path search radius in metres (must be finite and
    ///     positive). Defaults to 1 000 m.
    /// snap_threshold:
    ///     Minimum upstream-pixel count for stream-network snapping. Defaults
    ///     to 1 000 cells.
    /// clean_epsilon:
    ///     Topology-cleaning buffer epsilon in degrees. Defaults to 1e-5 deg.
    /// refine:
    ///     Whether to run the raster-based terminal refinement step. Default
    ///     is `True`.
    #[new]
    #[pyo3(signature = (dataset_path, *, snap_radius=None, snap_threshold=None, clean_epsilon=None, refine=true))]
    fn new(
        dataset_path: &str,
        snap_radius: Option<f64>,
        snap_threshold: Option<u32>,
        clean_epsilon: Option<f64>,
        refine: bool,
    ) -> PyResult<Self> {
        let session = DatasetSession::open(dataset_path).map_err(crate::error::dataset_err)?;

        let engine = Engine::builder(session)
            .with_raster_source(GdalRasterSource::new())
            .with_geometry_repair(GdalGeometryRepair::new())
            .build();

        let config = EngineConfig::new(snap_radius, snap_threshold, clean_epsilon, refine)?;

        Ok(Self { engine: Arc::new(engine), config })
    }

    /// Delineate the watershed upstream of a single outlet.
    ///
    /// Parameters
    /// ----------
    /// lat:
    ///     Outlet latitude in decimal degrees (EPSG:4326).
    /// lon:
    ///     Outlet longitude in decimal degrees (EPSG:4326).
    ///
    /// Returns
    /// -------
    /// DelineationResult
    #[pyo3(signature = (*, lat, lon))]
    fn delineate(&self, py: Python<'_>, lat: f64, lon: f64) -> PyResult<PyDelineationResult> {
        let engine = self.engine.clone();
        let options = self.config.to_delineation_options()?;

        py.allow_threads(move || {
            let coord = GeoCoord::new(lon, lat);
            engine
                .delineate(coord, &options)
                .map(PyDelineationResult::from_result)
                .map_err(engine_err_to_py)
        })
    }

    /// Delineate watersheds for a batch of outlets sharing the same options.
    ///
    /// Parameters
    /// ----------
    /// outlets:
    ///     A list of dicts, each with `"lat"` and `"lon"` keys.
    ///
    /// Returns
    /// -------
    /// list[DelineationResult]
    ///     Results in input order. Raises on the first failure encountered.
    #[pyo3(signature = (outlets,))]
    fn delineate_batch(
        &self,
        py: Python<'_>,
        outlets: &Bound<'_, PyList>,
    ) -> PyResult<Vec<PyDelineationResult>> {
        // Parse all outlets before releasing the GIL.
        let parsed: Vec<(f64, f64)> = outlets
            .iter()
            .map(|item| {
                let dict = item.downcast::<PyDict>()?;
                let lat: f64 = dict
                    .get_item("lat")?
                    .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("missing 'lat'"))?
                    .extract()?;
                let lon: f64 = dict
                    .get_item("lon")?
                    .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("missing 'lon'"))?
                    .extract()?;
                Ok((lat, lon))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let engine = self.engine.clone();
        let options = self.config.to_delineation_options()?;

        // Run the batch without holding the GIL; rayon parallelism is inside the engine.
        let results: Vec<Result<PyDelineationResult, String>> =
            py.allow_threads(move || {
                let coords: Vec<GeoCoord> = parsed
                    .iter()
                    .map(|(lat, lon)| GeoCoord::new(*lon, *lat))
                    .collect();

                engine
                    .delineate_batch_uniform(&coords, &options)
                    .into_iter()
                    .map(|r| {
                        r.map(PyDelineationResult::from_result)
                            .map_err(|e| e.to_string())
                    })
                    .collect()
            });

        // Re-raise the first error in input order.
        let mut py_results = Vec::with_capacity(results.len());
        for (i, r) in results.into_iter().enumerate() {
            match r {
                Ok(result) => py_results.push(result),
                Err(msg) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "outlet {} failed: {}",
                        i, msg
                    )))
                }
            }
        }

        Ok(py_results)
    }
}
