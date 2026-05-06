//! Python-exposed [`Engine`] wrapper.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use shed_core::Engine;
use shed_core::algo::GeoCoord;
use shed_core::parquet_cache::{
    DEFAULT_PARQUET_CACHE_MAX_BYTES, ParquetFooterCache, ParquetRowGroupCache,
};
use shed_core::session::DatasetSession;
use shed_gdal::{GdalGeometryRepair, GdalRasterSource};

use crate::config::{EngineConfig, RepairGeometry};
use crate::error::engine_err_to_py;
use crate::result::{PyAreaOnlyResult, PyDelineationResult};

const MAX_PARQUET_CACHE_MB: u64 = 1_048_576;
const BYTES_PER_MIB: u64 = 1024 * 1024;
const DEFAULT_PARQUET_CACHE_MAX_MB: u64 = DEFAULT_PARQUET_CACHE_MAX_BYTES / BYTES_PER_MIB;

/// Validate that `lat` is in [-90, 90] and `lon` is in [-180, 180].
fn validate_coord(lat: f64, lon: f64) -> PyResult<()> {
    if !(-90.0..=90.0).contains(&lat) {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "latitude {lat} is outside [-90, 90]"
        )));
    }
    if !(-180.0..=180.0).contains(&lon) {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "longitude {lon} is outside [-180, 180]"
        )));
    }
    Ok(())
}

fn is_remote_dataset_path(path: &str) -> bool {
    ["http://", "https://", "s3://", "gs://", "az://", "r2://"]
        .iter()
        .any(|scheme| path.starts_with(scheme))
}

fn resolve_parquet_cache_enabled(dataset_path: &str, explicit: Option<bool>) -> bool {
    explicit.unwrap_or_else(|| is_remote_dataset_path(dataset_path))
}

fn resolve_parquet_cache_max_bytes(enabled: bool, max_mb: u64) -> PyResult<Option<u64>> {
    if !enabled {
        return Ok(None);
    }

    if max_mb == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "parquet_cache_max_mb must be > 0 when parquet_cache=True",
        ));
    }
    if max_mb > MAX_PARQUET_CACHE_MB {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "parquet_cache_max_mb must be <= {MAX_PARQUET_CACHE_MB}"
        )));
    }

    max_mb.checked_mul(BYTES_PER_MIB).map(Some).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err("parquet_cache_max_mb overflows bytes")
    })
}

enum PyDelineateOutput {
    Geometry(PyDelineationResult),
    AreaOnly(PyAreaOnlyResult),
}

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
    /// snap_strategy:
    ///     Optional snap ranking strategy: `"weight-first"` (default, matches
    ///     HFX v0.2 contract) or `"distance-first"` (opt-in; use for datasets
    ///     whose weights are not hydrologically rank-meaningful).
    /// snap_threshold:
    ///     Minimum upstream-pixel count for stream-network snapping. Defaults
    ///     to 1 000 cells.
    /// clean_epsilon:
    ///     Topology-cleaning buffer epsilon in degrees. Defaults to 1e-5 deg.
    /// refine:
    ///     Whether to run the raster-based terminal refinement step. Default
    ///     is `True`.
    /// repair_geometry:
    ///     Optional geometry repair mode: `"auto"` (default), `"clean"`, or
    ///     `False` use pure-Rust topology cleaning; `"gdal"` opts into GDAL
    ///     geometry repair. `None` is treated as `"auto"`.
    /// parquet_cache:
    ///     Whether to cache Parquet row groups. Defaults to `True` for remote
    ///     dataset paths (`http://`, `https://`, `s3://`, `gs://`, `az://`,
    ///     `r2://`) and `False` for local paths. Pass `False` explicitly to
    ///     opt out for remote datasets.
    /// parquet_cache_max_mb:
    ///     Maximum cache budget in MiB when `parquet_cache` is enabled.
    #[new]
    #[pyo3(signature = (dataset_path, **kwargs))]
    fn new(
        py: Python<'_>,
        dataset_path: &str,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        const ALLOWED: &[&str] = &[
            "snap_radius",
            "snap_strategy",
            "snap_threshold",
            "clean_epsilon",
            "refine",
            "repair_geometry",
            "parquet_cache",
            "parquet_cache_max_mb",
        ];
        crate::kwargs::validate_kwargs(kwargs, ALLOWED, crate::kwargs::KwargContext::EngineNew)?;

        // Extract typed values from kwargs (None when missing, defaults applied below).
        let snap_radius: Option<f64> = kwargs
            .and_then(|k| k.get_item("snap_radius").ok().flatten())
            .map(|v| v.extract())
            .transpose()?;
        let snap_strategy: Option<String> = kwargs
            .and_then(|k| k.get_item("snap_strategy").ok().flatten())
            .map(|v| v.extract())
            .transpose()?;
        let snap_threshold: Option<u32> = kwargs
            .and_then(|k| k.get_item("snap_threshold").ok().flatten())
            .map(|v| v.extract())
            .transpose()?;
        let clean_epsilon: Option<f64> = kwargs
            .and_then(|k| k.get_item("clean_epsilon").ok().flatten())
            .map(|v| v.extract())
            .transpose()?;
        let refine: bool = kwargs
            .and_then(|k| k.get_item("refine").ok().flatten())
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(true);
        let repair_geometry = RepairGeometry::parse(
            kwargs
                .and_then(|k| k.get_item("repair_geometry").ok().flatten())
                .as_ref(),
        )?;
        let parquet_cache_explicit: Option<bool> = kwargs
            .and_then(|k| k.get_item("parquet_cache").ok().flatten())
            .map(|v| v.extract())
            .transpose()?;
        let parquet_cache_enabled =
            resolve_parquet_cache_enabled(dataset_path, parquet_cache_explicit);
        let parquet_cache_max_mb: u64 = kwargs
            .and_then(|k| k.get_item("parquet_cache_max_mb").ok().flatten())
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(DEFAULT_PARQUET_CACHE_MAX_MB);

        // Validate cache budget before releasing the GIL.
        let parquet_cache_max_bytes =
            resolve_parquet_cache_max_bytes(parquet_cache_enabled, parquet_cache_max_mb)?;

        // Validate config before releasing the GIL (config errors are cheap/immediate).
        let config = EngineConfig::new(
            snap_radius,
            snap_strategy.as_deref(),
            snap_threshold,
            clean_epsilon,
            refine,
            repair_geometry,
        )?;

        let row_group_cache = parquet_cache_max_bytes.map(ParquetRowGroupCache::new);
        let footer_cache = parquet_cache_max_bytes.map(|_| ParquetFooterCache::new());

        if let Some(max_bytes) = parquet_cache_max_bytes {
            tracing::info!(max_bytes = max_bytes, "parquet_cache enabled");
        }

        // Release the GIL for the synchronous I/O path (manifest + graph + catchment
        // id scan). This keeps the interpreter responsive and allows KeyboardInterrupt
        // during slow remote cold-starts.
        let dataset_path = dataset_path.to_owned();
        let session = py
            .allow_threads(move || {
                DatasetSession::open_with_caches(&dataset_path, row_group_cache, footer_cache)
            })
            .map_err(crate::error::dataset_err)?;

        let mut builder = Engine::builder(session).with_raster_source(GdalRasterSource::new());
        if config.requests_gdal_geometry_repair() {
            builder = builder.with_geometry_repair(GdalGeometryRepair::new());
        }
        let engine = builder.build();

        Ok(Self {
            engine: Arc::new(engine),
            config,
        })
    }

    /// Delineate the watershed upstream of a single outlet.
    ///
    /// Parameters
    /// ----------
    /// lat:
    ///     Outlet latitude in decimal degrees (EPSG:4326).
    /// lon:
    ///     Outlet longitude in decimal degrees (EPSG:4326).
    /// geometry:
    ///     When `True`, return a full `DelineationResult`. When `False`,
    ///     return scalar metadata and area without retaining geometry.
    ///
    /// Returns
    /// -------
    /// DelineationResult | AreaOnlyResult
    #[pyo3(signature = (**kwargs))]
    fn delineate(&self, py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Py<PyAny>> {
        const ALLOWED: &[&str] = &["lat", "lon", "geometry"];
        crate::kwargs::validate_kwargs(kwargs, ALLOWED, crate::kwargs::KwargContext::Delineate)?;

        // Extract required lat / lon — raise a parallel-to-PyO3 error if missing.
        let lat: f64 = kwargs
            .and_then(|k| k.get_item("lat").ok().flatten())
            .ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err(
                    "Engine.delineate() missing required keyword argument: 'lat'",
                )
            })?
            .extract()?;
        let lon: f64 = kwargs
            .and_then(|k| k.get_item("lon").ok().flatten())
            .ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err(
                    "Engine.delineate() missing required keyword argument: 'lon'",
                )
            })?
            .extract()?;
        let geometry: bool = kwargs
            .and_then(|k| k.get_item("geometry").ok().flatten())
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(true);

        validate_coord(lat, lon)?;

        let engine = self.engine.clone();
        let options = self.config.to_delineation_options()?;

        let output = py.allow_threads(move || {
            let coord = GeoCoord::new(lon, lat);
            if geometry {
                engine
                    .delineate(coord, &options)
                    .map(PyDelineationResult::from_result)
                    .map(PyDelineateOutput::Geometry)
                    .map_err(engine_err_to_py)
            } else {
                engine
                    .delineate_area_only(coord, &options)
                    .map(PyAreaOnlyResult::from_result)
                    .map(PyDelineateOutput::AreaOnly)
                    .map_err(engine_err_to_py)
            }
        })?;

        match output {
            PyDelineateOutput::Geometry(result) => Ok(Py::new(py, result)?.into_any()),
            PyDelineateOutput::AreaOnly(result) => Ok(Py::new(py, result)?.into_any()),
        }
    }

    /// Delineate watersheds for a batch of outlets sharing the same options.
    ///
    /// Parameters
    /// ----------
    /// outlets:
    ///     A list of dicts, each with `"lat"` and `"lon"` keys.
    /// progress:
    ///     Optional callable invoked after each outlet completes.  Receives a
    ///     single dict with keys `index`, `total`, `lat`, `lon`,
    ///     `duration_ms`, `status` (`"ok"` or `"error"`), `n_catchments`
    ///     (on success), and `error` (on failure).  Exceptions raised by the
    ///     callback are logged via `tracing::warn!` and otherwise ignored.
    ///     Passing `progress` disables Rayon parallelism and runs the batch
    ///     sequentially to preserve monotonic callback order.
    ///
    /// Returns
    /// -------
    /// list[DelineationResult]
    ///     Results in input order. Raises on the first failure encountered.
    #[pyo3(signature = (outlets, **kwargs))]
    fn delineate_batch(
        &self,
        py: Python<'_>,
        outlets: &Bound<'_, PyList>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyDelineationResult>> {
        const ALLOWED: &[&str] = &["progress"];
        crate::kwargs::validate_kwargs(
            kwargs,
            ALLOWED,
            crate::kwargs::KwargContext::DelineateBatch,
        )?;

        let progress: Option<PyObject> = kwargs
            .and_then(|k| k.get_item("progress").ok().flatten())
            .map(|v| {
                if !v.is_callable() {
                    let type_name = v.get_type().name()?;
                    return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                        "progress must be callable, not {type_name}"
                    )));
                }
                Ok(v.unbind())
            })
            .transpose()?;

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
                validate_coord(lat, lon)?;
                Ok((lat, lon))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let total = parsed.len();
        let engine = self.engine.clone();
        let options = self.config.to_delineation_options()?;

        tracing::info!(
            n_outlets = total,
            has_progress = progress.is_some(),
            "starting batch delineation"
        );

        let batch_start = std::time::Instant::now();

        let py_results = if let Some(cb) = progress {
            // Sequential path: fire the progress callback after each outlet.
            // Sequential ordering is required so callback events have a
            // monotonically increasing `index`. Parallel-with-progress is a
            // v2 concern (would require channel fan-in for ordering).
            sequential_delineate_with_progress(py, &parsed, total, &engine, &options, cb)?
        } else {
            // Parallel path (default): preserve Rayon parallelism via
            // `delineate_batch_uniform`. Identical to the pre-Phase-C body.
            let coords: Vec<GeoCoord> = parsed
                .iter()
                .map(|(lat, lon)| GeoCoord::new(*lon, *lat))
                .collect();

            let results: Vec<Result<PyDelineationResult, shed_core::EngineError>> = py
                .allow_threads(move || {
                    engine
                        .delineate_batch_uniform(&coords, &options)
                        .into_iter()
                        .map(|r| r.map(PyDelineationResult::from_result))
                        .collect()
                });

            let mut py_results = Vec::with_capacity(results.len());
            for (failed_index, r) in results.into_iter().enumerate() {
                match r {
                    Ok(result) => py_results.push(result),
                    Err(engine_err) => {
                        tracing::warn!(
                            n_outlets = total,
                            failed_index,
                            elapsed_ms = batch_start.elapsed().as_millis() as u64,
                            error = %engine_err,
                            "batch delineation aborted"
                        );
                        return Err(engine_err_to_py(engine_err));
                    }
                }
            }
            py_results
        };

        tracing::info!(
            n_outlets = total,
            elapsed_ms = batch_start.elapsed().as_millis() as u64,
            "batch delineation complete"
        );

        Ok(py_results)
    }
}

/// Sequential per-outlet delineation that fires `cb` after each outlet
/// completes. Bails on the first engine error (matches the parallel path).
///
/// Each outlet's compute runs inside `py.allow_threads`; the callback is
/// invoked under a freshly re-acquired GIL via `Python::with_gil`. Callback
/// exceptions are caught and logged via `tracing::warn!`, never propagated.
fn sequential_delineate_with_progress(
    py: Python<'_>,
    parsed: &[(f64, f64)],
    total: usize,
    engine: &Arc<Engine>,
    options: &shed_core::DelineationOptions,
    cb: PyObject,
) -> PyResult<Vec<PyDelineationResult>> {
    let batch_start = std::time::Instant::now();
    let mut py_results = Vec::with_capacity(total);

    for (index, &(lat, lon)) in parsed.iter().enumerate() {
        let t0 = std::time::Instant::now();
        let coord = GeoCoord::new(lon, lat);
        let engine_ref = engine.clone();
        let options_ref = options.clone();

        let outcome: Result<shed_core::DelineationResult, shed_core::EngineError> =
            py.allow_threads(move || engine_ref.delineate(coord, &options_ref));

        // Build and fire the progress event before recording the result.
        let duration_ms = t0.elapsed().as_millis() as u64;
        let event_result = Python::with_gil(|py| -> PyResult<Py<PyDict>> {
            let d = PyDict::new(py);
            d.set_item("index", index)?;
            d.set_item("total", total)?;
            d.set_item("lat", lat)?;
            d.set_item("lon", lon)?;
            d.set_item("duration_ms", duration_ms)?;
            match &outcome {
                Ok(res) => {
                    d.set_item("status", "ok")?;
                    d.set_item("n_catchments", res.upstream_atom_ids().len())?;
                }
                Err(e) => {
                    d.set_item("status", "error")?;
                    d.set_item("error", e.to_string())?;
                }
            }
            Ok(d.unbind())
        });

        match event_result {
            Ok(d) => {
                let call_result = Python::with_gil(|py| cb.call1(py, (d.bind(py),)));
                if let Err(err) = call_result {
                    tracing::warn!(
                        error = %err,
                        "delineate_batch progress callback raised; continuing"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "delineate_batch failed to build progress event; continuing"
                );
            }
        }

        // Record result — bail on first error (preserving original semantics).
        match outcome {
            Ok(result) => {
                py_results.push(PyDelineationResult::from_result(result));
            }
            Err(engine_err) => {
                tracing::warn!(
                    n_outlets = total,
                    failed_index = index,
                    elapsed_ms = batch_start.elapsed().as_millis() as u64,
                    error = %engine_err,
                    "batch delineation aborted"
                );
                return Err(engine_err_to_py(engine_err));
            }
        }
    }

    Ok(py_results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_dataset_schemes_are_detected() {
        for path in [
            "http://example.com/hfx",
            "https://example.com/hfx",
            "s3://bucket/hfx",
            "gs://bucket/hfx",
            "az://container/hfx",
            "r2://bucket/hfx",
        ] {
            assert!(is_remote_dataset_path(path), "{path} should be remote");
        }
    }

    #[test]
    fn local_and_file_dataset_paths_are_not_remote() {
        for path in [
            "/data/hfx",
            "./data/hfx",
            "../data/hfx",
            "file:///data/hfx",
            "hfx",
            "S3://bucket/hfx",
        ] {
            assert!(!is_remote_dataset_path(path), "{path} should be local");
        }
    }

    #[test]
    fn unset_parquet_cache_enables_for_remote_paths() {
        assert!(resolve_parquet_cache_enabled("s3://bucket/hfx", None));
        assert!(!resolve_parquet_cache_enabled("/data/hfx", None));
    }

    #[test]
    fn explicit_parquet_cache_value_is_honored() {
        assert!(!resolve_parquet_cache_enabled(
            "https://example.com/hfx",
            Some(false)
        ));
        assert!(resolve_parquet_cache_enabled("/data/hfx", Some(true)));
    }

    #[test]
    fn enabled_parquet_cache_validates_max_mb() {
        assert_eq!(
            resolve_parquet_cache_max_bytes(true, 1).unwrap(),
            Some(BYTES_PER_MIB)
        );

        let zero_message = resolve_parquet_cache_max_bytes(true, 0)
            .unwrap_err()
            .to_string();
        assert!(
            zero_message.contains("parquet_cache_max_mb must be > 0 when parquet_cache=True"),
            "{zero_message}"
        );

        let too_large_message = resolve_parquet_cache_max_bytes(true, MAX_PARQUET_CACHE_MB + 1)
            .unwrap_err()
            .to_string();
        assert!(
            too_large_message.contains(&format!(
                "parquet_cache_max_mb must be <= {MAX_PARQUET_CACHE_MB}"
            )),
            "{too_large_message}"
        );
    }

    #[test]
    fn disabled_parquet_cache_ignores_max_mb() {
        assert_eq!(resolve_parquet_cache_max_bytes(false, 0).unwrap(), None);
        assert_eq!(
            resolve_parquet_cache_max_bytes(false, MAX_PARQUET_CACHE_MB + 1).unwrap(),
            None
        );
    }
}
