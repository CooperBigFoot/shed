//! Engine configuration bridged from Python keyword arguments.

use pyo3::types::PyAnyMethods;
use shed_core::DelineationOptions;
use shed_core::algo::{CleanEpsilon, DEFAULT_CLEANING_EPSILON, SnapThreshold};
use shed_core::resolver::{ResolverConfig, SearchRadiusMetres, SnapStrategy};

/// Selects the optional external geometry repair backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairGeometry {
    /// Use the default pure-Rust topology cleaning path.
    Auto,
    /// Use the GDAL-backed repairer.
    Gdal,
    /// Use the pure-Rust topology cleaning path.
    Clean,
}

impl RepairGeometry {
    /// Parse the Python-facing repair mode.
    pub fn parse(value: Option<&pyo3::Bound<'_, pyo3::PyAny>>) -> pyo3::PyResult<Self> {
        let Some(value) = value else {
            return Ok(Self::Auto);
        };
        if value.is_none() {
            return Ok(Self::Auto);
        }
        if let Ok(flag) = value.extract::<bool>() {
            return match flag {
                false => Ok(Self::Clean),
                true => Err(pyo3::exceptions::PyValueError::new_err(
                    "invalid repair_geometry True; expected 'auto', 'gdal', 'clean', False, or None",
                )),
            };
        }
        let mode = value.extract::<String>()?;
        match mode.as_str() {
            "auto" => Ok(Self::Auto),
            "gdal" => Ok(Self::Gdal),
            "clean" => Ok(Self::Clean),
            other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "invalid repair_geometry {other:?}; expected 'auto', 'gdal', 'clean', False, or None"
            ))),
        }
    }

    /// Return true when the GDAL repairer should be installed in the engine.
    pub fn requests_gdal(self) -> bool {
        matches!(self, Self::Gdal)
    }
}

/// Holds validated per-engine configuration supplied at construction time.
#[derive(Clone)]
pub struct EngineConfig {
    snap_radius: Option<f64>,
    snap_strategy: Option<SnapStrategy>,
    snap_threshold: Option<u32>,
    clean_epsilon: Option<f64>,
    refine: bool,
    repair_geometry: RepairGeometry,
}

impl EngineConfig {
    /// Validate and store configuration values.
    ///
    /// Returns a Python `ValueError` if `snap_radius` is provided but is not
    /// finite and positive.
    pub fn new(
        snap_radius: Option<f64>,
        snap_strategy: Option<&str>,
        snap_threshold: Option<u32>,
        clean_epsilon: Option<f64>,
        refine: bool,
        repair_geometry: RepairGeometry,
    ) -> pyo3::PyResult<Self> {
        if let Some(r) = snap_radius {
            SearchRadiusMetres::new(r)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        }
        let snap_strategy = match snap_strategy {
            Some("distance-first") => Some(SnapStrategy::DistanceFirst),
            Some("weight-first") => Some(SnapStrategy::WeightFirst),
            Some(other) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid snap_strategy {other:?}; expected 'distance-first' or 'weight-first'"
                )));
            }
            None => None,
        };
        Ok(Self {
            snap_radius,
            snap_strategy,
            snap_threshold,
            clean_epsilon,
            refine,
            repair_geometry,
        })
    }

    /// Return true when this config should install the GDAL geometry repairer.
    pub fn requests_gdal_geometry_repair(&self) -> bool {
        self.repair_geometry.requests_gdal()
    }

    /// Build a [`DelineationOptions`] from the stored configuration.
    pub fn to_delineation_options(&self) -> pyo3::PyResult<DelineationOptions> {
        let mut opts = DelineationOptions::default().with_refine(self.refine);
        let mut resolver_config = ResolverConfig::new();

        if let Some(r) = self.snap_radius {
            let radius = SearchRadiusMetres::new(r)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            resolver_config = resolver_config.with_search_radius(radius);
        }

        if let Some(strategy) = self.snap_strategy {
            resolver_config = resolver_config.with_snap_strategy(strategy);
        }

        opts = opts.with_resolver_config(resolver_config);

        if let Some(t) = self.snap_threshold {
            opts = opts.with_snap_threshold(SnapThreshold::new(t));
        }

        let epsilon = self
            .clean_epsilon
            .map(CleanEpsilon::new)
            .unwrap_or(DEFAULT_CLEANING_EPSILON);
        opts = opts.with_clean_epsilon(epsilon);

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::{PyBool, PyString};

    #[test]
    fn repair_geometry_requests_gdal_only_for_gdal_mode() {
        assert!(!RepairGeometry::Auto.requests_gdal());
        assert!(RepairGeometry::Gdal.requests_gdal());
        assert!(!RepairGeometry::Clean.requests_gdal());
    }

    #[test]
    fn repair_geometry_parse_accepts_supported_python_values() {
        pyo3::Python::with_gil(|py| {
            assert_eq!(RepairGeometry::parse(None).unwrap(), RepairGeometry::Auto);
            assert_eq!(
                RepairGeometry::parse(Some(py.None().bind(py))).unwrap(),
                RepairGeometry::Auto
            );
            assert_eq!(
                RepairGeometry::parse(Some(PyString::new(py, "auto").as_any())).unwrap(),
                RepairGeometry::Auto
            );
            assert_eq!(
                RepairGeometry::parse(Some(PyString::new(py, "gdal").as_any())).unwrap(),
                RepairGeometry::Gdal
            );
            assert_eq!(
                RepairGeometry::parse(Some(PyString::new(py, "clean").as_any())).unwrap(),
                RepairGeometry::Clean
            );
            assert_eq!(
                RepairGeometry::parse(Some(PyBool::new(py, false).as_any())).unwrap(),
                RepairGeometry::Clean
            );
        });
    }

    #[test]
    fn repair_geometry_parse_rejects_invalid_python_values() {
        pyo3::Python::with_gil(|py| {
            let true_err = RepairGeometry::parse(Some(PyBool::new(py, true).as_any()))
                .expect_err("True should be invalid");
            assert!(
                true_err.to_string().contains("repair_geometry"),
                "message should mention repair_geometry: {true_err}"
            );

            let bogus_err = RepairGeometry::parse(Some(PyString::new(py, "bogus").as_any()))
                .expect_err("unknown mode should be invalid");
            assert!(
                bogus_err.to_string().contains("auto"),
                "message should list valid modes: {bogus_err}"
            );
        });
    }

    #[test]
    fn engine_config_requests_gdal_only_when_explicitly_configured() {
        let default_config = EngineConfig::new(None, None, None, None, true, RepairGeometry::Auto)
            .expect("default config should be valid");
        let auto_config = EngineConfig::new(None, None, None, None, true, RepairGeometry::Auto)
            .expect("auto config should be valid");
        let clean_config = EngineConfig::new(None, None, None, None, true, RepairGeometry::Clean)
            .expect("clean config should be valid");
        let false_config = EngineConfig::new(None, None, None, None, true, RepairGeometry::Clean)
            .expect("False config should be valid");
        let gdal_config = EngineConfig::new(None, None, None, None, true, RepairGeometry::Gdal)
            .expect("gdal config should be valid");

        assert!(!default_config.requests_gdal_geometry_repair());
        assert!(!auto_config.requests_gdal_geometry_repair());
        assert!(!clean_config.requests_gdal_geometry_repair());
        assert!(!false_config.requests_gdal_geometry_repair());
        assert!(gdal_config.requests_gdal_geometry_repair());
    }
}
