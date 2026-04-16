//! Engine configuration bridged from Python keyword arguments.

use shed_core::algo::{CleanEpsilon, SnapThreshold, DEFAULT_CLEANING_EPSILON};
use shed_core::resolver::{ResolverConfig, SearchRadiusMetres};
use shed_core::DelineationOptions;

/// Holds validated per-engine configuration supplied at construction time.
#[derive(Clone)]
pub struct EngineConfig {
    snap_radius: Option<f64>,
    snap_threshold: Option<u32>,
    clean_epsilon: Option<f64>,
    refine: bool,
}

impl EngineConfig {
    /// Validate and store configuration values.
    ///
    /// Returns a Python `ValueError` if `snap_radius` is provided but is not
    /// finite and positive.
    pub fn new(
        snap_radius: Option<f64>,
        snap_threshold: Option<u32>,
        clean_epsilon: Option<f64>,
        refine: bool,
    ) -> pyo3::PyResult<Self> {
        if let Some(r) = snap_radius {
            SearchRadiusMetres::new(r)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        }
        Ok(Self { snap_radius, snap_threshold, clean_epsilon, refine })
    }

    /// Build a [`DelineationOptions`] from the stored configuration.
    pub fn to_delineation_options(&self) -> pyo3::PyResult<DelineationOptions> {
        let mut opts = DelineationOptions::default().with_refine(self.refine);

        if let Some(r) = self.snap_radius {
            let radius = SearchRadiusMetres::new(r)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            opts = opts.with_resolver_config(ResolverConfig::new().with_search_radius(radius));
        }

        if let Some(t) = self.snap_threshold {
            opts = opts.with_snap_threshold(SnapThreshold::new(t));
        }

        let epsilon = self.clean_epsilon.map(CleanEpsilon::new).unwrap_or(DEFAULT_CLEANING_EPSILON);
        opts = opts.with_clean_epsilon(epsilon);

        Ok(opts)
    }
}
