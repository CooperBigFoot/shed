//! Engine configuration bridged from Python keyword arguments.

use shed_core::DelineationOptions;
use shed_core::algo::{CleanEpsilon, DEFAULT_CLEANING_EPSILON, SnapThreshold};
use shed_core::resolver::{ResolverConfig, SearchRadiusMetres, SnapStrategy};

/// Holds validated per-engine configuration supplied at construction time.
#[derive(Clone)]
pub struct EngineConfig {
    snap_radius: Option<f64>,
    snap_strategy: Option<SnapStrategy>,
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
        snap_strategy: Option<&str>,
        snap_threshold: Option<u32>,
        clean_epsilon: Option<f64>,
        refine: bool,
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
        })
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
