//! Engine composition layer — wires outlet resolution, upstream traversal,
//! terminal refinement, and watershed assembly into a single `delineate()` call.

use geo::MultiPolygon;
use hfx_core::AtomId;
use rayon::prelude::*;
use tracing::instrument;

use crate::algo::coord::GeoCoord;
use crate::algo::{
    AreaKm2, CleanEpsilon, DEFAULT_CLEANING_EPSILON, GeometryRepair, HoleFillMode, RasterSource,
    RefinementError, SnapThreshold, TraversalError, WkbDecodeError, WkbEncodeError,
    collect_upstream, decode_wkb_multi_polygon, encode_wkb_multi_polygon,
    refine_terminal_from_source,
};
use crate::assembly::{AssemblyOptions, assemble_watershed};
use crate::error::SessionError;
use crate::resolver::{
    OutletResolutionError, ResolutionMethod, ResolvedOutlet, ResolverConfig, resolve_outlet,
};
use crate::session::DatasetSession;

// ── RefinementOutcome ─────────────────────────────────────────────────────────

/// Records what happened during the optional terminal-refinement step.
#[derive(Debug, Clone, PartialEq)]
pub enum RefinementOutcome {
    /// Refinement ran successfully and the outlet was snapped.
    Applied {
        /// The refined outlet coordinate returned by the raster snap.
        refined_outlet: GeoCoord,
    },
    /// No raster files are registered with the session.
    NoRastersAvailable,
    /// No [`RasterSource`] implementation was attached to the engine.
    NoRasterSourceProvided,
    /// Refinement was disabled by the caller via [`DelineationOptions::with_refine`].
    Disabled,
}

// ── DelineationResult ─────────────────────────────────────────────────────────

/// The output of a successful [`Engine::delineate`] call.
#[derive(Debug, Clone)]
pub struct DelineationResult {
    terminal_atom_id: AtomId,
    input_outlet: GeoCoord,
    resolved_outlet: GeoCoord,
    resolution_method: ResolutionMethod,
    upstream_atom_ids: Vec<AtomId>,
    refinement: RefinementOutcome,
    geometry: MultiPolygon<f64>,
    area_km2: AreaKm2,
}

impl DelineationResult {
    /// Return the terminal atom ID that the outlet resolved to.
    pub fn terminal_atom_id(&self) -> AtomId {
        self.terminal_atom_id
    }

    /// Return the original input outlet coordinate.
    pub fn input_outlet(&self) -> GeoCoord {
        self.input_outlet
    }

    /// Return the resolved outlet coordinate (may differ after snapping).
    pub fn resolved_outlet(&self) -> GeoCoord {
        self.resolved_outlet
    }

    /// Return a reference to the resolution provenance.
    pub fn resolution_method(&self) -> &ResolutionMethod {
        &self.resolution_method
    }

    /// Return the slice of all upstream atom IDs (including the terminal).
    pub fn upstream_atom_ids(&self) -> &[AtomId] {
        &self.upstream_atom_ids
    }

    /// Return a reference to the refinement outcome.
    pub fn refinement(&self) -> &RefinementOutcome {
        &self.refinement
    }

    /// Return a reference to the assembled watershed geometry.
    pub fn geometry(&self) -> &MultiPolygon<f64> {
        &self.geometry
    }

    /// Return the geodesic watershed area in km².
    pub fn area_km2(&self) -> AreaKm2 {
        self.area_km2
    }

    /// Consume the result and return the watershed geometry.
    pub fn into_geometry(self) -> MultiPolygon<f64> {
        self.geometry
    }

    /// Encode the watershed geometry to OGC WKB bytes (little-endian, 2D).
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`WkbEncodeError::EncodeFailed`] | The geozero encoder fails |
    pub fn geometry_wkb(&self) -> Result<Vec<u8>, WkbEncodeError> {
        encode_wkb_multi_polygon(&self.geometry)
    }
}

// ── DelineationOptions ────────────────────────────────────────────────────────

/// Per-call configuration knobs for [`Engine::delineate`].
#[derive(Debug, Clone)]
pub struct DelineationOptions {
    resolver_config: ResolverConfig,
    snap_threshold: SnapThreshold,
    hole_fill_mode: HoleFillMode,
    clean_epsilon: CleanEpsilon,
    refine: bool,
}

impl Default for DelineationOptions {
    fn default() -> Self {
        Self {
            resolver_config: ResolverConfig::new(),
            snap_threshold: SnapThreshold::DEFAULT,
            hole_fill_mode: HoleFillMode::RemoveAll,
            clean_epsilon: DEFAULT_CLEANING_EPSILON,
            refine: true,
        }
    }
}

impl DelineationOptions {
    /// Override the outlet resolver configuration.
    pub fn with_resolver_config(mut self, config: ResolverConfig) -> Self {
        self.resolver_config = config;
        self
    }

    /// Override the flow-accumulation snap threshold.
    pub fn with_snap_threshold(mut self, threshold: SnapThreshold) -> Self {
        self.snap_threshold = threshold;
        self
    }

    /// Override the hole-fill strategy applied after geometry assembly.
    pub fn with_hole_fill_mode(mut self, mode: HoleFillMode) -> Self {
        self.hole_fill_mode = mode;
        self
    }

    /// Override the topology-cleaning epsilon.
    pub fn with_clean_epsilon(mut self, epsilon: CleanEpsilon) -> Self {
        self.clean_epsilon = epsilon;
        self
    }

    /// Enable or disable the terminal-refinement step.
    pub fn with_refine(mut self, refine: bool) -> Self {
        self.refine = refine;
        self
    }
}

// ── EngineError ───────────────────────────────────────────────────────────────

/// Errors that can occur during [`Engine::delineate`].
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Fired when the outlet coordinate cannot be resolved to an HFX atom.
    #[error("outlet resolution failed for {outlet}: {source}")]
    Resolution {
        /// The outlet coordinate that was supplied.
        outlet: GeoCoord,
        /// Underlying resolution error.
        source: OutletResolutionError,
    },

    /// Fired when the upstream graph traversal fails for the resolved atom.
    #[error("upstream traversal failed for atom {atom_id}: {source}")]
    Traversal {
        /// The raw atom ID that was traversed from.
        atom_id: i64,
        /// Underlying traversal error.
        source: TraversalError,
    },

    /// Fired when the terminal catchment row cannot be fetched for refinement.
    #[error("failed to fetch terminal catchment for refinement (atom {atom_id}): {source}")]
    TerminalCatchmentFetch {
        /// The raw atom ID whose catchment fetch failed.
        atom_id: i64,
        /// Underlying session error.
        source: SessionError,
    },

    /// Fired when the stored terminal catchment geometry fails WKB decode.
    #[error("failed to decode terminal catchment geometry (atom {atom_id}): {source}")]
    TerminalCatchmentDecode {
        /// The raw atom ID whose geometry could not be decoded.
        atom_id: i64,
        /// Underlying WKB decode error.
        source: WkbDecodeError,
    },

    /// Fired when the raster-based terminal refinement step fails.
    #[error("terminal refinement failed for atom {atom_id}: {source}")]
    Refinement {
        /// The raw atom ID for which refinement was attempted.
        atom_id: i64,
        /// Underlying refinement error.
        source: RefinementError,
    },

    /// Fired when final watershed assembly fails.
    #[error("watershed assembly failed for atom {atom_id}: {message}")]
    Assembly {
        /// The raw atom ID of the terminal atom being assembled.
        atom_id: i64,
        /// Human-readable description of the assembly failure.
        message: String,
        /// The original assembly error, preserved for error-chain inspection.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

// ── EngineBuilder ─────────────────────────────────────────────────────────────

/// Builder for [`Engine`].
pub struct EngineBuilder {
    session: DatasetSession,
    raster_source: Option<Box<dyn RasterSource + Send + Sync>>,
    geometry_repair: Option<Box<dyn GeometryRepair + Send + Sync>>,
}

impl EngineBuilder {
    /// Attach a [`RasterSource`] backend for terminal refinement.
    pub fn with_raster_source(mut self, source: impl RasterSource + Send + Sync + 'static) -> Self {
        self.raster_source = Some(Box::new(source));
        self
    }

    /// Attach a [`GeometryRepair`] backend for post-assembly topology repair.
    pub fn with_geometry_repair(
        mut self,
        repairer: impl GeometryRepair + Send + Sync + 'static,
    ) -> Self {
        self.geometry_repair = Some(Box::new(repairer));
        self
    }

    /// Consume the builder and return a ready-to-use [`Engine`].
    pub fn build(self) -> Engine {
        Engine {
            session: self.session,
            raster_source: self.raster_source,
            geometry_repair: self.geometry_repair,
        }
    }
}

// ── Engine ────────────────────────────────────────────────────────────────────

/// The shed watershed delineation engine.
///
/// Wires outlet resolution, upstream traversal, optional terminal refinement,
/// and watershed assembly into a single [`Engine::delineate`] call.
pub struct Engine {
    session: DatasetSession,
    raster_source: Option<Box<dyn RasterSource + Send + Sync>>,
    geometry_repair: Option<Box<dyn GeometryRepair + Send + Sync>>,
}

impl Engine {
    /// Return a builder for constructing an [`Engine`] from a [`DatasetSession`].
    pub fn builder(session: DatasetSession) -> EngineBuilder {
        EngineBuilder {
            session,
            raster_source: None,
            geometry_repair: None,
        }
    }

    /// Delineate the watershed upstream of `outlet`.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::Resolution`] | Outlet cannot be resolved to an atom |
    /// | [`EngineError::Traversal`] | Upstream graph traversal fails |
    /// | [`EngineError::TerminalCatchmentFetch`] | Terminal catchment row is missing (refinement only) |
    /// | [`EngineError::TerminalCatchmentDecode`] | Terminal catchment WKB is invalid (refinement only) |
    /// | [`EngineError::Refinement`] | Raster snap fails (refinement only) |
    /// | [`EngineError::Assembly`] | Watershed geometry assembly fails |
    #[instrument(skip(self, options), fields(outlet = %outlet))]
    pub fn delineate(
        &self,
        outlet: GeoCoord,
        options: &DelineationOptions,
    ) -> Result<DelineationResult, EngineError> {
        // Step 1: Resolve outlet
        let resolved = resolve_outlet(&self.session, outlet, &options.resolver_config)
            .map_err(|source| EngineError::Resolution { outlet, source })?;
        let terminal = resolved.atom_id;

        // Step 2: Upstream traversal
        let upstream = collect_upstream(terminal, self.session.graph()).map_err(|source| {
            EngineError::Traversal {
                atom_id: terminal.get(),
                source,
            }
        })?;

        // Step 3: Try refinement
        let (refinement, refined_geometry) = self.try_refine(terminal, &resolved, options)?;

        // Step 4: Assembly
        let assembly_options = self.build_assembly_options(options);
        let result = assemble_watershed(
            self.session.catchments(),
            &upstream,
            refined_geometry.as_ref(),
            assembly_options,
        )
        .map_err(|e| EngineError::Assembly {
            atom_id: terminal.get(),
            message: e.to_string(),
            source: Box::new(e),
        })?;
        let (geometry, area_km2) = result.into_parts();

        // Step 5: Compose result
        Ok(DelineationResult {
            terminal_atom_id: terminal,
            input_outlet: resolved.input_coord,
            resolved_outlet: resolved.resolved_coord,
            resolution_method: resolved.method,
            upstream_atom_ids: upstream.into_atom_ids(),
            refinement,
            geometry,
            area_km2,
        })
    }

    /// Delineate watersheds for a heterogeneous batch of (outlet, options) pairs.
    ///
    /// Results are returned in input order. Each element is the `Result` of the
    /// corresponding call — failures do not abort the batch.
    pub fn delineate_batch(
        &self,
        outlets: &[(GeoCoord, DelineationOptions)],
    ) -> Vec<Result<DelineationResult, EngineError>> {
        outlets
            .par_iter()
            .map(|(outlet, opts)| self.delineate(*outlet, opts))
            .collect()
    }

    /// Delineate watersheds for a slice of outlets sharing the same options.
    ///
    /// Results are returned in input order. Each element is the `Result` of the
    /// corresponding call — failures do not abort the batch.
    pub fn delineate_batch_uniform(
        &self,
        outlets: &[GeoCoord],
        options: &DelineationOptions,
    ) -> Vec<Result<DelineationResult, EngineError>> {
        outlets
            .par_iter()
            .map(|outlet| self.delineate(*outlet, options))
            .collect()
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Attempt terminal refinement, returning the outcome and an optional
    /// refined geometry to substitute into assembly.
    fn try_refine(
        &self,
        terminal: AtomId,
        resolved: &ResolvedOutlet,
        options: &DelineationOptions,
    ) -> Result<(RefinementOutcome, Option<MultiPolygon<f64>>), EngineError> {
        if !options.refine {
            return Ok((RefinementOutcome::Disabled, None));
        }
        let raster_paths = match self.session.raster_paths() {
            Some(p) => p,
            None => return Ok((RefinementOutcome::NoRastersAvailable, None)),
        };
        let raster_source = match self.raster_source.as_deref() {
            Some(s) => s,
            None => return Ok((RefinementOutcome::NoRasterSourceProvided, None)),
        };

        // Fetch terminal catchment geometry
        let terminal_atoms = self
            .session
            .catchments()
            .query_geometries_by_ids(&[terminal])
            .map_err(|source| EngineError::TerminalCatchmentFetch {
                atom_id: terminal.get(),
                source,
            })?;
        let terminal_atom = terminal_atoms.into_iter().next().ok_or_else(|| {
            EngineError::TerminalCatchmentFetch {
                atom_id: terminal.get(),
                source: SessionError::integrity(format!(
                    "terminal atom {} not in catchment store",
                    terminal.get()
                )),
            }
        })?;
        let terminal_polygon =
            decode_wkb_multi_polygon(terminal_atom.geometry()).map_err(|source| {
                EngineError::TerminalCatchmentDecode {
                    atom_id: terminal.get(),
                    source,
                }
            })?;

        // Refine
        let refinement_result = refine_terminal_from_source(
            raster_source,
            raster_paths.flow_dir(),
            raster_paths.flow_acc(),
            &terminal_polygon,
            resolved.resolved_coord,
            options.snap_threshold,
        )
        .map_err(|source| EngineError::Refinement {
            atom_id: terminal.get(),
            source,
        })?;

        let refined_coord = refinement_result.snapped_coord();
        let refined_polygon = refinement_result.into_polygon();
        Ok((
            RefinementOutcome::Applied {
                refined_outlet: refined_coord,
            },
            Some(refined_polygon),
        ))
    }

    /// Construct [`AssemblyOptions`] from per-call settings and the engine's
    /// optional geometry-repair backend.
    fn build_assembly_options<'a>(&'a self, options: &DelineationOptions) -> AssemblyOptions<'a> {
        let base = AssemblyOptions::new(options.hole_fill_mode, options.clean_epsilon);
        match self.geometry_repair.as_deref() {
            Some(repairer) => base.with_geometry_repair(repairer),
            None => base,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::DatasetSession;
    use crate::testutil::DatasetBuilder;

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Build a 3-atom linear dataset and open a session.
    ///
    /// Graph: 1 -> 2 -> 3 (atom 3 is the terminal / outlet atom).
    /// Default catchment bboxes (from DatasetBuilder):
    ///   atom 1: (0.50, 0.00, 0.90, 0.40)
    ///   atom 2: (1.00, 0.00, 1.40, 0.40)
    ///   atom 3: (1.50, 0.00, 1.90, 0.40)
    fn three_atom_session() -> (tempfile::TempDir, DatasetSession) {
        let (dir, root) = DatasetBuilder::new(3).build();
        let session = DatasetSession::open(&root).expect("session should open");
        (dir, session)
    }

    /// Coordinate inside atom 3's bbox — (1.70, 0.20).
    fn coord_in_atom3() -> GeoCoord {
        GeoCoord::new(1.70, 0.20)
    }

    /// Coordinate far outside any catchment.
    fn coord_outside() -> GeoCoord {
        GeoCoord::new(999.0, 999.0)
    }

    // ── engine_single_outlet_no_rasters ──────────────────────────────────────

    #[test]
    fn engine_single_outlet_no_rasters() {
        let (_dir, session) = three_atom_session();
        let engine = Engine::builder(session).build();

        let result = engine
            .delineate(coord_in_atom3(), &DelineationOptions::default())
            .expect("delineation should succeed");

        assert!(result.area_km2().as_f64() > 0.0, "area must be positive");
        assert!(
            result.geometry().0.len() >= 1,
            "geometry must have at least one polygon"
        );
        assert_eq!(
            result.refinement(),
            &RefinementOutcome::NoRastersAvailable,
            "no rasters registered → NoRastersAvailable"
        );
        assert!(
            result.upstream_atom_ids().len() >= 1,
            "at least one atom in upstream"
        );
    }

    // ── engine_outlet_outside_catchments ─────────────────────────────────────

    #[test]
    fn engine_outlet_outside_catchments() {
        let (_dir, session) = three_atom_session();
        let engine = Engine::builder(session).build();

        let err = engine
            .delineate(coord_outside(), &DelineationOptions::default())
            .expect_err("outlet outside catchments must fail");

        assert!(
            matches!(err, EngineError::Resolution { .. }),
            "expected Resolution error, got {err:?}"
        );
    }

    // ── engine_batch_mixed_success_failure ────────────────────────────────────

    #[test]
    fn engine_batch_mixed_success_failure() {
        let (_dir, session) = three_atom_session();
        let engine = Engine::builder(session).build();

        let opts = DelineationOptions::default();
        let results =
            engine.delineate_batch(&[(coord_in_atom3(), opts.clone()), (coord_outside(), opts)]);

        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok(), "first outlet should succeed");
        assert!(results[1].is_err(), "second outlet should fail");
    }

    // ── engine_single_headwater_atom ──────────────────────────────────────────

    #[test]
    fn engine_single_headwater_atom() {
        // Atom 1 is the headwater (no upstream). Use a coordinate inside atom 1.
        let (_dir, root) = DatasetBuilder::new(3).build();
        let session = DatasetSession::open(&root).expect("session should open");
        let engine = Engine::builder(session).build();

        // Atom 1 bbox: (0.50, 0.00, 0.90, 0.40), centre at ~(0.70, 0.20)
        let coord_in_atom1 = GeoCoord::new(0.70, 0.20);
        let result = engine
            .delineate(coord_in_atom1, &DelineationOptions::default())
            .expect("headwater delineation should succeed");

        assert!(
            result.upstream_atom_ids().len() == 1,
            "headwater has exactly 1 atom"
        );
        assert!(result.geometry().0.len() >= 1, "geometry is non-empty");
        assert!(result.area_km2().as_f64() > 0.0, "area is positive");
    }

    // ── engine_batch_empty_input ──────────────────────────────────────────────

    #[test]
    fn engine_batch_empty_input() {
        let (_dir, session) = three_atom_session();
        let engine = Engine::builder(session).build();

        let results = engine.delineate_batch(&[]);
        assert!(results.is_empty(), "empty input must yield empty output");
    }

    // ── engine_refinement_disabled ────────────────────────────────────────────

    #[test]
    fn engine_refinement_disabled() {
        let (_dir, session) = three_atom_session();
        let engine = Engine::builder(session).build();

        let opts = DelineationOptions::default().with_refine(false);
        let result = engine
            .delineate(coord_in_atom3(), &opts)
            .expect("delineation should succeed");

        assert_eq!(
            result.refinement(),
            &RefinementOutcome::Disabled,
            "refinement disabled → Disabled outcome"
        );
    }

    // ── engine_geometry_wkb_accessor ─────────────────────────────────────────

    #[test]
    fn engine_geometry_wkb_accessor() {
        let (_dir, session) = three_atom_session();
        let engine = Engine::builder(session).build();

        let result = engine
            .delineate(coord_in_atom3(), &DelineationOptions::default())
            .expect("delineation should succeed");

        let wkb = result.geometry_wkb().expect("WKB encoding should succeed");
        assert!(!wkb.is_empty(), "WKB bytes must not be empty");
        assert_eq!(wkb[0], 0x01, "first byte must be 0x01 (little-endian)");
    }
}
