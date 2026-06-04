//! Engine composition layer — wires outlet resolution, upstream traversal,
//! terminal refinement, and watershed assembly into a single `delineate()` call.

use std::collections::HashMap;

use geo::{BoundingRect, MultiPolygon};
use hfx_core::UnitId;
use rayon::prelude::*;
use tracing::instrument;

use crate::algo::coord::GeoCoord;
use crate::algo::{
    AreaKm2, CleanEpsilon, DEFAULT_CLEANING_EPSILON, GeometryRepair, HoleFillMode, RasterSource,
    RefinementError, SnapThreshold, TraversalError, WkbDecodeError, WkbEncodeError,
    collect_upstream, encode_wkb_multi_polygon, refine_terminal_from_source,
};
use crate::assembly::{AssemblyOptions, assemble_from_geometries};
use crate::error::SessionError;
use crate::reader::catchment_store::CatchmentGeometryQueryError;
use crate::refinement::{
    AppliedRefinementReason, ContainedTerminalPolygon, RefinementProvenance, RefinementStrategyName,
};
use crate::resolver::{
    OutletResolutionError, ResolutionMethod, ResolverConfig,
    resolve_outlet_at_level as resolve_outlet_in_resolver_at_level,
};
use crate::session::{DatasetSession, RasterKind};
use crate::staged::{
    DissolvedWatershed, LevelResolvedOutlet, LevelSelection, PreMergeDrainageUnit,
    PreMergeDrainageUnits, RefinementMode, SameLevelUpstreamUnits, SelectedLevel,
    TerminalRefinement,
};
use crate::telemetry::{
    Stage, StageGuard, record_bytes, record_cache_status, record_path, record_requests,
};

// ── RefinementOutcome ─────────────────────────────────────────────────────────

/// Records what happened during the optional terminal-refinement step.
#[derive(Debug, Clone, PartialEq)]
pub enum RefinementOutcome {
    /// Refinement ran successfully and the outlet was snapped.
    Applied {
        /// The refined outlet coordinate returned by the raster snap.
        refined_outlet: GeoCoord,
        /// Provenance explaining why refinement ran.
        provenance: RefinementProvenance,
    },
    /// Best-effort refinement was visibly skipped.
    BestEffortSkipped {
        /// Provenance explaining why refinement was skipped.
        provenance: RefinementProvenance,
    },
    /// Refinement was disabled by the caller.
    Disabled,
}

// ── DelineationResult ─────────────────────────────────────────────────────────

/// The output of a successful [`Engine::delineate`] call.
#[derive(Debug, Clone)]
pub struct DelineationResult {
    terminal_unit_id: UnitId,
    input_outlet: GeoCoord,
    resolved_outlet: GeoCoord,
    resolution_method: ResolutionMethod,
    upstream_unit_ids: Vec<UnitId>,
    refinement: RefinementOutcome,
    geometry: MultiPolygon<f64>,
    area_km2: AreaKm2,
}

impl DelineationResult {
    /// Return the terminal unit ID that the outlet resolved to.
    pub fn terminal_unit_id(&self) -> UnitId {
        self.terminal_unit_id
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

    /// Return the slice of all upstream unit IDs (including the terminal).
    pub fn upstream_unit_ids(&self) -> &[UnitId] {
        &self.upstream_unit_ids
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

// ── DelineationAreaOnlyResult ────────────────────────────────────────────────

/// The scalar output of a successful [`Engine::delineate_area_only`] call.
#[derive(Debug, Clone)]
pub struct DelineationAreaOnlyResult {
    terminal_unit_id: UnitId,
    input_outlet: GeoCoord,
    resolved_outlet: GeoCoord,
    resolution_method: ResolutionMethod,
    upstream_unit_ids: Vec<UnitId>,
    refinement: RefinementOutcome,
    area_km2: AreaKm2,
}

impl DelineationAreaOnlyResult {
    /// Consume a full delineation result while dropping the watershed geometry.
    pub fn from_delineation_result(result: DelineationResult) -> Self {
        Self {
            terminal_unit_id: result.terminal_unit_id,
            input_outlet: result.input_outlet,
            resolved_outlet: result.resolved_outlet,
            resolution_method: result.resolution_method,
            upstream_unit_ids: result.upstream_unit_ids,
            refinement: result.refinement,
            area_km2: result.area_km2,
        }
    }

    /// Return the terminal unit ID that the outlet resolved to.
    pub fn terminal_unit_id(&self) -> UnitId {
        self.terminal_unit_id
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

    /// Return the slice of all upstream unit IDs (including the terminal).
    pub fn upstream_unit_ids(&self) -> &[UnitId] {
        &self.upstream_unit_ids
    }

    /// Return a reference to the refinement outcome.
    pub fn refinement(&self) -> &RefinementOutcome {
        &self.refinement
    }

    /// Return the geodesic watershed area in km².
    pub fn area_km2(&self) -> AreaKm2 {
        self.area_km2
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
    refinement_mode: RefinementMode,
}

impl Default for DelineationOptions {
    fn default() -> Self {
        Self {
            resolver_config: ResolverConfig::new(),
            snap_threshold: SnapThreshold::DEFAULT,
            hole_fill_mode: HoleFillMode::RemoveAll,
            clean_epsilon: DEFAULT_CLEANING_EPSILON,
            refinement_mode: RefinementMode::default(),
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

    /// Override the terminal-refinement mode.
    pub fn with_refinement_mode(mut self, mode: RefinementMode) -> Self {
        self.refinement_mode = mode;
        self
    }

    /// Return the configured terminal-refinement mode.
    pub fn refinement_mode(&self) -> RefinementMode {
        self.refinement_mode
    }

    /// Return the configured outlet resolver settings.
    pub fn resolver_config(&self) -> &ResolverConfig {
        &self.resolver_config
    }

    /// Enable or disable the terminal-refinement step.
    #[deprecated(
        since = "0.1.123",
        note = "use with_refinement_mode(RefinementMode::BestEffort) or RefinementMode::Disabled"
    )]
    pub fn with_refine(mut self, refine: bool) -> Self {
        self.refinement_mode = RefinementMode::from(refine);
        self
    }
}

// ── EngineError ───────────────────────────────────────────────────────────────

/// Errors that can occur during [`Engine::delineate`].
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Fired when the outlet coordinate cannot be resolved to an HFX unit.
    #[error("outlet resolution failed for {outlet}: {source}")]
    Resolution {
        /// The outlet coordinate that was supplied.
        outlet: GeoCoord,
        /// Underlying resolution error.
        source: OutletResolutionError,
    },

    /// Fired when the upstream graph traversal fails for the resolved unit.
    #[error("upstream traversal failed for unit {unit_id}: {source}")]
    Traversal {
        /// The raw unit ID that was traversed from.
        unit_id: i64,
        /// Underlying traversal error.
        source: TraversalError,
    },

    /// Fired when the terminal catchment row cannot be fetched for refinement.
    #[error("failed to fetch terminal catchment for refinement (unit {unit_id}): {source}")]
    TerminalCatchmentFetch {
        /// The raw unit ID whose catchment fetch failed.
        unit_id: i64,
        /// Underlying session error.
        source: SessionError,
    },

    /// Fired when the stored terminal catchment geometry fails WKB decode.
    #[error("failed to decode terminal catchment geometry (unit {unit_id}): {source}")]
    TerminalCatchmentDecode {
        /// The raw unit ID whose geometry could not be decoded.
        unit_id: i64,
        /// Underlying WKB decode error.
        source: WkbDecodeError,
    },

    /// Fired when a raster artifact cannot be materialized as a local path.
    #[error("failed to localize raster for refinement (unit {unit_id}): {source}")]
    RasterLocalize {
        /// The raw unit ID for which raster localization was attempted.
        unit_id: i64,
        /// Underlying session error.
        source: SessionError,
    },

    /// Fired when the raster-based terminal refinement step fails.
    #[error("terminal refinement failed for unit {unit_id}: {source}")]
    Refinement {
        /// The raw unit ID for which refinement was attempted.
        unit_id: i64,
        /// Underlying refinement error.
        source: RefinementError,
    },

    /// Fired when final watershed assembly fails.
    #[error("watershed assembly failed for unit {unit_id}: {message}")]
    Assembly {
        /// The raw unit ID of the terminal unit being assembled.
        unit_id: i64,
        /// Human-readable description of the assembly failure.
        message: String,
        /// The original assembly error, preserved for error-chain inspection.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Fired when a loaded dataset has no stored session level index entries.
    ///
    /// This is unreachable after M2 graph/catchment validation for a valid HFX
    /// dataset. Treat it as an integrity error if tests can trigger it.
    #[error(
        "session level index is empty for loaded dataset {fabric:?} with {unit_count} manifest units"
    )]
    SessionLevelIndexEmpty {
        /// Manifest fabric name for the loaded dataset.
        fabric: String,
        /// Manifest unit count for the loaded dataset.
        unit_count: u64,
    },

    /// Fired when same-level traversal returns an unit outside the selected level.
    ///
    /// M2 graph/catchment validation requires graph edges to stay within one
    /// level, so this indicates an invalid loaded session or a stale level
    /// index.
    #[error("unit {unit_id} is not at selected level {selected_level:?}; found {actual_level:?}")]
    SameLevelInvariant {
        /// The raw unit ID whose stored level does not match the selected level.
        unit_id: i64,
        /// The selected level required by this staged run.
        selected_level: hfx_core::Level,
        /// The stored level for the unit, if the level index contains it.
        actual_level: Option<hfx_core::Level>,
    },

    /// Fired when pre-merge catchment metadata or geometry cannot be fetched.
    #[error("failed to fetch pre-merge catchment records for {unit_count} units: {source}")]
    PreMergeCatchmentFetch {
        /// Number of unit IDs requested for pre-merge materialization.
        unit_count: usize,
        /// Underlying session error.
        source: SessionError,
    },

    /// Fired when a pre-merge catchment geometry fails WKB decode.
    #[error("failed to decode pre-merge catchment geometry (unit {unit_id}): {source}")]
    PreMergeCatchmentDecode {
        /// The raw unit ID whose geometry could not be decoded.
        unit_id: i64,
        /// Underlying WKB decode error.
        source: WkbDecodeError,
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

    /// Return object-store request counters when network benchmarking is enabled.
    pub fn http_stats(&self) -> Option<crate::source_telemetry::HttpStatsSnapshot> {
        self.session.http_stats()
    }

    /// Select the HFX drainage-unit level for a staged delineation run.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::SessionLevelIndexEmpty`] | The loaded session has no stored level-index entries |
    pub fn select_level(&self, choice: LevelSelection) -> Result<SelectedLevel, EngineError> {
        match choice {
            LevelSelection::Finest => self
                .session
                .max_level()
                .map(SelectedLevel::from_proven_level)
                .ok_or_else(|| EngineError::SessionLevelIndexEmpty {
                    fabric: self.session.manifest().fabric_name().to_string(),
                    unit_count: self.session.manifest().unit_count().get(),
                }),
        }
    }

    /// Resolve an outlet within a selected HFX drainage-unit level.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::Resolution`] | Outlet cannot be resolved to an unit |
    pub fn resolve_outlet_at_level(
        &self,
        outlet: GeoCoord,
        selected_level: SelectedLevel,
        config: &ResolverConfig,
    ) -> Result<LevelResolvedOutlet, EngineError> {
        resolve_outlet_in_resolver_at_level(&self.session, outlet, selected_level, config)
            .map(|resolved| LevelResolvedOutlet::new(selected_level, resolved))
            .map_err(|source| EngineError::Resolution { outlet, source })
    }

    /// Traverse the same-level upstream graph for a level-resolved outlet.
    ///
    /// This stage is a thin topology wrapper around [`collect_upstream`]. It
    /// adds the terminal and selected-level tags, and validates that every
    /// traversed unit belongs to the selected level already proven by the
    /// session level index.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::Traversal`] | Upstream graph traversal fails |
    /// | [`EngineError::SameLevelInvariant`] | A traversed unit is absent from or outside the selected level |
    pub fn traverse_upstream_at_level(
        &self,
        outlet: &LevelResolvedOutlet,
    ) -> Result<SameLevelUpstreamUnits, EngineError> {
        let terminal = outlet.resolved().unit_id;
        let selected_level = outlet.selected_level();
        let upstream = collect_upstream(terminal, self.session.graph()).map_err(|source| {
            EngineError::Traversal {
                unit_id: terminal.get(),
                source,
            }
        })?;

        for unit_id in upstream.iter().copied() {
            let actual_level = self.session.level_of(unit_id);
            if actual_level != Some(selected_level.level()) {
                return Err(EngineError::SameLevelInvariant {
                    unit_id: unit_id.get(),
                    selected_level: selected_level.level(),
                    actual_level,
                });
            }
        }

        Ok(SameLevelUpstreamUnits::new(
            terminal,
            selected_level,
            upstream,
        ))
    }

    /// Materialize pristine pre-merge drainage-unit records.
    ///
    /// Metadata is fetched via `query_by_ids`; decoded geometries are fetched
    /// separately via `query_geometries_by_ids`, preserving the instrumented
    /// geometry-decode path used by assembly and refinement.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::PreMergeCatchmentFetch`] | Metadata or geometry rows cannot be read, or a requested row is missing |
    /// | [`EngineError::PreMergeCatchmentDecode`] | A requested geometry fails WKB decode |
    /// | [`EngineError::SameLevelInvariant`] | A materialized record is outside the selected level |
    pub fn produce_pre_merge_units(
        &self,
        upstream: &SameLevelUpstreamUnits,
    ) -> Result<PreMergeDrainageUnits, EngineError> {
        let ids = upstream.upstream().unit_ids();
        let metadata_by_id = self
            .session
            .catchments()
            .query_by_ids(ids)
            .map_err(|source| EngineError::PreMergeCatchmentFetch {
                unit_count: ids.len(),
                source,
            })?
            .into_iter()
            .map(|unit| (unit.id(), unit))
            .collect::<HashMap<_, _>>();

        let mut geometry_by_id = self
            .session
            .catchments()
            .query_geometries_by_ids(ids)
            .map_err(|source| match source {
                CatchmentGeometryQueryError::Read { source } => {
                    EngineError::PreMergeCatchmentFetch {
                        unit_count: ids.len(),
                        source,
                    }
                }
                CatchmentGeometryQueryError::Decode { unit_id, source } => {
                    EngineError::PreMergeCatchmentDecode {
                        unit_id: unit_id.get(),
                        source,
                    }
                }
            })?
            .into_iter()
            .map(|row| row.into_parts())
            .collect::<HashMap<_, _>>();

        let mut units = Vec::with_capacity(ids.len());
        for &id in ids {
            let metadata =
                metadata_by_id
                    .get(&id)
                    .ok_or_else(|| EngineError::PreMergeCatchmentFetch {
                        unit_count: ids.len(),
                        source: SessionError::integrity(format!(
                            "pre-merge unit {} not in catchment store metadata",
                            id.get()
                        )),
                    })?;
            if metadata.level() != upstream.selected_level().level() {
                return Err(EngineError::SameLevelInvariant {
                    unit_id: id.get(),
                    selected_level: upstream.selected_level().level(),
                    actual_level: Some(metadata.level()),
                });
            }
            let geometry =
                geometry_by_id
                    .remove(&id)
                    .ok_or_else(|| EngineError::PreMergeCatchmentFetch {
                        unit_count: ids.len(),
                        source: SessionError::integrity(format!(
                            "pre-merge unit {} not in catchment store geometry rows",
                            id.get()
                        )),
                    })?;

            units.push(PreMergeDrainageUnit::new(
                id,
                metadata.level(),
                metadata.area(),
                metadata.upstream_area(),
                metadata.outlet(),
                geometry,
            ));
        }

        Ok(PreMergeDrainageUnits::new(
            upstream.terminal(),
            upstream.selected_level(),
            units,
        ))
    }

    /// Attempt terminal refinement using the pre-merge terminal geometry.
    ///
    /// This stage preserves the current best-effort raster refinement behavior
    /// while avoiding a second terminal catchment geometry query after
    /// [`Engine::produce_pre_merge_units`] has already decoded it.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::PreMergeCatchmentFetch`] | The pre-merge collection does not contain its terminal record |
    /// | [`EngineError::RasterLocalize`] | Remote rasters cannot be materialized locally |
    /// | [`EngineError::Refinement`] | Raster snap fails or the terminal geometry is degenerate |
    pub fn refine_terminal_placeholder(
        &self,
        resolved: &LevelResolvedOutlet,
        units: &PreMergeDrainageUnits,
        options: &DelineationOptions,
    ) -> Result<TerminalRefinement, EngineError> {
        let terminal = units.terminal();
        if options.refinement_mode == RefinementMode::Disabled {
            return Ok(TerminalRefinement::Disabled);
        }
        if self.session.raster_paths().is_none() {
            return Ok(TerminalRefinement::best_effort_no_d8_aux_declared());
        }
        let raster_source = match self.raster_source.as_deref() {
            Some(s) => s,
            None => return Ok(TerminalRefinement::best_effort_no_raster_source_provided()),
        };

        let terminal_polygon = units
            .terminal_unit()
            .filter(|unit| unit.id() == terminal)
            .ok_or_else(|| EngineError::PreMergeCatchmentFetch {
                unit_count: units.units().len(),
                source: SessionError::integrity(format!(
                    "terminal unit {} not in pre-merge units",
                    terminal.get()
                )),
            })?
            .geometry();
        let terminal_bbox =
            terminal_polygon
                .bounding_rect()
                .ok_or_else(|| EngineError::Refinement {
                    unit_id: terminal.get(),
                    source: RefinementError::DegenerateTerminalPolygon,
                })?;

        let flow_dir = {
            let _guard = StageGuard::enter(Stage::RasterLocalizeFlowDir);
            let flow_dir = self
                .session
                .localize_raster_window(RasterKind::FlowDir, terminal_bbox)
                .map_err(|source| EngineError::RasterLocalize {
                    unit_id: terminal.get(),
                    source,
                })?;
            let bytes = flow_dir.header_bytes() + flow_dir.tile_bytes();
            record_bytes(bytes);
            record_requests(flow_dir.tile_count() as u64);
            record_cache_status(if bytes == 0 { "no_fetch" } else { "fetched" });
            record_path(flow_dir.path());
            flow_dir
        };
        let flow_acc = {
            let _guard = StageGuard::enter(Stage::RasterLocalizeFlowAcc);
            let flow_acc = self
                .session
                .localize_raster_window(RasterKind::FlowAcc, terminal_bbox)
                .map_err(|source| EngineError::RasterLocalize {
                    unit_id: terminal.get(),
                    source,
                })?;
            let bytes = flow_acc.header_bytes() + flow_acc.tile_bytes();
            record_bytes(bytes);
            record_requests(flow_acc.tile_count() as u64);
            record_cache_status(if bytes == 0 { "no_fetch" } else { "fetched" });
            record_path(flow_acc.path());
            flow_acc
        };
        tracing::debug!(
            flow_dir_cog_header_bytes = flow_dir.header_bytes(),
            flow_dir_cog_tile_bytes = flow_dir.tile_bytes(),
            flow_dir_cog_tile_count = flow_dir.tile_count(),
            flow_dir_window_pixels = flow_dir.window_pixels(),
            flow_acc_cog_header_bytes = flow_acc.header_bytes(),
            flow_acc_cog_tile_bytes = flow_acc.tile_bytes(),
            flow_acc_cog_tile_count = flow_acc.tile_count(),
            flow_acc_window_pixels = flow_acc.window_pixels(),
            "localized raster windows for refinement"
        );
        let flow_dir_uri = flow_dir.path().to_string_lossy();
        let flow_acc_uri = flow_acc.path().to_string_lossy();

        let refinement_result = {
            let _refine_guard = StageGuard::enter(Stage::TerminalRefine);
            refine_terminal_from_source(
                raster_source,
                flow_dir_uri.as_ref(),
                flow_acc_uri.as_ref(),
                terminal_polygon,
                resolved.resolved().resolved_coord,
                options.snap_threshold,
            )
            .map_err(|source| EngineError::Refinement {
                unit_id: terminal.get(),
                source,
            })?
        };

        let refined_outlet = refinement_result.snapped_coord();
        let geometry =
            ContainedTerminalPolygon::new_unchecked_from_d8_carve(refinement_result.into_polygon())
                .map_err(|_source| EngineError::Refinement {
                    unit_id: terminal.get(),
                    source: RefinementError::EmptyPolygonization,
                })?;

        Ok(TerminalRefinement::Applied {
            refined_outlet,
            geometry,
            provenance: RefinementProvenance::Applied {
                strategy: RefinementStrategyName::BestEffortD8IfPresent,
                why: AppliedRefinementReason::D8AuxMatchedTerminalBbox {
                    declaration_index: 0,
                },
            },
        })
    }

    /// Dissolve pre-merge drainage-unit geometries into the final watershed.
    ///
    /// When terminal refinement supplies an applied override, the whole
    /// terminal polygon is excluded and replaced by the refined terminal
    /// geometry before calling the geometry-only assembly path.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::Assembly`] | Watershed geometry assembly fails |
    pub fn dissolve_watershed(
        &self,
        units: &PreMergeDrainageUnits,
        refinement: &TerminalRefinement,
        options: &DelineationOptions,
    ) -> Result<DissolvedWatershed, EngineError> {
        let terminal = units.terminal();
        let refined_terminal_geometry = match refinement {
            TerminalRefinement::Applied { geometry, .. } => Some(geometry.polygon()),
            TerminalRefinement::Disabled | TerminalRefinement::BestEffortSkipped { .. } => None,
        };

        let mut geometries_by_id = std::collections::BTreeMap::new();
        for unit in units.units() {
            if unit.id() == terminal && refined_terminal_geometry.is_some() {
                continue;
            }
            geometries_by_id.insert(unit.id(), unit.geometry().clone());
        }
        if let Some(geometry) = refined_terminal_geometry {
            geometries_by_id.insert(terminal, geometry.clone());
        }

        let mut geometries = Vec::with_capacity(geometries_by_id.len());
        for (unit_id, geometry) in geometries_by_id {
            if unit_id == terminal && refined_terminal_geometry.is_some() {
                if geometry.0.is_empty() {
                    let error =
                        crate::assembly::AssemblyError::EmptyRefinedTerminalGeometry { unit_id };
                    return Err(EngineError::Assembly {
                        unit_id: terminal.get(),
                        message: error.to_string(),
                        source: Box::new(error),
                    });
                }
            } else if geometry.0.is_empty() {
                let error = crate::assembly::AssemblyError::EmptyCatchmentGeometry { unit_id };
                return Err(EngineError::Assembly {
                    unit_id: terminal.get(),
                    message: error.to_string(),
                    source: Box::new(error),
                });
            }
            geometries.push(geometry);
        }

        let assembly_options = self.build_assembly_options(options);
        let result = assemble_from_geometries(geometries, assembly_options).map_err(|e| {
            EngineError::Assembly {
                unit_id: terminal.get(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;
        let (geometry, area_km2) = result.into_parts();
        Ok(DissolvedWatershed::new(geometry, area_km2))
    }

    /// Compose the public delineation result from completed staged outputs.
    pub fn compose_result(
        &self,
        resolved: LevelResolvedOutlet,
        upstream: SameLevelUpstreamUnits,
        refinement: TerminalRefinement,
        dissolved: DissolvedWatershed,
    ) -> DelineationResult {
        DelineationResult {
            terminal_unit_id: resolved.resolved().unit_id,
            input_outlet: resolved.resolved().input_coord,
            resolved_outlet: resolved.resolved().resolved_coord,
            resolution_method: resolved.resolved().method.clone(),
            upstream_unit_ids: upstream.upstream().unit_ids().to_vec(),
            refinement: refinement_outcome_from_terminal(&refinement),
            geometry: dissolved.geometry().clone(),
            area_km2: dissolved.area_km2(),
        }
    }

    /// Delineate the watershed upstream of `outlet`.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::Resolution`] | Outlet cannot be resolved to an unit |
    /// | [`EngineError::Traversal`] | Upstream graph traversal fails |
    /// | [`EngineError::TerminalCatchmentFetch`] | Terminal catchment row is missing (refinement only) |
    /// | [`EngineError::TerminalCatchmentDecode`] | Terminal catchment WKB is invalid (refinement only) |
    /// | [`EngineError::RasterLocalize`] | Remote rasters cannot be materialized locally (refinement only) |
    /// | [`EngineError::Refinement`] | Raster snap fails (refinement only) |
    /// | [`EngineError::Assembly`] | Watershed geometry assembly fails |
    #[instrument(skip(self, options), fields(outlet = %outlet))]
    pub fn delineate(
        &self,
        outlet: GeoCoord,
        options: &DelineationOptions,
    ) -> Result<DelineationResult, EngineError> {
        // Step 1: Select finest level and resolve outlet within that level.
        let level_resolved = {
            let _guard = StageGuard::enter(Stage::OutletResolve);
            let selected_level = self.select_level(LevelSelection::Finest)?;
            self.resolve_outlet_at_level(outlet, selected_level, &options.resolver_config)?
        };

        // Step 2: Upstream traversal
        let same_level_upstream = {
            let _guard = StageGuard::enter(Stage::UpstreamTraversal);
            self.traverse_upstream_at_level(&level_resolved)?
        };

        let pre_merge = self.produce_pre_merge_units(&same_level_upstream)?;

        // Step 3: Try refinement
        let terminal_refinement =
            self.refine_terminal_placeholder(&level_resolved, &pre_merge, options)?;

        // Step 4: Assembly
        let dissolved = {
            let _guard = StageGuard::enter(Stage::WatershedAssembly);
            self.dissolve_watershed(&pre_merge, &terminal_refinement, options)?
        };

        // Step 5: Compose result
        let result = {
            let _guard = StageGuard::enter(Stage::ResultCompose);
            self.compose_result(
                level_resolved,
                same_level_upstream,
                terminal_refinement,
                dissolved,
            )
        };
        Ok(result)
    }

    /// Delineate the watershed upstream of `outlet` and return scalar metadata only.
    ///
    /// This conservative implementation reuses [`Engine::delineate`] for the
    /// hydrologic work, then drops the assembled geometry before returning.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EngineError::Resolution`] | Outlet cannot be resolved to an unit |
    /// | [`EngineError::Traversal`] | Upstream graph traversal fails |
    /// | [`EngineError::TerminalCatchmentFetch`] | Terminal catchment row is missing (refinement only) |
    /// | [`EngineError::TerminalCatchmentDecode`] | Terminal catchment WKB is invalid (refinement only) |
    /// | [`EngineError::RasterLocalize`] | Remote rasters cannot be materialized locally (refinement only) |
    /// | [`EngineError::Refinement`] | Raster snap fails (refinement only) |
    /// | [`EngineError::Assembly`] | Watershed geometry assembly fails |
    #[instrument(skip(self, options), fields(outlet = %outlet))]
    pub fn delineate_area_only(
        &self,
        outlet: GeoCoord,
        options: &DelineationOptions,
    ) -> Result<DelineationAreaOnlyResult, EngineError> {
        self.delineate(outlet, options)
            .map(DelineationAreaOnlyResult::from_delineation_result)
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

fn refinement_outcome_from_terminal(refinement: &TerminalRefinement) -> RefinementOutcome {
    match refinement {
        TerminalRefinement::Applied {
            refined_outlet,
            provenance,
            ..
        } => RefinementOutcome::Applied {
            refined_outlet: *refined_outlet,
            provenance: provenance.clone(),
        },
        TerminalRefinement::BestEffortSkipped { provenance } => {
            RefinementOutcome::BestEffortSkipped {
                provenance: provenance.clone(),
            }
        }
        TerminalRefinement::Disabled => RefinementOutcome::Disabled,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use geo::Rect;
    use hfx_core::FlowDirEncoding;

    use super::*;
    use crate::algo::{
        AccumulationTile, FlowDirectionTile, GeoTransform, GridCoord, GridDims, RasterSourceError,
        RasterTile, Raw,
    };
    use crate::reader::catchment_store::reset_geometry_decode_counts_for_test;
    use crate::session::DatasetSession;
    use crate::testutil::{DatasetBuilder, TestCatchment};

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Build a 3-unit linear dataset and open a session.
    ///
    /// Graph: 1 -> 2 -> 3 (unit 3 is the terminal / outlet unit).
    /// Default catchment bboxes (from DatasetBuilder):
    ///   unit 1: (0.50, 0.00, 0.90, 0.40)
    ///   unit 2: (1.00, 0.00, 1.40, 0.40)
    ///   unit 3: (1.50, 0.00, 1.90, 0.40)
    fn three_unit_session() -> (tempfile::TempDir, DatasetSession) {
        let (dir, root) = DatasetBuilder::new(3).build();
        let session = DatasetSession::open_path(&root).expect("session should open");
        (dir, session)
    }

    /// Coordinate inside unit 3's bbox — (1.70, 0.20).
    fn coord_in_unit3() -> GeoCoord {
        GeoCoord::new(1.70, 0.20)
    }

    /// Coordinate far outside any catchment.
    fn coord_outside() -> GeoCoord {
        GeoCoord::new(999.0, 999.0)
    }

    fn test_raster_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    fn make_flow_tile(values: &[u8]) -> FlowDirectionTile<Raw> {
        let dims = GridDims::new(5, 5);
        let mut tile = FlowDirectionTile::new(dims, test_raster_geo(), FlowDirEncoding::Esri)
            .expect("flow direction tile should build");
        for row in 0..5 {
            for col in 0..5 {
                tile.set_raw(GridCoord::new(row, col), values[row * 5 + col]);
            }
        }
        tile
    }

    fn make_accumulation_tile(values: &[f32]) -> AccumulationTile<Raw> {
        let dims = GridDims::new(5, 5);
        let raw = RasterTile::from_vec(values.to_vec(), dims, f32::NAN, test_raster_geo())
            .expect("accumulation tile should build");
        AccumulationTile::from_raw(raw)
    }

    struct AppliedRefinementRasterSource;

    impl RasterSource for AppliedRefinementRasterSource {
        fn load_flow_direction(
            &self,
            _uri: &str,
            _bbox: &Rect<f64>,
        ) -> Result<FlowDirectionTile<Raw>, RasterSourceError> {
            #[rustfmt::skip]
            let values = [
                2, 4, 4, 4, 8,
                1, 2, 4, 8, 16,
                1, 1, 4, 16, 16,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ];
            Ok(make_flow_tile(&values))
        }

        fn load_accumulation(
            &self,
            _uri: &str,
            _bbox: &Rect<f64>,
        ) -> Result<AccumulationTile<Raw>, RasterSourceError> {
            let mut values = [1.0_f32; 25];
            values[2 * 5 + 2] = 800.0;
            Ok(make_accumulation_tile(&values))
        }
    }

    // ── engine_single_outlet_no_rasters ──────────────────────────────────────

    #[test]
    fn engine_single_outlet_no_rasters() {
        let (_dir, session) = three_unit_session();
        let engine = Engine::builder(session).build();

        let result = engine
            .delineate(coord_in_unit3(), &DelineationOptions::default())
            .expect("delineation should succeed");

        assert!(result.area_km2().as_f64() > 0.0, "area must be positive");
        assert!(
            !result.geometry().0.is_empty(),
            "geometry must have at least one polygon"
        );
        assert_eq!(
            result.refinement(),
            &RefinementOutcome::BestEffortSkipped {
                provenance: RefinementProvenance::BestEffortSkipped {
                    strategy: RefinementStrategyName::BestEffortD8IfPresent,
                    why: crate::refinement::BestEffortSkipReason::NoD8AuxDeclared,
                },
            },
            "no D8 aux registered -> visible best-effort skip"
        );
        assert!(
            !result.upstream_unit_ids().is_empty(),
            "at least one unit in upstream"
        );
    }

    // ── engine_outlet_outside_catchments ─────────────────────────────────────

    #[test]
    fn engine_outlet_outside_catchments() {
        let (_dir, session) = three_unit_session();
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
        let (_dir, session) = three_unit_session();
        let engine = Engine::builder(session).build();

        let opts = DelineationOptions::default();
        let results =
            engine.delineate_batch(&[(coord_in_unit3(), opts.clone()), (coord_outside(), opts)]);

        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok(), "first outlet should succeed");
        assert!(results[1].is_err(), "second outlet should fail");
    }

    // ── engine_single_headwater_unit ──────────────────────────────────────────

    #[test]
    fn engine_single_headwater_unit() {
        // Unit 1 is the headwater (no upstream). Use a coordinate inside unit 1.
        let (_dir, root) = DatasetBuilder::new(3).build();
        let session = DatasetSession::open_path(&root).expect("session should open");
        let engine = Engine::builder(session).build();

        // Unit 1 bbox: (0.50, 0.00, 0.90, 0.40), centre at ~(0.70, 0.20)
        let coord_in_unit1 = GeoCoord::new(0.70, 0.20);
        let result = engine
            .delineate(coord_in_unit1, &DelineationOptions::default())
            .expect("headwater delineation should succeed");

        assert!(
            result.upstream_unit_ids().len() == 1,
            "headwater has exactly 1 unit"
        );
        assert!(!result.geometry().0.is_empty(), "geometry is non-empty");
        assert!(result.area_km2().as_f64() > 0.0, "area is positive");
    }

    // ── engine_batch_empty_input ──────────────────────────────────────────────

    #[test]
    fn engine_batch_empty_input() {
        let (_dir, session) = three_unit_session();
        let engine = Engine::builder(session).build();

        let results = engine.delineate_batch(&[]);
        assert!(results.is_empty(), "empty input must yield empty output");
    }

    // ── engine_refinement_disabled ────────────────────────────────────────────

    #[test]
    fn engine_refinement_disabled() {
        let (_dir, session) = three_unit_session();
        let engine = Engine::builder(session).build();

        let opts = DelineationOptions::default().with_refinement_mode(RefinementMode::Disabled);
        let result = engine
            .delineate(coord_in_unit3(), &opts)
            .expect("delineation should succeed");

        assert_eq!(
            result.refinement(),
            &RefinementOutcome::Disabled,
            "refinement disabled → Disabled outcome"
        );
    }

    #[test]
    fn applied_refinement_decodes_terminal_geometry_once() {
        reset_geometry_decode_counts_for_test();
        let (_dir, root) = DatasetBuilder::new(2)
            .with_rasters()
            .with_custom_catchments(vec![
                TestCatchment {
                    id: 1,
                    area_km2: 1.0,
                    up_area_km2: None,
                    polygon: (-5.0, -5.0, -4.0, -4.0),
                },
                TestCatchment {
                    id: 2,
                    area_km2: 25.0,
                    up_area_km2: Some(26.0),
                    polygon: (0.0, -5.0, 5.0, 0.0),
                },
            ])
            .build();
        let session = DatasetSession::open_path(&root).expect("session should open");
        let engine = Engine::builder(session)
            .with_raster_source(AppliedRefinementRasterSource)
            .build();
        let terminal = UnitId::new(2).expect("valid unit id");

        let result = engine
            .delineate(
                GeoCoord::new(2.5, -2.5),
                &DelineationOptions::default().with_snap_threshold(SnapThreshold::new(500)),
            )
            .expect("delineation should succeed");

        assert!(matches!(
            result.refinement(),
            RefinementOutcome::Applied { .. }
        ));
        assert_eq!(
            engine
                .session
                .catchments()
                .geometry_decode_count_for_test(terminal),
            1,
            "terminal geometry should be decoded for refinement only"
        );
    }

    // ── engine_geometry_wkb_accessor ─────────────────────────────────────────

    #[test]
    fn engine_geometry_wkb_accessor() {
        let (_dir, session) = three_unit_session();
        let engine = Engine::builder(session).build();

        let result = engine
            .delineate(coord_in_unit3(), &DelineationOptions::default())
            .expect("delineation should succeed");

        let wkb = result.geometry_wkb().expect("WKB encoding should succeed");
        assert!(!wkb.is_empty(), "WKB bytes must not be empty");
        assert_eq!(wkb[0], 0x01, "first byte must be 0x01 (little-endian)");
    }
}
