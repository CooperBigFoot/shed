//! Terminal refinement strategy contract and provenance types.

use geo::{BoundingRect, MultiPolygon};
use hfx_core::FlowDirEncoding;
use hfx_core::UnitId;
use object_store::path::Path as ObjectPath;

use crate::algo::coord::GeoCoord;
use crate::algo::{RasterSource, RefinementError, SnapThreshold, refine_terminal_from_source};
use crate::error::SessionError;
use crate::session::{DatasetSession, RasterKind};
use crate::telemetry::{
    Stage, StageGuard, record_bytes, record_cache_status, record_path, record_requests,
};

/// Runs terminal-only geometry refinement using typed engine context.
pub trait TerminalRefinementStrategy: Send + Sync {
    /// Refine the terminal geometry or return a visible best-effort skip.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`TerminalRefinementError::EmptyContainedTerminalGeometry`] | A D8 carve returns an empty geometry |
    /// | [`TerminalRefinementError::Algorithm`] | The underlying refinement algorithm fails |
    /// | [`TerminalRefinementError::RasterSource`] | A required raster source is absent |
    fn refine_terminal(
        &self,
        input: TerminalRefinementInput<'_>,
        pantry: &D8RefinementPantry<'_>,
    ) -> Result<TerminalRefinementDecision, TerminalRefinementError>;
}

/// Terminal-only input for a refinement strategy.
#[derive(Debug, Clone, Copy)]
pub struct TerminalRefinementInput<'a> {
    /// Terminal drainage-unit ID.
    pub terminal_unit: UnitId,
    /// Pre-merge whole-terminal geometry decoded by the M3 staged path.
    pub terminal_geometry: &'a MultiPolygon<f64>,
    /// Outlet resolved before refinement.
    pub resolved_outlet: GeoCoord,
    /// Minimum flow accumulation used by D8 snapping.
    pub snap_threshold: SnapThreshold,
}

/// D8-specific pantry for M4 terminal refinement.
///
/// This deliberately exposes only the dataset session and optional raster
/// source needed by the built-in D8 path. General auxiliary binding and custom
/// strategy authoring are deferred beyond M4.
#[derive(Clone, Copy)]
pub struct D8RefinementPantry<'a> {
    /// Dataset session that owns declared HFX artifacts.
    pub session: &'a DatasetSession,
    /// Engine-attached raster source, if available.
    pub raster_source: Option<&'a (dyn RasterSource + Send + Sync)>,
}

/// Built-in terminal refinement strategy for declared HFX blessed-D8 rasters.
#[derive(Debug, Default, Clone, Copy)]
pub struct D8RasterRefinementStrategy;

impl TerminalRefinementStrategy for D8RasterRefinementStrategy {
    fn refine_terminal(
        &self,
        input: TerminalRefinementInput<'_>,
        pantry: &D8RefinementPantry<'_>,
    ) -> Result<TerminalRefinementDecision, TerminalRefinementError> {
        let terminal_bbox =
            input
                .terminal_geometry
                .bounding_rect()
                .ok_or(TerminalRefinementError::Algorithm {
                    unit_id: input.terminal_unit.get(),
                    source: RefinementError::DegenerateTerminalPolygon,
                })?;
        let handle = pantry
            .session
            .select_d8_raster_for_bbox(terminal_bbox)
            .map_err(|source| TerminalRefinementError::D8Selection {
                unit_id: input.terminal_unit.get(),
                source,
            })?;

        let Some(raster_source) = pantry.raster_source else {
            return Ok(TerminalRefinementDecision::BestEffortSkipped {
                provenance: RefinementProvenance::BestEffortSkipped {
                    strategy: RefinementStrategyName::BestEffortD8IfPresent,
                    why: BestEffortSkipReason::NoRasterSourceProvided,
                },
            });
        };

        let flow_dir = {
            let _guard = StageGuard::enter(Stage::RasterLocalizeFlowDir);
            let flow_dir = pantry
                .session
                .localize_d8_raster_window(&handle, RasterKind::FlowDir, terminal_bbox)
                .map_err(|source| TerminalRefinementError::RasterLocalize {
                    unit_id: input.terminal_unit.get(),
                    kind: RasterKind::FlowDir,
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
            let flow_acc = pantry
                .session
                .localize_d8_raster_window(&handle, RasterKind::FlowAcc, terminal_bbox)
                .map_err(|source| TerminalRefinementError::RasterLocalize {
                    unit_id: input.terminal_unit.get(),
                    kind: RasterKind::FlowAcc,
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
            declaration_index = handle.declaration_index(),
            "localized selected D8 raster windows for refinement"
        );
        let flow_dir_uri = flow_dir.path().to_string_lossy();
        let flow_acc_uri = flow_acc.path().to_string_lossy();

        let refinement_result = {
            let _refine_guard = StageGuard::enter(Stage::TerminalRefine);
            refine_terminal_from_source(
                raster_source,
                flow_dir_uri.as_ref(),
                flow_acc_uri.as_ref(),
                input.terminal_geometry,
                input.resolved_outlet,
                input.snap_threshold,
            )
            .map_err(|source| TerminalRefinementError::Algorithm {
                unit_id: input.terminal_unit.get(),
                source,
            })?
        };

        let refined_outlet = refinement_result.snapped_coord();
        let geometry =
            ContainedTerminalPolygon::new_unchecked_from_d8_carve(refinement_result.into_polygon())
                .map_err(|_source| TerminalRefinementError::Algorithm {
                    unit_id: input.terminal_unit.get(),
                    source: RefinementError::EmptyPolygonization,
                })?;

        Ok(TerminalRefinementDecision::Applied {
            refined_outlet,
            geometry,
            provenance: RefinementProvenance::Applied {
                strategy: RefinementStrategyName::BuiltInD8,
                why: AppliedRefinementReason::D8AuxMatchedTerminalBbox {
                    declaration_index: handle.declaration_index(),
                },
            },
        })
    }
}

/// Typed handle for one selected blessed-D8 raster declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct D8RasterHandle {
    declaration_index: usize,
    flow_dir_uri: String,
    flow_acc_uri: String,
    remote_flow_dir_path: Option<ObjectPath>,
    remote_flow_acc_path: Option<ObjectPath>,
    flow_dir_encoding: FlowDirEncoding,
}

impl D8RasterHandle {
    /// Construct a D8 handle after path resolution and coverage checks pass.
    pub(crate) fn new(
        declaration_index: usize,
        flow_dir_uri: String,
        flow_acc_uri: String,
        remote_flow_dir_path: Option<ObjectPath>,
        remote_flow_acc_path: Option<ObjectPath>,
        flow_dir_encoding: FlowDirEncoding,
    ) -> Self {
        Self {
            declaration_index,
            flow_dir_uri,
            flow_acc_uri,
            remote_flow_dir_path,
            remote_flow_acc_path,
            flow_dir_encoding,
        }
    }

    /// Return the zero-based declaration index from manifest order.
    pub fn declaration_index(&self) -> usize {
        self.declaration_index
    }

    /// Return the resolved flow-direction raster URI.
    pub fn flow_dir_uri(&self) -> &str {
        &self.flow_dir_uri
    }

    /// Return the resolved flow-accumulation raster URI.
    pub fn flow_acc_uri(&self) -> &str {
        &self.flow_acc_uri
    }

    /// Return the selected remote flow-direction object-store path, if remote.
    pub fn remote_flow_dir_path(&self) -> Option<&ObjectPath> {
        self.remote_flow_dir_path.as_ref()
    }

    /// Return the selected remote flow-accumulation object-store path, if remote.
    pub fn remote_flow_acc_path(&self) -> Option<&ObjectPath> {
        self.remote_flow_acc_path.as_ref()
    }

    /// Return the declared flow-direction encoding.
    pub fn flow_dir_encoding(&self) -> FlowDirEncoding {
        self.flow_dir_encoding
    }
}

/// Refined terminal polygon produced by the built-in D8 carve.
///
/// The wrapper documents the shrink contract at the type boundary. It does not
/// enforce strict vector containment because polygonized raster output can
/// legitimately extend a fraction of a cell past the source terminal boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct ContainedTerminalPolygon {
    polygon: MultiPolygon<f64>,
}

impl ContainedTerminalPolygon {
    /// Wrap a D8-carved terminal polygon after proving it is non-empty.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`TerminalRefinementError::EmptyContainedTerminalGeometry`] | The carve produced no polygons |
    pub fn new_unchecked_from_d8_carve(
        polygon: MultiPolygon<f64>,
    ) -> Result<Self, TerminalRefinementError> {
        if polygon.0.is_empty() {
            return Err(TerminalRefinementError::EmptyContainedTerminalGeometry);
        }
        Ok(Self { polygon })
    }

    /// Return the wrapped polygon.
    pub fn polygon(&self) -> &MultiPolygon<f64> {
        &self.polygon
    }

    /// Consume the wrapper and return the wrapped polygon.
    pub fn into_polygon(self) -> MultiPolygon<f64> {
        self.polygon
    }
}

/// Strategy-level decision before engine policy is applied.
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalRefinementDecision {
    /// A strategy produced a terminal override.
    Applied {
        /// Refined outlet coordinate returned by raster snapping.
        refined_outlet: GeoCoord,
        /// Refined terminal geometry.
        geometry: ContainedTerminalPolygon,
        /// Provenance explaining why refinement ran.
        provenance: RefinementProvenance,
    },
    /// Best-effort refinement was visibly skipped.
    BestEffortSkipped {
        /// Provenance explaining why refinement was skipped.
        provenance: RefinementProvenance,
    },
}

/// Provenance for the terminal refinement stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefinementProvenance {
    /// Refinement was disabled by caller policy.
    Disabled,
    /// A strategy produced a terminal override.
    Applied {
        /// Strategy that produced the override.
        strategy: RefinementStrategyName,
        /// Reason the strategy applied.
        why: AppliedRefinementReason,
    },
    /// Best-effort refinement was visibly skipped.
    BestEffortSkipped {
        /// Strategy that was considered.
        strategy: RefinementStrategyName,
        /// Reason the strategy skipped.
        why: BestEffortSkipReason,
    },
}

/// Stable names for terminal refinement strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefinementStrategyName {
    /// Built-in D8 strategy.
    BuiltInD8,
    /// Convenience D8-if-present strategy.
    BestEffortD8IfPresent,
}

/// Reasons an applied refinement ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppliedRefinementReason {
    /// D8 declaration selected for the terminal bbox.
    D8AuxMatchedTerminalBbox {
        /// Zero-based declaration index in manifest order.
        declaration_index: usize,
    },
}

/// Reasons best-effort refinement skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BestEffortSkipReason {
    /// The dataset declares no blessed D8 auxiliary raster pair.
    NoD8AuxDeclared,
    /// The engine has no raster source attached.
    NoRasterSourceProvided,
}

/// Errors from the terminal-refinement strategy seam.
#[derive(Debug, thiserror::Error)]
pub enum TerminalRefinementError {
    /// Fired when the D8 carve returns no terminal polygons.
    #[error("D8 terminal refinement produced an empty terminal geometry")]
    EmptyContainedTerminalGeometry,

    /// Fired when a required raster source is absent.
    #[error("terminal refinement strategy {strategy:?} requires a raster source")]
    RasterSource {
        /// Strategy that required raster access.
        strategy: RefinementStrategyName,
    },

    /// Fired when selecting the covering D8 declaration fails.
    #[error("failed to select D8 raster for terminal refinement (unit {unit_id}): {source}")]
    D8Selection {
        /// Terminal drainage-unit ID.
        unit_id: i64,
        /// Underlying session error.
        source: SessionError,
    },

    /// Fired when a selected D8 raster window cannot be localized.
    #[error(
        "failed to localize selected D8 {kind:?} raster for terminal refinement (unit {unit_id}): {source}"
    )]
    RasterLocalize {
        /// Terminal drainage-unit ID.
        unit_id: i64,
        /// Raster kind being localized.
        kind: RasterKind,
        /// Underlying session error.
        source: SessionError,
    },

    /// Fired when the underlying D8 refinement algorithm fails.
    #[error("terminal refinement algorithm failed for unit {unit_id}: {source}")]
    Algorithm {
        /// Terminal drainage-unit ID.
        unit_id: i64,
        /// Underlying algorithm error.
        source: RefinementError,
    },
}
