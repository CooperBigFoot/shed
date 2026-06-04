//! Terminal refinement strategy contract and provenance types.

use geo::MultiPolygon;
use hfx_core::UnitId;

use crate::algo::coord::GeoCoord;
use crate::algo::{RasterSource, RefinementError, SnapThreshold};
use crate::session::DatasetSession;

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

    /// Fired when the underlying D8 refinement algorithm fails.
    #[error("terminal refinement algorithm failed for unit {unit_id}: {source}")]
    Algorithm {
        /// Terminal drainage-unit ID.
        unit_id: i64,
        /// Underlying algorithm error.
        source: RefinementError,
    },
}
