//! Typed contract for the M3 finest-level staged delineation skeleton.
//!
//! This module names the intermediate values that the staged engine path will
//! pass between phases. Step 1 intentionally defines the vocabulary and the
//! independently callable method contract only; later M3 steps add the
//! `Engine` method bodies.
//!
//! ```rust,ignore
//! pub fn select_level(&self, choice: LevelSelection) -> Result<SelectedLevel, EngineError>;
//!
//! pub fn resolve_outlet_at_level(
//!     &self,
//!     outlet: GeoCoord,
//!     level: SelectedLevel,
//!     config: &ResolverConfig,
//! ) -> Result<LevelResolvedOutlet, EngineError>;
//!
//! pub fn traverse_upstream_at_level(
//!     &self,
//!     outlet: &LevelResolvedOutlet,
//! ) -> Result<SameLevelUpstreamUnits, EngineError>;
//!
//! pub fn produce_pre_merge_units(
//!     &self,
//!     upstream: &SameLevelUpstreamUnits,
//! ) -> Result<PreMergeDrainageUnits, EngineError>;
//!
//! pub fn refine_terminal_placeholder(
//!     &self,
//!     resolved: &LevelResolvedOutlet,
//!     units: &PreMergeDrainageUnits,
//!     options: &DelineationOptions,
//! ) -> Result<TerminalRefinement, EngineError>;
//!
//! pub fn dissolve_watershed(
//!     &self,
//!     units: &PreMergeDrainageUnits,
//!     refinement: &TerminalRefinement,
//!     options: &DelineationOptions,
//! ) -> Result<DissolvedWatershed, EngineError>;
//!
//! pub fn compose_result(
//!     &self,
//!     resolved: LevelResolvedOutlet,
//!     upstream: SameLevelUpstreamUnits,
//!     units: &PreMergeDrainageUnits,
//!     refinement: TerminalRefinement,
//!     dissolved: DissolvedWatershed,
//! ) -> DelineationResult;
//! ```
//!
//! Stage order:
//!
//! ```mermaid
//! flowchart LR
//!     select[select level]
//!     resolve[resolve outlet within level]
//!     traverse[traverse upstream same-level graph]
//!     records[produce pre-merge drainage-unit records]
//!     refine[terminal refinement strategy seam]
//!     dissolve[dissolve/assemble]
//!     compose[compose result]
//!
//!     select --> resolve --> traverse --> records --> refine --> dissolve --> compose
//! ```

use geo::MultiPolygon;
use hfx_core::{Level, OutletCoord, UnitId};

use crate::algo::coord::GeoCoord;
use crate::algo::{AreaKm2, UpstreamUnits};
use crate::refinement::{
    BestEffortSkipReason, ContainedTerminalPolygon, RefinementProvenance, RefinementStrategyName,
};
use crate::resolver::ResolvedOutlet;

/// Selects the HFX drainage-unit level used for the staged delineation run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelSelection {
    /// Use the finest level present in the loaded dataset.
    Finest,
}

/// Dataset-proven selected drainage-unit level.
///
/// The wrapped [`Level`] is private so downstream stages cannot be called with
/// an arbitrary raw level. Step 2 adds construction through
/// `Engine::select_level` after consulting `DatasetSession`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedLevel {
    level: Level,
}

impl SelectedLevel {
    /// Construct a selected level after the dataset session proves it exists.
    pub(crate) fn from_proven_level(level: Level) -> Self {
        Self { level }
    }

    /// Construct a selected level for focused integration tests.
    #[cfg(feature = "test-fixtures")]
    pub fn from_proven_level_for_test(level: Level) -> Self {
        Self::from_proven_level(level)
    }

    /// Return the selected HFX level.
    pub fn level(self) -> Level {
        self.level
    }
}

/// Controls whether terminal refinement is attempted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RefinementMode {
    /// Try raster refinement when the dataset and engine provide raster inputs.
    #[default]
    BestEffort,
    /// Require declared D8 raster refinement and fail if it cannot be applied.
    RequireD8,
    /// Skip terminal refinement and dissolve whole drainage-unit polygons.
    Disabled,
}

impl From<bool> for RefinementMode {
    fn from(refine: bool) -> Self {
        if refine {
            Self::BestEffort
        } else {
            Self::Disabled
        }
    }
}

/// Outlet resolution result constrained to the selected level.
#[derive(Debug, Clone, PartialEq)]
pub struct LevelResolvedOutlet {
    selected_level: SelectedLevel,
    resolved: ResolvedOutlet,
}

impl LevelResolvedOutlet {
    /// Construct a level-resolved outlet after the resolver stage has constrained it.
    pub(crate) fn new(selected_level: SelectedLevel, resolved: ResolvedOutlet) -> Self {
        Self {
            selected_level,
            resolved,
        }
    }

    /// Return the selected level used during outlet resolution.
    pub fn selected_level(&self) -> SelectedLevel {
        self.selected_level
    }

    /// Return the resolved outlet payload.
    pub fn resolved(&self) -> &ResolvedOutlet {
        &self.resolved
    }
}

/// Same-level upstream traversal result for a level-resolved outlet.
#[derive(Debug, Clone, PartialEq)]
pub struct SameLevelUpstreamUnits {
    terminal: UnitId,
    selected_level: SelectedLevel,
    upstream: UpstreamUnits,
}

impl SameLevelUpstreamUnits {
    /// Construct same-level upstream units after traversal validates the level invariant.
    pub(crate) fn new(
        terminal: UnitId,
        selected_level: SelectedLevel,
        upstream: UpstreamUnits,
    ) -> Self {
        Self {
            terminal,
            selected_level,
            upstream,
        }
    }

    /// Return the terminal unit at the selected level.
    pub fn terminal(&self) -> UnitId {
        self.terminal
    }

    /// Return the selected level shared by every upstream unit.
    pub fn selected_level(&self) -> SelectedLevel {
        self.selected_level
    }

    /// Return the inclusive upstream unit set, terminal first.
    pub fn upstream(&self) -> &UpstreamUnits {
        &self.upstream
    }
}

/// Pristine drainage-unit record before terminal carving or dissolve.
///
/// This record intentionally exposes source drainage-unit data, not final
/// watershed output. Summing [`area`](Self::area) across pre-merge records does
/// not define final `area_km2`, and unioning these geometries does not define
/// final refined geometry. The final geometry and area are produced only by the
/// downstream dissolve stage.
#[derive(Debug, Clone, PartialEq)]
pub struct PreMergeDrainageUnit {
    id: UnitId,
    level: Level,
    area: hfx_core::AreaKm2,
    up_area: Option<hfx_core::AreaKm2>,
    outlet: OutletCoord,
    geometry: MultiPolygon<f64>,
}

impl PreMergeDrainageUnit {
    /// Construct a pristine pre-merge drainage-unit record from source fields.
    pub(crate) fn new(
        id: UnitId,
        level: Level,
        area: hfx_core::AreaKm2,
        up_area: Option<hfx_core::AreaKm2>,
        outlet: OutletCoord,
        geometry: MultiPolygon<f64>,
    ) -> Self {
        Self {
            id,
            level,
            area,
            up_area,
            outlet,
            geometry,
        }
    }

    /// Construct a pre-merge record for focused integration tests.
    #[cfg(feature = "test-fixtures")]
    pub fn new_for_test(
        id: UnitId,
        level: Level,
        area: hfx_core::AreaKm2,
        up_area: Option<hfx_core::AreaKm2>,
        outlet: OutletCoord,
        geometry: MultiPolygon<f64>,
    ) -> Self {
        Self::new(id, level, area, up_area, outlet, geometry)
    }

    /// Return the drainage unit ID.
    pub fn id(&self) -> UnitId {
        self.id
    }

    /// Return the HFX level of this drainage unit.
    pub fn level(&self) -> Level {
        self.level
    }

    /// Return the local drainage area from `catchments.parquet`.
    pub fn area(&self) -> hfx_core::AreaKm2 {
        self.area
    }

    /// Return the total upstream drainage area from `catchments.parquet`, if present.
    pub fn up_area(&self) -> Option<hfx_core::AreaKm2> {
        self.up_area
    }

    /// Return the declared outlet coordinate for this drainage unit.
    pub fn outlet(&self) -> OutletCoord {
        self.outlet
    }

    /// Return the whole drainage-unit geometry before terminal refinement.
    pub fn geometry(&self) -> &MultiPolygon<f64> {
        &self.geometry
    }
}

/// Terminal-first collection of pre-merge drainage-unit records.
///
/// Includes the whole terminal polygon and never a carved terminal. The
/// terminal-first ordering exists for typed inspection; it cannot affect final
/// geometry because the downstream dissolve path re-sorts polygons by spatial
/// key before reducing them.
#[derive(Debug, Clone, PartialEq)]
pub struct PreMergeDrainageUnits {
    terminal: UnitId,
    selected_level: SelectedLevel,
    units: Vec<PreMergeDrainageUnit>,
}

impl PreMergeDrainageUnits {
    /// Construct a terminal-first collection after records are materialized.
    pub(crate) fn new(
        terminal: UnitId,
        selected_level: SelectedLevel,
        units: Vec<PreMergeDrainageUnit>,
    ) -> Self {
        Self {
            terminal,
            selected_level,
            units,
        }
    }

    /// Construct a terminal-first collection for focused integration tests.
    #[cfg(feature = "test-fixtures")]
    pub fn new_for_test(
        terminal: UnitId,
        selected_level: SelectedLevel,
        units: Vec<PreMergeDrainageUnit>,
    ) -> Self {
        Self::new(terminal, selected_level, units)
    }

    /// Return the terminal unit ID represented by the first record.
    pub fn terminal(&self) -> UnitId {
        self.terminal
    }

    /// Return the whole terminal drainage-unit record.
    pub fn terminal_unit(&self) -> Option<&PreMergeDrainageUnit> {
        self.units.first()
    }

    /// Return the selected level shared by every record.
    pub fn selected_level(&self) -> SelectedLevel {
        self.selected_level
    }

    /// Return the terminal-first drainage-unit records.
    pub fn units(&self) -> &[PreMergeDrainageUnit] {
        &self.units
    }
}

/// Terminal-refinement result for the staged contract.
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalRefinement {
    /// Refinement was disabled by the caller.
    Disabled,
    /// Best-effort refinement was visibly skipped.
    BestEffortSkipped {
        /// Provenance explaining why refinement was skipped.
        provenance: RefinementProvenance,
    },
    /// Refinement produced a terminal geometry override.
    Applied {
        /// Refined outlet coordinate returned by raster snapping.
        refined_outlet: GeoCoord,
        /// Refined terminal geometry used instead of the whole terminal polygon.
        geometry: ContainedTerminalPolygon,
        /// Provenance explaining why refinement ran.
        provenance: RefinementProvenance,
    },
}

impl TerminalRefinement {
    /// Construct a visible best-effort skip for missing D8 declarations.
    pub fn best_effort_no_d8_aux_declared() -> Self {
        Self::BestEffortSkipped {
            provenance: RefinementProvenance::BestEffortSkipped {
                strategy: RefinementStrategyName::BestEffortD8IfPresent,
                why: BestEffortSkipReason::NoD8AuxDeclared,
            },
        }
    }

    /// Construct a visible best-effort skip for a missing raster source.
    pub fn best_effort_no_raster_source_provided() -> Self {
        Self::BestEffortSkipped {
            provenance: RefinementProvenance::BestEffortSkipped {
                strategy: RefinementStrategyName::BestEffortD8IfPresent,
                why: BestEffortSkipReason::NoRasterSourceProvided,
            },
        }
    }
}

/// Final dissolved watershed geometry and computed geodesic area.
#[derive(Debug, Clone, PartialEq)]
pub struct DissolvedWatershed {
    geometry: MultiPolygon<f64>,
    area_km2: AreaKm2,
}

impl DissolvedWatershed {
    /// Construct a dissolved watershed from assembled geometry and area.
    pub(crate) fn new(geometry: MultiPolygon<f64>, area_km2: AreaKm2) -> Self {
        Self { geometry, area_km2 }
    }

    /// Return the dissolved watershed geometry.
    pub fn geometry(&self) -> &MultiPolygon<f64> {
        &self.geometry
    }

    /// Return the geodesic watershed area in km².
    pub fn area_km2(&self) -> AreaKm2 {
        self.area_km2
    }
}
