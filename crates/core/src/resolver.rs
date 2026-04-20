//! Outlet resolution — resolve a user coordinate to a terminal HFX atom ID.
//!
//! Two code paths:
//! - **Snap path** (`snap.parquet` present): nearest-geometry search within a
//!   configurable search radius.
//! - **PiP path** (no snap file): point-in-polygon containment test against
//!   catchment geometries with area-based tie-breaking.

use std::fmt;

use geo::{Contains, Intersects};
use hfx_core::{AtomId, BoundingBox, CatchmentAtom, MainstemStatus, SnapId, Weight};
use tracing::{debug, info, instrument, warn};

use crate::algo::coord::GeoCoord;
use crate::algo::wkb::{decode_wkb, decode_wkb_multi_polygon};
use crate::error::SessionError;
use crate::session::DatasetSession;

// ── SearchRadiusMetres ────────────────────────────────────────────────────────

/// Search radius for snap-path outlet resolution, in metres.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct SearchRadiusMetres(f64);

impl SearchRadiusMetres {
    /// Default search radius (1 000 m).
    pub const DEFAULT: Self = Self(1000.0);

    /// Construct a new search radius.
    ///
    /// # Errors
    ///
    /// Returns an error string if `metres` is not finite or not positive.
    pub fn new(metres: f64) -> Result<Self, &'static str> {
        if !metres.is_finite() || metres <= 0.0 {
            return Err("search radius must be finite and positive");
        }
        Ok(Self(metres))
    }

    /// Return the raw value in metres.
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

impl fmt::Display for SearchRadiusMetres {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.0} m", self.0)
    }
}

/// Snap-target ranking strategy for outlet resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapStrategy {
    /// Opt-in strategy: prefer the nearest candidate, using weight/mainstem
    /// only as tie-breakers within a configurable distance tolerance band.
    /// Use this for datasets whose weights are not hydrologically rank-meaningful.
    DistanceFirst,
    /// Default strategy (HFX v0.2): prefer the highest-weight candidate inside
    /// the search radius, using mainstem preference, distance, then snap ID as
    /// tie-breakers. Requires weights to be monotonically increasing in drainage
    /// dominance (e.g. upstream area in km²).
    WeightFirst,
}

impl fmt::Display for SnapStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::DistanceFirst => "distance-first",
            Self::WeightFirst => "weight-first",
        };
        f.write_str(value)
    }
}

// ── ResolverConfig ────────────────────────────────────────────────────────────

/// Configuration for [`resolve_outlet`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResolverConfig {
    search_radius: SearchRadiusMetres,
    distance_tolerance_m: f64,
    snap_strategy: SnapStrategy,
}

impl ResolverConfig {
    /// Create a config with default values.
    pub fn new() -> Self {
        Self {
            search_radius: SearchRadiusMetres::DEFAULT,
            distance_tolerance_m: 1.0,
            snap_strategy: SnapStrategy::WeightFirst,
        }
    }

    /// Override the snap-path search radius.
    pub fn with_search_radius(mut self, radius: SearchRadiusMetres) -> Self {
        self.search_radius = radius;
        self
    }

    /// Override the distance tolerance for snap-target tie-breaking.
    ///
    /// Candidates within this tolerance of the nearest candidate are
    /// considered equidistant, allowing weight and mainstem status to
    /// break the tie instead of floating-point noise.
    ///
    /// Must be finite and non-negative (zero disables tolerance).
    ///
    /// # Errors
    ///
    /// Returns an error string if `tolerance_m` is negative, NaN, or infinite.
    pub fn with_distance_tolerance(mut self, tolerance_m: f64) -> Result<Self, &'static str> {
        if !tolerance_m.is_finite() || tolerance_m < 0.0 {
            return Err("distance tolerance must be finite and non-negative");
        }
        self.distance_tolerance_m = tolerance_m;
        Ok(self)
    }

    /// Override the snap-target ranking strategy.
    pub fn with_snap_strategy(mut self, strategy: SnapStrategy) -> Self {
        self.snap_strategy = strategy;
        self
    }

    /// Return the configured search radius.
    pub fn search_radius(&self) -> SearchRadiusMetres {
        self.search_radius
    }

    /// Return the configured distance tolerance in metres.
    pub fn distance_tolerance_m(&self) -> f64 {
        self.distance_tolerance_m
    }

    /// Return the configured snap-target ranking strategy.
    pub fn snap_strategy(&self) -> SnapStrategy {
        self.snap_strategy
    }
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── PipTieBreak ───────────────────────────────────────────────────────────────

/// The reason a tie was broken when multiple catchments contain the outlet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipTieBreak {
    /// The catchment with the highest total upstream drainage area was chosen.
    HighestUpstreamArea,
    /// The catchment with the highest local (self) area was chosen.
    HighestLocalArea,
    /// All area metrics were equal; the catchment with the lowest atom ID was
    /// chosen as a deterministic fallback.
    LowestAtomId,
}

// ── ResolutionMethod ──────────────────────────────────────────────────────────

/// Provenance record describing which resolution code path was used.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolutionMethod {
    /// The outlet was resolved via the snap-file nearest-geometry search.
    Snap {
        /// Ranking strategy used to choose the winning snap candidate.
        strategy: SnapStrategy,
        /// ID of the snap target that was selected.
        snap_id: SnapId,
        /// Planar distance in metres from the input outlet to the snapped point.
        distance_m: f64,
        /// Snap weight reported by the HFX dataset.
        weight: Weight,
        /// Whether the selected snap target lies on the mainstem.
        mainstem_status: MainstemStatus,
        /// Number of snap candidates examined inside the search bbox.
        candidates_considered: usize,
    },
    /// The outlet was resolved via point-in-polygon containment testing.
    PointInPolygon {
        /// Number of catchment candidates examined.
        candidates_considered: usize,
        /// Set when more than one catchment contained the outlet and a
        /// tie-breaking rule was applied.
        tie_break: Option<PipTieBreak>,
    },
}

// ── ResolvedOutlet ────────────────────────────────────────────────────────────

/// The result of a successful outlet resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedOutlet {
    /// The HFX atom ID that the outlet resolved to.
    pub atom_id: AtomId,
    /// The original coordinate supplied by the caller.
    pub input_coord: GeoCoord,
    /// The coordinate that was actually used for the resolution (may differ
    /// from `input_coord` after snapping).
    pub resolved_coord: GeoCoord,
    /// Provenance: which resolution path was taken and why.
    pub method: ResolutionMethod,
}

// ── OutletResolutionError ─────────────────────────────────────────────────────

/// Errors that can occur during outlet resolution.
#[derive(Debug, thiserror::Error)]
pub enum OutletResolutionError {
    /// Fired on the snap path when no snap target geometry intersects the
    /// search bounding box around the outlet.
    #[error("no snap candidates within {search_radius} of outlet {outlet}")]
    NoSnapCandidates {
        /// The outlet coordinate that was queried.
        outlet: GeoCoord,
        /// The search radius that was used.
        search_radius: SearchRadiusMetres,
    },

    /// Fired on the PiP path when the outlet falls outside every catchment
    /// polygon in the dataset.
    #[error("outlet {outlet} is outside all catchment polygons")]
    OutsideAllCatchments {
        /// The outlet coordinate that was tested.
        outlet: GeoCoord,
    },

    /// Fired when reading catchment or snap data from the Parquet store fails.
    #[error("dataset read error during resolution: {source}")]
    DatasetRead {
        /// Underlying session error.
        #[from]
        source: SessionError,
    },

    /// Fired when all candidate geometries failed to decode.
    ///
    /// Individual decode failures are logged as warnings and skipped.
    /// This variant is returned only when every candidate in the search
    /// area had corrupt or unsupported geometry, leaving no valid
    /// candidates to evaluate.
    #[error("all {count} candidate geometries near outlet {outlet} failed to decode")]
    AllGeometriesCorrupt {
        /// The outlet coordinate that was queried.
        outlet: GeoCoord,
        /// Number of candidates that failed to decode.
        count: usize,
    },
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Convert a search radius in metres to a bounding box in degrees around a point.
///
/// # Limitations
///
/// Does not handle antimeridian wraparound. For outlets near lon=±180,
/// candidates on the opposite side of the antimeridian will be missed.
fn search_bbox(center: GeoCoord, radius_m: f64) -> Result<BoundingBox, OutletResolutionError> {
    let lat_rad = center.lat.to_radians();
    let cos_lat = lat_rad.cos().abs().max(1e-10);
    let dlat = radius_m / 110_540.0;
    let dlon = radius_m / (111_320.0 * cos_lat);
    let minx = ((center.lon - dlon).max(-180.0)) as f32;
    let miny = ((center.lat - dlat).max(-90.0)) as f32;
    let maxx = ((center.lon + dlon).min(180.0)) as f32;
    let maxy = ((center.lat + dlat).min(90.0)) as f32;
    BoundingBox::new(minx, miny, maxx, maxy).map_err(|e| OutletResolutionError::DatasetRead {
        source: SessionError::integrity(format!("search bbox construction failed: {e}")),
    })
}

/// Local tangent-plane distance in metres between two WGS84 points.
fn local_metre_distance(a: GeoCoord, b: GeoCoord) -> f64 {
    let lat_avg = ((a.lat + b.lat) / 2.0).to_radians();
    let dx_m = (b.lon - a.lon) * 111_320.0 * lat_avg.cos();
    let dy_m = (b.lat - a.lat) * 110_540.0;
    (dx_m * dx_m + dy_m * dy_m).sqrt()
}

/// Find the nearest point on a geometry to the outlet, with local-metre distance.
/// Returns None for degenerate geometries (Closest::Indeterminate).
fn snap_nearest_point(outlet: GeoCoord, geom: &geo::Geometry<f64>) -> Option<(f64, GeoCoord)> {
    use geo::{Closest, ClosestPoint};
    let outlet_point: geo::Point<f64> = outlet.into();
    match geom.closest_point(&outlet_point) {
        Closest::Intersection(p) | Closest::SinglePoint(p) => {
            let nearest = GeoCoord::from(p);
            let dist = local_metre_distance(outlet, nearest);
            Some((dist, nearest))
        }
        Closest::Indeterminate => None,
    }
}

// ── Snap resolution ───────────────────────────────────────────────────────────

/// Scored snap candidate after distance computation.
struct ScoredCandidate {
    target: hfx_core::SnapTarget,
    distance_m: f64,
    nearest_coord: GeoCoord,
}

fn internal_resolver_inconsistency(detail: impl Into<String>) -> OutletResolutionError {
    OutletResolutionError::DatasetRead {
        source: SessionError::integrity(detail),
    }
}

/// Resolve via snap-file nearest-geometry search.
///
/// # Errors
///
/// | Variant | Condition |
/// |---|---|
/// | [`OutletResolutionError::NoSnapCandidates`] | No targets within search radius |
/// | [`OutletResolutionError::DatasetRead`] | Parquet store query failed |
fn resolve_via_snap(
    session: &DatasetSession,
    outlet: GeoCoord,
    config: &ResolverConfig,
) -> Result<ResolvedOutlet, OutletResolutionError> {
    // 1. Build search bbox.
    let bbox = search_bbox(outlet, config.search_radius().as_f64())?;

    // 2. Query snap store.
    let candidates = session
        .snap()
        .ok_or_else(|| {
            internal_resolver_inconsistency("resolve_via_snap called without snap store")
        })?
        .query_by_bbox(&bbox)?;
    let total_candidates = candidates.len();
    debug!(
        candidate_count = total_candidates,
        outlet = %outlet,
        "snap bbox candidates retrieved"
    );

    // 3. Empty bbox result → no candidates.
    if candidates.is_empty() {
        return Err(OutletResolutionError::NoSnapCandidates {
            outlet,
            search_radius: config.search_radius(),
        });
    }

    // 4. Score each candidate: decode WKB, compute nearest point, apply circular filter.
    let mut scored: Vec<ScoredCandidate> = Vec::with_capacity(candidates.len());
    let mut decode_failures: usize = 0;
    for target in candidates {
        let geom = match decode_wkb(target.geometry()) {
            Ok(g) => g,
            Err(e) => {
                warn!(
                    snap_id = target.id().get(),
                    error = %e,
                    "failed to decode snap target WKB, skipping"
                );
                decode_failures += 1;
                continue;
            }
        };

        let (distance_m, nearest_coord) = match snap_nearest_point(outlet, &geom) {
            Some(pair) => pair,
            None => {
                warn!(
                    snap_id = target.id().get(),
                    "indeterminate closest point for snap target, skipping"
                );
                decode_failures += 1;
                continue;
            }
        };

        // Post-filter: bbox is rectangular, search radius is circular.
        if distance_m > config.search_radius().as_f64() {
            continue;
        }

        scored.push(ScoredCandidate {
            target,
            distance_m,
            nearest_coord,
        });
    }

    // 5. No scored candidates after filtering → error.
    if scored.is_empty() {
        // Only report corrupt geometries when every candidate failed to
        // decode. Mixed outcomes (some decoded, some filtered by radius)
        // are a normal "no candidates" situation.
        if decode_failures > 0 && decode_failures == total_candidates {
            return Err(OutletResolutionError::AllGeometriesCorrupt {
                outlet,
                count: decode_failures,
            });
        }
        return Err(OutletResolutionError::NoSnapCandidates {
            outlet,
            search_radius: config.search_radius(),
        });
    }

    // 6. Two-step selection with distance tolerance:
    //    a) Find the minimum distance among all scored candidates.
    //    b) Restrict to candidates within min_distance + tolerance.
    //    c) Among those, rank by weight DESC → mainstem DESC → snap_id ASC.
    let winner = match config.snap_strategy() {
        SnapStrategy::DistanceFirst => {
            let tolerance = config.distance_tolerance_m();
            let min_distance = scored
                .iter()
                .map(|c| c.distance_m)
                .min_by(f64::total_cmp)
                .ok_or_else(|| {
                    internal_resolver_inconsistency(
                        "distance-first snap selection called with no scored candidates",
                    )
                })?;
            let threshold = min_distance + tolerance;

            scored
                .into_iter()
                .filter(|c| c.distance_m <= threshold)
                .min_by(|a, b| {
                    // Within the tolerance band: rank by weight, mainstem, then id.
                    // Distance is NOT used here — all candidates in the band are
                    // treated as equidistant.
                    b.target
                        .weight()
                        .get()
                        .total_cmp(&a.target.weight().get())
                        .then_with(|| {
                            let mainstem_rank = |s: MainstemStatus| match s {
                                MainstemStatus::Mainstem => 1u8,
                                MainstemStatus::Tributary => 0u8,
                            };
                            mainstem_rank(b.target.mainstem_status())
                                .cmp(&mainstem_rank(a.target.mainstem_status()))
                        })
                        .then_with(|| a.target.id().get().cmp(&b.target.id().get()))
                })
                .ok_or_else(|| {
                    internal_resolver_inconsistency(
                        "distance-first tolerance band produced no snap winner",
                    )
                })?
        }
        SnapStrategy::WeightFirst => scored
            .into_iter()
            .min_by(|a, b| {
                b.target
                    .weight()
                    .get()
                    .total_cmp(&a.target.weight().get())
                    .then_with(|| {
                        let mainstem_rank = |s: MainstemStatus| match s {
                            MainstemStatus::Mainstem => 1u8,
                            MainstemStatus::Tributary => 0u8,
                        };
                        mainstem_rank(b.target.mainstem_status())
                            .cmp(&mainstem_rank(a.target.mainstem_status()))
                    })
                    .then_with(|| a.distance_m.total_cmp(&b.distance_m))
                    .then_with(|| a.target.id().get().cmp(&b.target.id().get()))
            })
            .ok_or_else(|| {
                internal_resolver_inconsistency(
                    "weight-first snap selection called with no scored candidates",
                )
            })?,
    };

    // 7. Build result.
    info!(
        snap_id = winner.target.id().get(),
        catchment_id = winner.target.catchment_id().get(),
        distance_m = winner.distance_m,
        "snap resolved outlet"
    );

    Ok(ResolvedOutlet {
        atom_id: winner.target.catchment_id(),
        input_coord: outlet,
        resolved_coord: winner.nearest_coord,
        method: ResolutionMethod::Snap {
            strategy: config.snap_strategy(),
            snap_id: winner.target.id(),
            distance_m: winner.distance_m,
            weight: winner.target.weight(),
            mainstem_status: winner.target.mainstem_status(),
            candidates_considered: total_candidates,
        },
    })
}

// ── PiP resolution ───────────────────────────────────────────────────────────

/// Fixed buffer in metres for PiP bbox query. The point must be inside the
/// catchment, so a small buffer suffices for bbox intersection filtering.
const PIP_BUFFER_M: f64 = 100.0;

/// Resolve via point-in-polygon containment test.
fn resolve_via_pip(
    session: &DatasetSession,
    outlet: GeoCoord,
) -> Result<ResolvedOutlet, OutletResolutionError> {
    // 1. Build a small search bbox around the outlet.
    let bbox = search_bbox(outlet, PIP_BUFFER_M)?;

    // 2. Query catchment store for candidates.
    let candidates = session.catchments().query_by_bbox(&bbox)?;
    debug!(
        candidate_count = candidates.len(),
        outlet = %outlet,
        "PiP bbox candidates retrieved"
    );

    // 3. Empty candidates → outside all catchments.
    if candidates.is_empty() {
        return Err(OutletResolutionError::OutsideAllCatchments { outlet });
    }

    // 4. Decode geometries upfront, skipping failures.
    let mut decode_failures: usize = 0;
    let decoded: Vec<(&CatchmentAtom, geo::MultiPolygon<f64>)> = candidates
        .iter()
        .filter_map(|atom| match decode_wkb_multi_polygon(atom.geometry()) {
            Ok(mp) => Some((atom, mp)),
            Err(e) => {
                warn!(
                    atom_id = atom.id().get(),
                    error = %e,
                    "failed to decode WKB geometry, skipping candidate"
                );
                decode_failures += 1;
                None
            }
        })
        .collect();

    // 5. Convert outlet to geo::Point.
    let point: geo::Point<f64> = outlet.into();

    // 6. Phase 1 — strict containment.
    let mut hits: Vec<&CatchmentAtom> = decoded
        .iter()
        .filter(|(_, mp)| mp.contains(&point))
        .map(|(atom, _)| *atom)
        .collect();
    debug!(phase1_hits = hits.len(), "PiP phase 1 (contains) complete");

    // 7. Phase 2 — boundary fallback if no strict hits.
    if hits.is_empty() {
        hits = decoded
            .iter()
            .filter(|(_, mp)| mp.intersects(&point))
            .map(|(atom, _)| *atom)
            .collect();
        debug!(
            phase2_hits = hits.len(),
            "PiP phase 2 (intersects) complete"
        );
    }

    // 8. No hits at all → outside all catchments (or all geometries corrupt).
    if hits.is_empty() {
        // Only report corrupt geometries when every candidate failed to
        // decode. Mixed outcomes (some decoded but didn't contain the
        // point) are a normal "outside all catchments" situation.
        if decode_failures > 0 && decode_failures == candidates.len() {
            return Err(OutletResolutionError::AllGeometriesCorrupt {
                outlet,
                count: decode_failures,
            });
        }
        return Err(OutletResolutionError::OutsideAllCatchments { outlet });
    }

    // 9. Single hit → no tie-break needed.
    if hits.len() == 1 {
        let winner = hits[0];
        info!(
            atom_id = winner.id().get(),
            tie_break = ?Option::<PipTieBreak>::None,
            "PiP resolved outlet"
        );
        return Ok(ResolvedOutlet {
            atom_id: winner.id(),
            input_coord: outlet,
            resolved_coord: outlet,
            method: ResolutionMethod::PointInPolygon {
                candidates_considered: decoded.len(),
                tie_break: None,
            },
        });
    }

    // 10. Multiple hits → sort by tie-break cascade.
    hits.sort_by(|a, b| {
        // 1. upstream_area DESC (Some beats None, higher value wins)
        let ua_a = a.upstream_area().map(|u| u.get());
        let ua_b = b.upstream_area().map(|u| u.get());
        let ua_ord = match (ua_a, ua_b) {
            (Some(x), Some(y)) => y.total_cmp(&x),       // DESC
            (Some(_), None) => std::cmp::Ordering::Less, // Some before None
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        ua_ord
            .then_with(|| {
                // 2. area DESC
                b.area().get().total_cmp(&a.area().get())
            })
            .then_with(|| {
                // 3. atom_id ASC
                a.id().cmp(&b.id())
            })
    });

    // Determine which rule actually broke the tie.
    let tie_break = {
        let a = hits[0];
        let b = hits[1];
        if a.upstream_area().map(|u| u.get()) != b.upstream_area().map(|u| u.get()) {
            Some(PipTieBreak::HighestUpstreamArea)
        } else if a.area().get().total_cmp(&b.area().get()) != std::cmp::Ordering::Equal {
            Some(PipTieBreak::HighestLocalArea)
        } else {
            Some(PipTieBreak::LowestAtomId)
        }
    };

    let winner = hits[0];
    info!(
        atom_id = winner.id().get(),
        tie_break = ?tie_break,
        "PiP resolved outlet with tie-break"
    );

    Ok(ResolvedOutlet {
        atom_id: winner.id(),
        input_coord: outlet,
        resolved_coord: outlet,
        method: ResolutionMethod::PointInPolygon {
            candidates_considered: decoded.len(),
            tie_break,
        },
    })
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Resolve a user-provided outlet coordinate to a single terminal HFX atom ID.
///
/// Uses the snap-file path when `snap.parquet` is present in the dataset,
/// falling back to point-in-polygon containment when it is not.
///
/// # Errors
///
/// | Variant | Condition |
/// |---|---|
/// | [`OutletResolutionError::NoSnapCandidates`] | Snap path: no targets within search radius |
/// | [`OutletResolutionError::OutsideAllCatchments`] | PiP path: outlet not in any catchment |
/// | [`OutletResolutionError::DatasetRead`] | Parquet store query failed |
/// | [`OutletResolutionError::AllGeometriesCorrupt`] | All candidate geometries in the search area failed to decode |
#[instrument(skip(session, config), fields(outlet = %outlet))]
pub fn resolve_outlet(
    session: &DatasetSession,
    outlet: GeoCoord,
    config: &ResolverConfig,
) -> Result<ResolvedOutlet, OutletResolutionError> {
    if session.snap().is_some() {
        debug!("snap.parquet present, using snap resolution path");
        resolve_via_snap(session, outlet, config)
    } else {
        debug!("no snap.parquet, using point-in-polygon resolution path");
        resolve_via_pip(session, outlet)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::DatasetSession;
    use crate::testutil::{DatasetBuilder, TestCatchment, TestSnapGeometry, TestSnapTarget};

    // ── Group A: SearchRadiusMetres ───────────────────────────────────────────

    #[test]
    fn search_radius_default() {
        assert_eq!(SearchRadiusMetres::DEFAULT.as_f64(), 1000.0);
    }

    #[test]
    fn search_radius_display() {
        assert_eq!(
            format!("{}", SearchRadiusMetres::new(1500.0).unwrap()),
            "1500 m"
        );
    }

    #[test]
    fn search_radius_rejects_negative() {
        assert!(SearchRadiusMetres::new(-1.0).is_err());
    }

    #[test]
    fn search_radius_rejects_zero() {
        assert!(SearchRadiusMetres::new(0.0).is_err());
    }

    #[test]
    fn search_radius_rejects_nan() {
        assert!(SearchRadiusMetres::new(f64::NAN).is_err());
    }

    // ── Group B: ResolverConfig ───────────────────────────────────────────────

    #[test]
    fn config_default_radius() {
        let config = ResolverConfig::new();
        assert_eq!(config.search_radius(), SearchRadiusMetres::DEFAULT);
    }

    #[test]
    fn config_with_custom_radius() {
        let config =
            ResolverConfig::new().with_search_radius(SearchRadiusMetres::new(5000.0).unwrap());
        assert_eq!(config.search_radius().as_f64(), 5000.0);
    }

    #[test]
    fn config_default_tolerance() {
        let config = ResolverConfig::new();
        assert_eq!(config.distance_tolerance_m(), 1.0);
    }

    #[test]
    fn config_default_snap_strategy() {
        let config = ResolverConfig::new();
        assert_eq!(config.snap_strategy(), SnapStrategy::WeightFirst);
    }

    #[test]
    fn snap_strategy_display_uses_kebab_case() {
        assert_eq!(SnapStrategy::DistanceFirst.to_string(), "distance-first");
        assert_eq!(SnapStrategy::WeightFirst.to_string(), "weight-first");
    }

    #[test]
    fn config_with_custom_tolerance() {
        let config = ResolverConfig::new().with_distance_tolerance(5.0).unwrap();
        assert_eq!(config.distance_tolerance_m(), 5.0);
    }

    #[test]
    fn config_tolerance_zero_is_valid() {
        let config = ResolverConfig::new().with_distance_tolerance(0.0).unwrap();
        assert_eq!(config.distance_tolerance_m(), 0.0);
    }

    #[test]
    fn config_with_custom_snap_strategy() {
        let config = ResolverConfig::new().with_snap_strategy(SnapStrategy::WeightFirst);
        assert_eq!(config.snap_strategy(), SnapStrategy::WeightFirst);
    }

    #[test]
    fn config_tolerance_rejects_negative() {
        assert!(ResolverConfig::new().with_distance_tolerance(-1.0).is_err());
    }

    #[test]
    fn config_tolerance_rejects_nan() {
        assert!(ResolverConfig::new()
            .with_distance_tolerance(f64::NAN)
            .is_err());
    }

    #[test]
    fn config_tolerance_rejects_infinity() {
        assert!(ResolverConfig::new()
            .with_distance_tolerance(f64::INFINITY)
            .is_err());
    }

    // ── Group C: search_bbox ──────────────────────────────────────────────────

    #[test]
    fn search_bbox_at_equator() {
        let center = GeoCoord::new(0.0, 0.0);
        let bbox = search_bbox(center, 1000.0).unwrap();
        // At equator, ~0.009 degrees for 1km
        // Just verify it's non-degenerate and roughly symmetric
        // (BoundingBox doesn't expose fields directly, but we can check it was created successfully)
        let _ = bbox;
    }

    #[test]
    fn search_bbox_at_60n() {
        // At 60N, longitude degrees are ~half the size, so dlon should be ~2x dlat
        let center = GeoCoord::new(10.0, 60.0);
        let _bbox = search_bbox(center, 1000.0).unwrap();
    }

    #[test]
    fn search_bbox_near_pole() {
        let center = GeoCoord::new(0.0, 89.0);
        let _bbox = search_bbox(center, 1000.0).unwrap();
    }

    #[test]
    fn search_bbox_large_radius() {
        let center = GeoCoord::new(0.0, 0.0);
        let _bbox = search_bbox(center, 50_000.0).unwrap();
    }

    // ── Group D: local_metre_distance ─────────────────────────────────────────

    #[test]
    fn local_distance_coincident() {
        let p = GeoCoord::new(0.0, 0.0);
        assert_eq!(local_metre_distance(p, p), 0.0);
    }

    #[test]
    fn local_distance_short_ns() {
        // 0.001 degrees latitude at equator ≈ 111 metres
        let a = GeoCoord::new(0.0, 0.0);
        let b = GeoCoord::new(0.0, 0.001);
        let d = local_metre_distance(a, b);
        assert!(d > 100.0 && d < 120.0, "expected ~111m, got {d}");
    }

    #[test]
    fn local_distance_short_ew() {
        // 0.001 degrees longitude at equator ≈ 111 metres
        let a = GeoCoord::new(0.0, 0.0);
        let b = GeoCoord::new(0.001, 0.0);
        let d = local_metre_distance(a, b);
        assert!(d > 100.0 && d < 120.0, "expected ~111m, got {d}");
    }

    #[test]
    fn local_distance_symmetry() {
        let a = GeoCoord::new(10.0, 50.0);
        let b = GeoCoord::new(10.1, 50.05);
        assert_eq!(local_metre_distance(a, b), local_metre_distance(b, a));
    }

    // ── Group E: snap_nearest_point ───────────────────────────────────────────

    #[test]
    fn snap_nearest_point_to_point() {
        use geo::{Geometry, Point};
        let outlet = GeoCoord::new(0.0, 0.0);
        let geom = Geometry::Point(Point::new(0.001, 0.0));
        let (dist, nearest) = snap_nearest_point(outlet, &geom).unwrap();
        assert!(dist > 100.0 && dist < 120.0);
        assert_eq!(nearest.lon, 0.001);
        assert_eq!(nearest.lat, 0.0);
    }

    #[test]
    fn snap_nearest_point_to_linestring() {
        use geo::{Geometry, LineString};
        // Perpendicular drop from (1.0, 0.001) onto horizontal line at y=0
        let outlet = GeoCoord::new(1.0, 0.001);
        let geom = Geometry::LineString(LineString::from(vec![
            (0.5_f64, 0.0_f64),
            (1.5_f64, 0.0_f64),
        ]));
        let (dist, nearest) = snap_nearest_point(outlet, &geom).unwrap();
        assert!(dist > 100.0 && dist < 120.0, "expected ~111m, got {dist}");
        // Nearest point should be at approximately (1.0, 0.0)
        assert!((nearest.lon - 1.0).abs() < 0.0001);
        assert!(nearest.lat.abs() < 0.0001);
    }

    #[test]
    fn snap_nearest_coincident() {
        use geo::{Geometry, Point};
        let outlet = GeoCoord::new(1.0, 1.0);
        let geom = Geometry::Point(Point::new(1.0, 1.0));
        let (dist, _) = snap_nearest_point(outlet, &geom).unwrap();
        assert_eq!(dist, 0.0);
    }

    // ── Group F: PipTieBreak ──────────────────────────────────────────────────

    #[test]
    fn pip_tie_break_variants_distinct() {
        assert_ne!(
            PipTieBreak::HighestUpstreamArea,
            PipTieBreak::HighestLocalArea
        );
        assert_ne!(PipTieBreak::HighestLocalArea, PipTieBreak::LowestAtomId);
    }

    #[test]
    fn distance_first_snap_prefers_exact_match() {
        let catchments = vec![
            TestCatchment {
                id: 1,
                area_km2: 1.0,
                up_area_km2: None,
                polygon: (0.0, 0.0, 0.4, 0.4),
            },
            TestCatchment {
                id: 2,
                area_km2: 1000.0,
                up_area_km2: None,
                polygon: (0.5, 0.0, 0.9, 0.4),
            },
        ];
        let targets = vec![
            TestSnapTarget {
                id: 11,
                catchment_id: 1,
                weight: 1.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.2, 0.2),
            },
            TestSnapTarget {
                id: 22,
                catchment_id: 2,
                weight: 1000.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.205, 0.2),
            },
        ];
        let (_dir, root) = DatasetBuilder::new(2)
            .with_custom_catchments(catchments)
            .with_custom_snap_targets(targets)
            .build();
        let session = DatasetSession::open(&root).unwrap();

        let result = resolve_outlet(
            &session,
            GeoCoord::new(0.2, 0.2),
            &ResolverConfig::new().with_snap_strategy(SnapStrategy::DistanceFirst),
        )
        .unwrap();
        assert_eq!(result.atom_id, AtomId::new(1).unwrap());
        assert!(matches!(
            result.method,
            ResolutionMethod::Snap {
                strategy: SnapStrategy::DistanceFirst,
                snap_id,
                ..
            } if snap_id == hfx_core::SnapId::new(11).unwrap()
        ));
    }

    #[test]
    fn weight_first_snap_prefers_heavier_candidate() {
        let catchments = vec![
            TestCatchment {
                id: 1,
                area_km2: 1.0,
                up_area_km2: None,
                polygon: (0.0, 0.0, 0.4, 0.4),
            },
            TestCatchment {
                id: 2,
                area_km2: 1000.0,
                up_area_km2: None,
                polygon: (0.5, 0.0, 0.9, 0.4),
            },
        ];
        let targets = vec![
            TestSnapTarget {
                id: 11,
                catchment_id: 1,
                weight: 1.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.2, 0.2),
            },
            TestSnapTarget {
                id: 22,
                catchment_id: 2,
                weight: 1000.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.205, 0.2),
            },
        ];
        let (_dir, root) = DatasetBuilder::new(2)
            .with_custom_catchments(catchments)
            .with_custom_snap_targets(targets)
            .build();
        let session = DatasetSession::open(&root).unwrap();
        let config = ResolverConfig::new().with_snap_strategy(SnapStrategy::WeightFirst);

        let result = resolve_outlet(&session, GeoCoord::new(0.2, 0.2), &config).unwrap();
        assert_eq!(result.atom_id, AtomId::new(2).unwrap());
        assert!(matches!(
            result.method,
            ResolutionMethod::Snap {
                strategy: SnapStrategy::WeightFirst,
                snap_id,
                ..
            } if snap_id == hfx_core::SnapId::new(22).unwrap()
        ));
    }

    #[test]
    fn weight_first_snap_still_prefers_mainstem_on_weight_tie() {
        let catchments = vec![
            TestCatchment {
                id: 1,
                area_km2: 10.0,
                up_area_km2: None,
                polygon: (0.0, 0.0, 0.4, 0.4),
            },
            TestCatchment {
                id: 2,
                area_km2: 10.0,
                up_area_km2: None,
                polygon: (0.5, 0.0, 0.9, 0.4),
            },
        ];
        let targets = vec![
            TestSnapTarget {
                id: 11,
                catchment_id: 1,
                weight: 50.0,
                is_mainstem: false,
                geometry: TestSnapGeometry::Point(0.22, 0.2),
            },
            TestSnapTarget {
                id: 22,
                catchment_id: 2,
                weight: 50.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.205, 0.2),
            },
        ];
        let (_dir, root) = DatasetBuilder::new(2)
            .with_custom_catchments(catchments)
            .with_custom_snap_targets(targets)
            .build();
        let session = DatasetSession::open(&root).unwrap();
        let config = ResolverConfig::new().with_snap_strategy(SnapStrategy::WeightFirst);

        let result = resolve_outlet(&session, GeoCoord::new(0.2, 0.2), &config).unwrap();
        assert_eq!(result.atom_id, AtomId::new(2).unwrap());
    }

    #[test]
    fn distance_first_zero_tolerance_prefers_strict_nearest() {
        let catchments = vec![
            TestCatchment {
                id: 1,
                area_km2: 1.0,
                up_area_km2: None,
                polygon: (0.0, 0.0, 0.4, 0.4),
            },
            TestCatchment {
                id: 2,
                area_km2: 1000.0,
                up_area_km2: None,
                polygon: (0.5, 0.0, 0.9, 0.4),
            },
        ];
        let targets = vec![
            TestSnapTarget {
                id: 11,
                catchment_id: 1,
                weight: 1.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.2, 0.2),
            },
            TestSnapTarget {
                id: 22,
                catchment_id: 2,
                weight: 1000.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.200005, 0.2),
            },
        ];
        let (_dir, root) = DatasetBuilder::new(2)
            .with_custom_catchments(catchments)
            .with_custom_snap_targets(targets)
            .build();
        let session = DatasetSession::open(&root).unwrap();
        let config = ResolverConfig::new()
            .with_snap_strategy(SnapStrategy::DistanceFirst)
            .with_distance_tolerance(0.0)
            .unwrap();

        let result = resolve_outlet(&session, GeoCoord::new(0.2, 0.2), &config).unwrap();
        assert_eq!(result.atom_id, AtomId::new(1).unwrap());
        assert!(matches!(
            result.method,
            ResolutionMethod::Snap {
                strategy: SnapStrategy::DistanceFirst,
                snap_id,
                ..
            } if snap_id == hfx_core::SnapId::new(11).unwrap()
        ));
    }

    #[test]
    fn default_strategy_picks_mainstem_over_coincident_tiny_stub() {
        // Bulgaria regression: when the input sits exactly on a tiny
        // tributary stub's first vertex (distance=0), the default strategy
        // must still pick the larger mainstem ~60 m away. This encodes the
        // HFX v0.2 weight-first contract: weight determines ranking;
        // mainstem/distance/id are only tie-breakers.
        let catchments = vec![
            TestCatchment {
                id: 1,
                area_km2: 0.08,
                up_area_km2: None,
                polygon: (9.99, 44.99, 10.01, 45.01),
            },
            TestCatchment {
                id: 2,
                area_km2: 100.0,
                up_area_km2: Some(8800.0),
                polygon: (9.98, 44.98, 10.02, 45.02),
            },
        ];
        let targets = vec![
            TestSnapTarget {
                id: 11,
                catchment_id: 1,
                weight: 0.1,
                is_mainstem: false,
                // First vertex at (10.0, 45.0) — coincident with the input.
                geometry: TestSnapGeometry::LineString(10.0, 45.0, 10.0001, 45.0001),
            },
            TestSnapTarget {
                id: 22,
                catchment_id: 2,
                weight: 8816.84,
                is_mainstem: true,
                // ~60 m east at this latitude (0.000771 deg lon * 111320 * cos45° ≈ 60.7 m).
                geometry: TestSnapGeometry::LineString(10.000771, 44.9995, 10.000771, 45.0005),
            },
        ];
        let (_dir, root) = DatasetBuilder::new(2)
            .with_custom_catchments(catchments)
            .with_custom_snap_targets(targets)
            .build();
        let session = DatasetSession::open(&root).unwrap();

        let result =
            resolve_outlet(&session, GeoCoord::new(10.0, 45.0), &ResolverConfig::new()).unwrap();

        assert_eq!(
            result.atom_id,
            AtomId::new(2).unwrap(),
            "default strategy must pick the mainstem (atom 2), not the coincident tiny stub (atom 1)"
        );
        assert!(matches!(
            result.method,
            ResolutionMethod::Snap {
                strategy: SnapStrategy::WeightFirst,
                snap_id,
                ..
            } if snap_id == hfx_core::SnapId::new(22).unwrap()
        ));
    }
}
