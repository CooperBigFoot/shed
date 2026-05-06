//! Dissolve a set of polygons into a single multi-polygon via iterative union.
//!
//! `geo` does not expose a dedicated unary-union API, but `MultiPolygon`
//! implements [`BooleanOps`]. Unioning a flat `MultiPolygon` against an empty
//! `MultiPolygon` is not a viable replacement in `geo` 0.29 because overlapping
//! members are not dissolved; the raw comparison is kept for benchmarks only.

use geo::{BooleanOps, BoundingRect, MultiPolygon, Polygon};
use rayon::prelude::*;
use tracing::{debug, instrument};

/// Errors from polygon dissolve operations.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DissolveError {
    /// Returned when the input polygon set is empty.
    #[error("cannot dissolve an empty polygon set")]
    EmptyInput,
}

/// Dissolve polygons into a unified multi-polygon.
///
/// Performs an iterative boolean union: seeds the accumulator with the first
/// polygon, then folds each subsequent polygon into the result via
/// [`BooleanOps::union`].
///
/// # Errors
///
/// | Condition | Error |
/// |-----------|-------|
/// | `polygons` is empty | [`DissolveError::EmptyInput`] |
#[instrument(skip(polygons))]
pub fn dissolve(polygons: Vec<Polygon<f64>>) -> Result<MultiPolygon<f64>, DissolveError> {
    if polygons.is_empty() {
        return Err(DissolveError::EmptyInput);
    }

    debug!(count = polygons.len(), "dissolving polygons");

    let result = dissolve_reduce_strategy(polygons);

    debug!(polygon_count = result.0.len(), "dissolve complete");

    Ok(result)
}

#[doc(hidden)]
pub fn dissolve_reduce_strategy(polygons: Vec<Polygon<f64>>) -> MultiPolygon<f64> {
    polygons
        .into_par_iter()
        .map(|p| MultiPolygon::new(vec![p]))
        .reduce(|| MultiPolygon::new(vec![]), |a, b| a.union(&b))
}

#[allow(dead_code)]
#[doc(hidden)]
pub fn dissolve_spatial_reduce_strategy(mut polygons: Vec<Polygon<f64>>) -> MultiPolygon<f64> {
    polygons.sort_by_key(spatial_key);
    dissolve_reduce_strategy(polygons)
}

#[allow(dead_code)]
#[doc(hidden)]
pub fn dissolve_unary_union_strategy(polygons: Vec<Polygon<f64>>) -> MultiPolygon<f64> {
    dissolve_reduce_strategy(polygons)
}

#[allow(dead_code)]
#[doc(hidden)]
pub fn dissolve_raw_unary_union_comparison(polygons: Vec<Polygon<f64>>) -> MultiPolygon<f64> {
    MultiPolygon::new(polygons).union(&MultiPolygon::new(vec![]))
}

#[allow(dead_code)]
fn spatial_key(polygon: &Polygon<f64>) -> u64 {
    let Some(bounds) = polygon.bounding_rect() else {
        return 0;
    };

    let center_x = (bounds.min().x + bounds.max().x) * 0.5;
    let center_y = (bounds.min().y + bounds.max().y) * 0.5;
    morton_key(center_x, center_y)
}

#[allow(dead_code)]
fn morton_key(x: f64, y: f64) -> u64 {
    let xi = quantize_for_morton(x);
    let yi = quantize_for_morton(y);
    interleave_bits(xi) | (interleave_bits(yi) << 1)
}

#[allow(dead_code)]
fn quantize_for_morton(value: f64) -> u32 {
    let normalized = if value.is_finite() {
        ((value + 180.0) / 360.0).clamp(0.0, 1.0)
    } else {
        0.0
    };

    (normalized * f64::from(u16::MAX)).round() as u32
}

#[allow(dead_code)]
fn interleave_bits(mut value: u32) -> u64 {
    value &= 0x0000_ffff;
    let mut spread = u64::from(value);
    spread = (spread | (spread << 16)) & 0x0000_ffff_0000_ffff;
    spread = (spread | (spread << 8)) & 0x00ff_00ff_00ff_00ff;
    spread = (spread | (spread << 4)) & 0x0f0f_0f0f_0f0f_0f0f;
    spread = (spread | (spread << 2)) & 0x3333_3333_3333_3333;
    (spread | (spread << 1)) & 0x5555_5555_5555_5555
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::Area;
    use geo::LineString;

    fn rect(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]),
            vec![],
        )
    }

    fn disjoint_grid_fixture() -> Vec<Polygon<f64>> {
        (0..4)
            .flat_map(|row| {
                (0..4).map(move |col| {
                    let x = f64::from(col) * 2.0;
                    let y = f64::from(row) * 2.0;
                    rect(x, y, x + 1.0, y + 1.0)
                })
            })
            .collect()
    }

    fn overlapping_chain_fixture() -> Vec<Polygon<f64>> {
        (0..12)
            .map(|i| {
                let x = f64::from(i) * 0.75;
                rect(x, 0.0, x + 1.0, 1.0)
            })
            .collect()
    }

    fn refinement_like_fixture() -> Vec<Polygon<f64>> {
        vec![
            rect(0.0, 0.0, 1.0, 1.0),
            rect(1.0, 0.0, 2.0, 1.0),
            rect(2.0, 0.0, 3.0, 1.0),
            rect(0.0, 1.0, 1.0, 2.0),
            rect(1.0, 1.0, 2.0, 2.0),
            rect(2.0, 1.0, 3.0, 2.0),
            rect(3.0, 1.0, 4.0, 2.0),
            rect(1.0, 2.0, 2.0, 3.0),
            rect(2.0, 2.0, 3.0, 3.0),
            rect(3.0, 2.0, 4.0, 3.0),
            rect(4.0, 2.0, 5.0, 3.0),
            rect(2.0, 3.0, 3.0, 4.0),
            rect(3.0, 3.0, 4.0, 4.0),
            rect(4.0, 3.0, 5.0, 4.0),
            rect(5.0, 3.0, 6.0, 4.0),
            rect(3.0, 4.0, 4.0, 5.0),
            rect(4.0, 4.0, 5.0, 5.0),
            rect(5.0, 4.0, 6.0, 5.0),
            rect(4.0, 5.0, 5.0, 6.0),
            rect(5.0, 5.0, 6.0, 6.0),
        ]
    }

    fn assert_same_invariants(left: &MultiPolygon<f64>, right: &MultiPolygon<f64>) {
        assert_eq!(left.0.len(), right.0.len());
        assert!(
            (total_unsigned_area(left) - total_unsigned_area(right)).abs() < 1e-9,
            "area mismatch: left={}, right={}",
            total_unsigned_area(left),
            total_unsigned_area(right)
        );
    }

    fn assert_strategies_match(polygons: Vec<Polygon<f64>>) {
        let reduce = dissolve_reduce_strategy(polygons.clone());
        let spatial = dissolve_spatial_reduce_strategy(polygons.clone());
        let unary = dissolve_unary_union_strategy(polygons);

        assert_same_invariants(&reduce, &spatial);
        assert_same_invariants(&reduce, &unary);
    }

    fn total_unsigned_area(multi_polygon: &MultiPolygon<f64>) -> f64 {
        multi_polygon.0.iter().map(Polygon::unsigned_area).sum()
    }

    #[test]
    fn empty_input_returns_error() {
        let result = dissolve(Vec::new());
        assert!(matches!(result, Err(DissolveError::EmptyInput)));
    }

    #[test]
    fn single_polygon() {
        let poly = rect(0.0, 0.0, 1.0, 1.0);
        let result = dissolve(vec![poly]).unwrap();
        assert_eq!(result.0.len(), 1);
    }

    #[test]
    fn two_overlapping_polygons() {
        let a = rect(0.0, 0.0, 2.0, 2.0);
        let b = rect(1.0, 1.0, 3.0, 3.0);
        let result = dissolve(vec![a, b]).unwrap();
        assert_eq!(result.0.len(), 1);
    }

    #[test]
    fn two_disjoint_polygons() {
        let a = rect(0.0, 0.0, 1.0, 1.0);
        let b = rect(2.0, 2.0, 3.0, 3.0);
        let result = dissolve(vec![a, b]).unwrap();
        assert_eq!(result.0.len(), 2);
    }

    #[test]
    fn three_polygons_chain() {
        let a = rect(0.0, 0.0, 2.0, 1.0);
        let b = rect(1.0, 0.0, 3.0, 1.0);
        let c = rect(2.0, 0.0, 4.0, 1.0);
        let result = dissolve(vec![a, b, c]).unwrap();
        assert_eq!(result.0.len(), 1);
    }

    #[test]
    fn strategies_match_for_disjoint_grid() {
        assert_strategies_match(disjoint_grid_fixture());
    }

    #[test]
    fn strategies_match_for_overlapping_chain() {
        assert_strategies_match(overlapping_chain_fixture());
    }

    #[test]
    fn strategies_match_for_refinement_like_fixture() {
        assert_strategies_match(refinement_like_fixture());
    }
}
