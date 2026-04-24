//! Dissolve a set of polygons into a single multi-polygon via iterative union.

use geo::{BooleanOps, MultiPolygon, Polygon};
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

    let result = polygons
        .into_par_iter()
        .map(|p| MultiPolygon::new(vec![p]))
        .reduce(|| MultiPolygon::new(vec![]), |a, b| a.union(&b));

    debug!(polygon_count = result.0.len(), "dissolve complete");

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::LineString;

    fn rect(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]),
            vec![],
        )
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
}
