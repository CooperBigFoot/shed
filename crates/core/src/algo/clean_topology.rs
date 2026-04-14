//! Clean topological artifacts from multi-polygon geometries via buffer-unbuffer.

use std::cmp::Ordering;

use geo::{Area, BooleanOps, Coord, LineString, MultiPolygon, Polygon};
use rayon::prelude::*;
use tracing::{debug, instrument};

use crate::algo::clean_epsilon::{CleanEpsilon, DEFAULT_CLEANING_EPSILON};

/// Mitre-join denominator threshold. When `1 + cos(angle)` falls below this,
/// switch from mitre to bevel join to prevent spikes. Corresponds to a mitre
/// limit of ~5.0.
const MITRE_DENOM_THRESHOLD: f64 = 0.08;

/// Distance threshold for deduplicating consecutive vertices.
const DEDUP_THRESHOLD: f64 = 1e-7;

/// Minimum number of vertices for a valid ring (triangle).
const MIN_RING_VERTICES: usize = 3;

/// Clean topological artifacts by buffering outward then inward.
///
/// Applies a positive buffer of `epsilon` to close slivers and fill
/// micro-gaps, then a negative buffer of the same magnitude to restore
/// the original boundary position. Self-intersections introduced by
/// concave offsets are resolved via boolean self-union.
#[instrument(skip(geom))]
pub fn clean_topology(geom: MultiPolygon<f64>, epsilon: CleanEpsilon) -> MultiPolygon<f64> {
    let eps = epsilon.as_f64();
    debug!(
        epsilon = eps,
        polygon_count = geom.0.len(),
        "cleaning topology"
    );
    let expanded = buffer_multi_polygon(&geom, eps);
    let result = buffer_multi_polygon(&expanded, -eps);
    debug!(polygon_count = result.0.len(), "topology cleaned");
    result
}

/// Offset each polygon independently, then union all results.
fn buffer_multi_polygon(mp: &MultiPolygon<f64>, distance: f64) -> MultiPolygon<f64> {
    let buffered: Vec<Polygon<f64>> =
        mp.0.iter()
            .filter_map(|p| offset_polygon(p, distance))
            .collect();

    if buffered.is_empty() {
        return MultiPolygon::new(vec![]);
    }

    // Union all offset polygons together
    buffered
        .par_iter()
        .map(|p| MultiPolygon::new(vec![p.clone()]))
        .reduce(|| MultiPolygon::new(vec![]), |a, b| a.union(&b))
}

/// Offset exterior and interior rings, validate result has positive area.
fn offset_polygon(poly: &Polygon<f64>, distance: f64) -> Option<Polygon<f64>> {
    let exterior_coords = offset_ring(poly.exterior().0.as_slice(), distance);
    if exterior_coords.len() < MIN_RING_VERTICES + 1 {
        return None; // Ring collapsed
    }
    let exterior = LineString::from(exterior_coords);

    let interiors: Vec<LineString<f64>> = poly
        .interiors()
        .iter()
        .filter_map(|ring| {
            let coords = offset_ring(ring.0.as_slice(), distance);
            if coords.len() > MIN_RING_VERTICES {
                Some(LineString::from(coords))
            } else {
                None
            }
        })
        .collect();

    let result = Polygon::new(exterior, interiors);

    // Self-union to resolve any self-intersections from concave offsets
    let cleaned = MultiPolygon::new(vec![result]).union(&MultiPolygon::new(vec![]));

    // Take the largest polygon from the union result
    cleaned
        .0
        .into_iter()
        .max_by(|a, b| {
            a.unsigned_area()
                .partial_cmp(&b.unsigned_area())
                .unwrap_or(Ordering::Equal)
        })
        .filter(|p| p.unsigned_area() > 0.0)
}

/// Offset a coordinate ring by `distance` using mitre/bevel joins.
///
/// Right-hand normal `(dy, -dx)/len` means:
/// - On CCW exterior rings → normals point outward → positive distance expands
/// - On CW hole rings → normals point inward → positive distance shrinks hole
fn offset_ring(coords: &[Coord<f64>], distance: f64) -> Vec<Coord<f64>> {
    // Need at least 3 unique vertices (4 coords counting closure)
    if coords.len() < 4 {
        return vec![];
    }

    // Work with unclosed ring
    let n = coords.len() - 1; // drop the closing duplicate
    let mut result = Vec::with_capacity(n * 2);

    for i in 0..n {
        let prev = coords[(i + n - 1) % n];
        let curr = coords[i];
        let next = coords[(i + 1) % n];

        let n1 = match edge_normal(prev, curr) {
            Some(n) => n,
            None => continue,
        };
        let n2 = match edge_normal(curr, next) {
            Some(n) => n,
            None => continue,
        };

        let cos_angle = n1.x * n2.x + n1.y * n2.y;
        let denom = 1.0 + cos_angle;

        if denom < MITRE_DENOM_THRESHOLD {
            // Bevel: emit two offset points
            result.push(Coord {
                x: curr.x + distance * n1.x,
                y: curr.y + distance * n1.y,
            });
            result.push(Coord {
                x: curr.x + distance * n2.x,
                y: curr.y + distance * n2.y,
            });
        } else {
            // Mitre: single offset point
            let mx = (n1.x + n2.x) / denom;
            let my = (n1.y + n2.y) / denom;
            result.push(Coord {
                x: curr.x + distance * mx,
                y: curr.y + distance * my,
            });
        }
    }

    let mut deduped = dedup_ring(&result);

    // Close the ring
    if deduped.len() >= MIN_RING_VERTICES {
        deduped.push(deduped[0]);
    }

    deduped
}

/// Remove near-duplicate consecutive vertices.
fn dedup_ring(coords: &[Coord<f64>]) -> Vec<Coord<f64>> {
    if coords.is_empty() {
        return vec![];
    }

    let mut result = vec![coords[0]];
    for c in coords.iter().skip(1) {
        let last = result.last().unwrap();
        let dx = c.x - last.x;
        let dy = c.y - last.y;
        if dx * dx + dy * dy > DEDUP_THRESHOLD * DEDUP_THRESHOLD {
            result.push(*c);
        }
    }
    result
}

/// Compute the right-hand unit normal of the directed edge from `a` to `b`.
///
/// Returns `(dy, -dx) / length`. Returns `None` for degenerate (zero-length) edges.
///
/// Using the right-hand normal means:
/// - On CCW exterior rings → normals point outward → positive distance expands
/// - On CW hole rings → normals point inward → positive distance shrinks hole
fn edge_normal(a: Coord<f64>, b: Coord<f64>) -> Option<Coord<f64>> {
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    let len = (dx * dx + dy * dy).sqrt();
    if len < 1e-15 {
        return None;
    }
    Some(Coord {
        x: dy / len,
        y: -dx / len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit_square() -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            vec![],
        )
    }

    #[test]
    fn edge_normal_horizontal() {
        // Edge (0,0)->(1,0): dx=1, dy=0 → right-hand normal = (dy, -dx)/len = (0, -1)
        let a = Coord { x: 0.0, y: 0.0 };
        let b = Coord { x: 1.0, y: 0.0 };
        let n = edge_normal(a, b).unwrap();
        assert!((n.x - 0.0).abs() < 1e-10, "expected nx=0, got {}", n.x);
        assert!((n.y - (-1.0)).abs() < 1e-10, "expected ny=-1, got {}", n.y);
    }

    #[test]
    fn edge_normal_vertical() {
        // Edge (0,0)->(0,1): dx=0, dy=1 → right-hand normal = (dy, -dx)/len = (1, 0)
        let a = Coord { x: 0.0, y: 0.0 };
        let b = Coord { x: 0.0, y: 1.0 };
        let n = edge_normal(a, b).unwrap();
        assert!((n.x - 1.0).abs() < 1e-10, "expected nx=1, got {}", n.x);
        assert!((n.y - 0.0).abs() < 1e-10, "expected ny=0, got {}", n.y);
    }

    #[test]
    fn edge_normal_degenerate() {
        let a = Coord { x: 0.0, y: 0.0 };
        let b = Coord { x: 0.0, y: 0.0 };
        assert!(edge_normal(a, b).is_none());
    }

    #[test]
    fn dedup_removes_duplicates() {
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord {
                x: 0.0 + 1e-10,
                y: 0.0,
            }, // near-duplicate, within DEDUP_THRESHOLD
            Coord { x: 1.0, y: 0.0 },
            Coord { x: 1.0, y: 1.0 },
        ];
        let result = dedup_ring(&coords);
        // The near-duplicate should be removed
        assert_eq!(
            result.len(),
            3,
            "expected 3 unique vertices, got {}",
            result.len()
        );
        assert!((result[0].x - 0.0).abs() < 1e-10);
        assert!((result[1].x - 1.0).abs() < 1e-10);
        assert!((result[2].x - 1.0).abs() < 1e-10);
    }

    #[test]
    fn offset_ring_square_expands() {
        let square = unit_square();
        let coords: Vec<Coord<f64>> = square.exterior().0.clone();
        let offset_coords = offset_ring(&coords, 0.1);

        assert!(
            offset_coords.len() >= MIN_RING_VERTICES + 1,
            "offset ring should have at least {} coords",
            MIN_RING_VERTICES + 1
        );

        let offset_poly = Polygon::new(LineString::from(offset_coords), vec![]);
        let original_area = square.unsigned_area();
        let offset_area = offset_poly.unsigned_area();

        assert!(
            offset_area > original_area,
            "offset area {} should be larger than original area {}",
            offset_area,
            original_area
        );
    }

    #[test]
    fn offset_ring_too_few_coords() {
        // Ring with < 4 coords (only 3 — no closing vertex)
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 1.0, y: 0.0 },
            Coord { x: 0.5, y: 1.0 },
        ];
        let result = offset_ring(&coords, 0.1);
        assert!(
            result.is_empty(),
            "expected empty result for ring with < 4 coords"
        );
    }

    #[test]
    fn clean_topology_preserves_shape() {
        let poly = unit_square();
        let mp = MultiPolygon::new(vec![poly.clone()]);
        let original_area = poly.unsigned_area();

        let cleaned = clean_topology(mp, DEFAULT_CLEANING_EPSILON);

        assert!(!cleaned.0.is_empty(), "result should not be empty");
        let cleaned_area: f64 = cleaned.0.iter().map(|p| p.unsigned_area()).sum();

        // Area should be within 1% of original after buffer-unbuffer with tiny epsilon
        let ratio = (cleaned_area - original_area).abs() / original_area;
        assert!(
            ratio < 0.01,
            "area changed by {:.2}%, expected < 1%",
            ratio * 100.0
        );
    }

    #[test]
    fn clean_topology_empty() {
        let mp: MultiPolygon<f64> = MultiPolygon::new(vec![]);
        let result = clean_topology(mp, DEFAULT_CLEANING_EPSILON);
        assert!(result.0.is_empty(), "empty input should yield empty output");
    }

    #[test]
    fn clean_topology_closes_sliver() {
        // Two rectangles with a tiny gap (0.0001 degrees wide)
        let rect1 = Polygon::new(
            LineString::from(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            vec![],
        );
        let rect2 = Polygon::new(
            LineString::from(vec![
                (1.0001, 0.0),
                (2.0, 0.0),
                (2.0, 1.0),
                (1.0001, 1.0),
                (1.0001, 0.0),
            ]),
            vec![],
        );

        let mp = MultiPolygon::new(vec![rect1, rect2]);
        assert_eq!(mp.0.len(), 2, "input should have 2 polygons");

        // Use epsilon larger than the gap (0.001 > 0.0001)
        let cleaned = clean_topology(mp, CleanEpsilon::new(0.001));

        // The gap should be closed — result should be a single merged polygon
        assert_eq!(
            cleaned.0.len(),
            1,
            "gap should be closed, expected 1 polygon but got {}",
            cleaned.0.len()
        );
    }
}
