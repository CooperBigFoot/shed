//! Binary mask to polygon conversion.

use std::collections::HashMap;

use geo::{Coord, LineString, Polygon};
use tracing::{debug, instrument};

use crate::algo::catchment_mask::CatchmentMask;
use crate::algo::coord::GridCoord;
use crate::algo::geo_transform::GeoTransform;

/// Convert a binary catchment mask to a polygon in geographic coordinates.
///
/// Returns `None` when the mask contains no `true` cells.
#[instrument(skip(mask, geo))]
pub fn polygonize(mask: &CatchmentMask, geo: &GeoTransform) -> Option<Polygon<f64>> {
    let edges = extract_edges(mask);
    debug!(edge_count = edges.len(), "extracted boundary edges");

    if edges.is_empty() {
        return None;
    }

    let rings = assemble_rings(edges);
    debug!(ring_count = rings.len(), "assembled rings");

    // Find the exterior ring (largest absolute area).
    let areas: Vec<f64> = rings.iter().map(|r| shoelace_area(r)).collect();
    let exterior_idx = areas
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.abs().partial_cmp(&b.abs()).unwrap())
        .map(|(i, _)| i)?;

    // Build exterior ring — must have negative signed area (CW in pixel coords
    // → CCW in geographic coords after y-flip from negative pixel_height).
    let mut exterior_ring = rings[exterior_idx].clone();
    if shoelace_area(&exterior_ring) > 0.0 {
        exterior_ring.reverse();
    }

    // Build hole rings — must have positive signed area (CCW in pixel coords
    // → CW in geographic coords after y-flip from negative pixel_height).
    let hole_rings: Vec<Vec<(usize, usize)>> = rings
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != exterior_idx)
        .map(|(_, r)| {
            let mut hole = r.clone();
            if shoelace_area(&hole) < 0.0 {
                hole.reverse();
            }
            hole
        })
        .collect();

    let exterior_coords = to_geo_coords(&exterior_ring, geo);
    let exterior_ls = LineString::from(exterior_coords);

    let interior_ls: Vec<LineString<f64>> = hole_rings
        .iter()
        .map(|r| LineString::from(to_geo_coords(r, geo)))
        .collect();

    Some(Polygon::new(exterior_ls, interior_ls))
}

/// Extract directed boundary edges from a binary mask.
///
/// For each `true` pixel, emits one edge per side that borders a `false` cell
/// or the mask boundary. Edges are oriented so the `true` region is on the
/// right side when walking along the edge direction.
fn extract_edges(mask: &CatchmentMask) -> Vec<((usize, usize), (usize, usize))> {
    let rows = mask.rows();
    let cols = mask.cols();
    let mut edges = Vec::new();

    for r in 0..rows {
        for c in 0..cols {
            if !mask.contains(GridCoord::new(r, c)) {
                continue;
            }

            // Top edge: neighbor above is false/OOB → edge (r,c) → (r,c+1)
            if r == 0 || !mask.contains(GridCoord::new(r - 1, c)) {
                edges.push(((r, c), (r, c + 1)));
            }

            // Bottom edge: neighbor below is false/OOB → edge (r+1,c+1) → (r+1,c)
            if r + 1 == rows || !mask.contains(GridCoord::new(r + 1, c)) {
                edges.push(((r + 1, c + 1), (r + 1, c)));
            }

            // Left edge: neighbor left is false/OOB → edge (r+1,c) → (r,c)
            if c == 0 || !mask.contains(GridCoord::new(r, c - 1)) {
                edges.push(((r + 1, c), (r, c)));
            }

            // Right edge: neighbor right is false/OOB → edge (r,c+1) → (r+1,c+1)
            if c + 1 == cols || !mask.contains(GridCoord::new(r, c + 1)) {
                edges.push(((r, c + 1), (r + 1, c + 1)));
            }
        }
    }

    edges
}

/// Chain directed edges into closed rings.
///
/// Builds a map from each start point to its end points, then follows chains
/// until each ring closes back on itself.
fn assemble_rings(edges: Vec<((usize, usize), (usize, usize))>) -> Vec<Vec<(usize, usize)>> {
    // Map: start → list of ends (multiple edges can share a start in complex topologies).
    let mut adjacency: HashMap<(usize, usize), Vec<(usize, usize)>> = HashMap::new();
    for (start, end) in edges {
        adjacency.entry(start).or_default().push(end);
    }

    let mut rings = Vec::new();

    while let Some(&origin) = adjacency.keys().next() {
        let mut ring = vec![origin];
        let mut current = origin;

        loop {
            let ends = adjacency.get_mut(&current).unwrap();
            let next = ends.pop().unwrap();
            if ends.is_empty() {
                adjacency.remove(&current);
            }
            if next == origin {
                // Close the ring by repeating the first point.
                ring.push(origin);
                break;
            }
            ring.push(next);
            current = next;
        }

        rings.push(ring);
    }

    rings
}

/// Compute the signed area of a ring using the shoelace formula.
///
/// Positive area means CCW orientation in standard (y-up) coordinates.
/// In pixel coordinates (y-down), positive area means CW traversal.
fn shoelace_area(ring: &[(usize, usize)]) -> f64 {
    let n = ring.len();
    if n < 3 {
        return 0.0;
    }
    let mut area = 0.0;
    for i in 0..n - 1 {
        let (r0, c0) = ring[i];
        let (r1, c1) = ring[i + 1];
        area += (c0 as f64) * (r1 as f64) - (c1 as f64) * (r0 as f64);
    }
    area / 2.0
}

/// Convert pixel-corner coordinates to geographic coordinates using the geo-transform.
///
/// Formula: `x = origin_x + c * pixel_width`, `y = origin_y + r * pixel_height`.
/// No +0.5 offset — these are corners, not centers.
fn to_geo_coords(ring: &[(usize, usize)], geo: &GeoTransform) -> Vec<Coord<f64>> {
    ring.iter()
        .map(|&(r, c)| Coord {
            x: geo.origin_x() + c as f64 * geo.pixel_width(),
            y: geo.origin_y() + r as f64 * geo.pixel_height(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::catchment_mask::CatchmentMask;
    use crate::algo::coord::{GeoCoord, GridDims};

    fn simple_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    #[test]
    fn empty_mask_returns_none() {
        let mask = CatchmentMask::new(vec![false; 4], GridDims::new(2, 2));
        assert!(polygonize(&mask, &simple_geo()).is_none());
    }

    #[test]
    fn single_pixel() {
        // One true cell at (0,0) in a 1x1 grid.
        let mask = CatchmentMask::new(vec![true], GridDims::new(1, 1));
        let geo = simple_geo(); // origin=(0,0), pw=1, ph=-1
        let poly = polygonize(&mask, &geo).expect("should produce a polygon");

        // Pixel (0,0) corners in pixel space: (r,c) ∈ {(0,0),(0,1),(1,0),(1,1)}
        // In geo coords: x = c*1, y = r*(-1)
        //   (0,0) → (0,0), (0,1) → (1,0), (1,0) → (0,-1), (1,1) → (1,-1)
        let exterior = poly.exterior();
        let coords: Vec<(f64, f64)> = exterior.coords().map(|c| (c.x, c.y)).collect();

        // Must be a closed ring (first == last) with 5 points (4 corners + repeat).
        assert_eq!(coords.len(), 5, "expected 4 corners + close: {coords:?}");
        assert_eq!(coords[0], coords[coords.len() - 1], "ring must be closed");

        // All four corners must appear.
        let expected_corners = [(0.0, 0.0), (1.0, 0.0), (1.0, -1.0), (0.0, -1.0)];
        for corner in &expected_corners {
            assert!(
                coords
                    .iter()
                    .any(|c| (c.0 - corner.0).abs() < 1e-10 && (c.1 - corner.1).abs() < 1e-10),
                "missing corner {corner:?} in {coords:?}"
            );
        }

        assert!(
            poly.interiors().is_empty(),
            "single pixel should have no holes"
        );
    }

    #[test]
    fn two_by_two_block() {
        // 2x2 all true → one rectangle.
        let mask = CatchmentMask::new(vec![true; 4], GridDims::new(2, 2));
        let poly = polygonize(&mask, &simple_geo()).expect("should produce a polygon");

        let exterior = poly.exterior();
        let coords: Vec<(f64, f64)> = exterior.coords().map(|c| (c.x, c.y)).collect();

        // Bounding box corners: (0,0),(2,0),(2,-2),(0,-2)
        let expected_corners = [(0.0, 0.0), (2.0, 0.0), (2.0, -2.0), (0.0, -2.0)];
        for corner in &expected_corners {
            assert!(
                coords
                    .iter()
                    .any(|c| (c.0 - corner.0).abs() < 1e-10 && (c.1 - corner.1).abs() < 1e-10),
                "missing corner {corner:?} in {coords:?}"
            );
        }

        assert!(poly.interiors().is_empty());
    }

    #[test]
    fn l_shape() {
        // L-shape: top-left 2x1 + bottom-left 1x2 in a 2x2 grid.
        // true  false
        // true  true
        #[rustfmt::skip]
        let data = vec![
            true,  false,
            true,  true,
        ];
        let mask = CatchmentMask::new(data, GridDims::new(2, 2));
        let poly = polygonize(&mask, &simple_geo()).expect("should produce a polygon");

        // L-shape has 6 distinct corner points.
        let exterior = poly.exterior();
        // Closed ring: num unique points = len - 1.
        assert!(
            exterior.coords().count() >= 7,
            "L-shape ring needs 6+ corners"
        );
        assert!(poly.interiors().is_empty());
    }

    #[test]
    fn donut_with_hole() {
        // 3x3 all true except center cell (1,1).
        #[rustfmt::skip]
        let data = vec![
            true, true,  true,
            true, false, true,
            true, true,  true,
        ];
        let mask = CatchmentMask::new(data, GridDims::new(3, 3));
        let poly = polygonize(&mask, &simple_geo()).expect("should produce a polygon");

        assert_eq!(
            poly.interiors().len(),
            1,
            "donut must have exactly one hole"
        );
    }

    #[test]
    fn full_tile() {
        // Entire 3x3 mask true → single outer rectangle, no holes.
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        let poly = polygonize(&mask, &simple_geo()).expect("should produce a polygon");

        let expected_corners = [(0.0, 0.0), (3.0, 0.0), (3.0, -3.0), (0.0, -3.0)];
        let exterior = poly.exterior();
        let coords: Vec<(f64, f64)> = exterior.coords().map(|c| (c.x, c.y)).collect();
        for corner in &expected_corners {
            assert!(
                coords
                    .iter()
                    .any(|c| (c.0 - corner.0).abs() < 1e-10 && (c.1 - corner.1).abs() < 1e-10),
                "missing corner {corner:?}"
            );
        }
        assert!(poly.interiors().is_empty());
    }

    #[test]
    fn geographic_coordinates() {
        // Single true cell with a non-trivial GeoTransform.
        // origin=(10, 50), pw=0.5, ph=-0.5
        let geo = GeoTransform::new(GeoCoord::new(10.0, 50.0), 0.5, -0.5);
        let mask = CatchmentMask::new(vec![true], GridDims::new(1, 1));
        let poly = polygonize(&mask, &geo).expect("should produce a polygon");

        // Pixel (0,0) corners in geo:
        //   (r=0,c=0) → x=10+0*0.5=10,   y=50+0*(-0.5)=50
        //   (r=0,c=1) → x=10+1*0.5=10.5, y=50
        //   (r=1,c=1) → x=10.5,           y=50+1*(-0.5)=49.5
        //   (r=1,c=0) → x=10,             y=49.5
        let exterior = poly.exterior();
        let coords: Vec<(f64, f64)> = exterior.coords().map(|c| (c.x, c.y)).collect();

        let expected = [(10.0, 50.0), (10.5, 50.0), (10.5, 49.5), (10.0, 49.5)];
        for corner in &expected {
            assert!(
                coords
                    .iter()
                    .any(|c| (c.0 - corner.0).abs() < 1e-10 && (c.1 - corner.1).abs() < 1e-10),
                "missing corner {corner:?} in {coords:?}"
            );
        }
    }

    #[test]
    fn exterior_ring_is_ccw_in_geographic_space() {
        use geo::algorithm::Area;
        // 2x2 all-true mask with negative pixel_height (standard geo raster).
        let mask = CatchmentMask::new(vec![true; 4], GridDims::new(2, 2));
        let geo = simple_geo(); // origin=(0,0), pw=1, ph=-1
        let poly = polygonize(&mask, &geo).expect("should produce a polygon");

        // geo::Area::signed_area() returns positive for CCW exterior (y-up convention).
        let signed = poly.signed_area();
        assert!(
            signed > 0.0,
            "polygon should have positive signed_area (CCW exterior), got {signed}"
        );
    }

    #[test]
    fn geodesic_area_is_not_earth_complement() {
        use geo::algorithm::GeodesicArea;
        // Small mask with realistic 0.001° pixels near the equator.
        let geo = GeoTransform::new(GeoCoord::new(8.0, 47.0), 0.001, -0.001);
        let mask = CatchmentMask::new(vec![true; 4], GridDims::new(2, 2));
        let poly = polygonize(&mask, &geo).expect("should produce a polygon");

        let area = poly.geodesic_area_signed();
        // Area should be positive (CCW exterior) and tiny — a 0.002° × 0.002° box.
        assert!(area > 0.0, "geodesic area should be positive, got {area}");
        assert!(
            area < 1_000_000.0, // well under 1 km² in m²
            "geodesic area should be tiny, not Earth's complement (~510M km²), got {area} m²"
        );
    }
}
