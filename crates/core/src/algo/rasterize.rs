//! Scanline polygon rasterizer.

use geo::{MultiPolygon, Polygon};
use rayon::prelude::*;
use tracing::instrument;

use crate::algo::coord::GridDims;
use crate::algo::geo_transform::GeoTransform;

/// Grid cell count above which rasterization runs scanlines in parallel.
const PARALLEL_THRESHOLD: usize = 100_000;

/// Collect all ring vertices (exterior + interior) from a MultiPolygon in pixel space.
///
/// Each ring is appended as a closed sequence of `(row_f64, col_f64)` pairs,
/// with a sentinel `(f64::NAN, f64::NAN)` between rings so `fill_row` treats
/// them as independent edge lists. The even-odd rule naturally handles holes:
/// both exterior and interior ring edges toggle the inside/outside parity.
fn collect_all_vertices(multi_polygon: &MultiPolygon<f64>, geo: &GeoTransform) -> Vec<(f64, f64)> {
    let mut all_vertices: Vec<(f64, f64)> = Vec::new();

    for polygon in &multi_polygon.0 {
        // Exterior ring
        let ext_verts: Vec<(f64, f64)> = polygon
            .exterior()
            .coords()
            .map(|c| geo.coord_to_pixel_f64(c.x, c.y))
            .collect();
        all_vertices.extend_from_slice(&ext_verts);
        // Sentinel separates rings so edges don't bleed across ring boundaries.
        all_vertices.push((f64::NAN, f64::NAN));

        // Interior rings (holes)
        for hole in polygon.interiors() {
            let hole_verts: Vec<(f64, f64)> = hole
                .coords()
                .map(|c| geo.coord_to_pixel_f64(c.x, c.y))
                .collect();
            all_vertices.extend_from_slice(&hole_verts);
            all_vertices.push((f64::NAN, f64::NAN));
        }
    }

    all_vertices
}

/// Fill a single scanline row using the even-odd rule across multiple rings.
///
/// Vertices are expected in the format produced by [`collect_all_vertices`]:
/// consecutive `(row_f64, col_f64)` pairs per ring, separated by `NAN` sentinels.
/// Edges that span a NAN are skipped, so rings remain independent.
#[inline]
fn fill_row_multi(r: usize, cols: usize, vertices: &[(f64, f64)], row_cells: &mut [bool]) {
    let scan_y = r as f64 + 0.5;
    let mut intersections: Vec<f64> = Vec::new();

    let n = vertices.len();
    for i in 0..n.saturating_sub(1) {
        let (y0, x0) = vertices[i];
        let (y1, x1) = vertices[i + 1];

        // Skip edges that touch a sentinel.
        if y0.is_nan() || x0.is_nan() || y1.is_nan() || x1.is_nan() {
            continue;
        }

        let (lo, hi) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
        if scan_y <= lo || scan_y > hi {
            continue;
        }

        let t = (scan_y - y0) / (y1 - y0);
        let x_intersect = x0 + t * (x1 - x0);
        intersections.push(x_intersect);
    }

    intersections.sort_by(|a, b| a.partial_cmp(b).unwrap());

    for pair in intersections.chunks(2) {
        if pair.len() < 2 {
            break;
        }
        let col_start = ((pair[0] - 0.5).ceil().max(0.0) as usize).min(cols);
        let col_end = ((pair[1] - 0.5).ceil().max(0.0) as usize).min(cols);

        for cell in &mut row_cells[col_start..col_end] {
            *cell = true;
        }
    }
}

/// Rasterize a [`MultiPolygon`] into a flat boolean grid using the even-odd scanline fill rule.
///
/// Returns a `Vec<bool>` of length `dims.rows * dims.cols` where `true` indicates the pixel
/// center falls inside the MultiPolygon (accounting for holes via the even-odd rule).
///
/// The grid is laid out in row-major order: index `r * cols + c` corresponds to
/// pixel `(row=r, col=c)`.
///
/// # Algorithm
///
/// For each scanline row `r`, the scanline Y is `r + 0.5` in pixel-row space.
/// Edges from all rings (exterior and interior) of all component polygons are collected.
/// The even-odd rule naturally handles holes: each ring boundary toggles the parity,
/// so interior ring edges toggle back to "outside".
#[instrument(skip(multi_polygon, geo))]
pub fn rasterize_multi_polygon(
    multi_polygon: &MultiPolygon<f64>,
    geo: &GeoTransform,
    dims: GridDims,
) -> Vec<bool> {
    let rows = dims.rows;
    let cols = dims.cols;
    let mut mask = vec![false; rows * cols];

    let vertices = collect_all_vertices(multi_polygon, geo);

    if vertices.len() < 2 {
        return mask;
    }

    if rows * cols >= PARALLEL_THRESHOLD {
        mask.par_chunks_mut(cols)
            .enumerate()
            .for_each(|(r, row_cells)| {
                fill_row_multi(r, cols, &vertices, row_cells);
            });
    } else {
        for (r, row_cells) in mask.chunks_mut(cols).enumerate() {
            fill_row_multi(r, cols, &vertices, row_cells);
        }
    }

    mask
}

/// Rasterize a single [`Polygon`] into a flat boolean grid using the even-odd scanline fill rule.
///
/// Convenience wrapper around [`rasterize_multi_polygon`] for callers that already have
/// a single polygon. Interior rings (holes) are correctly excluded.
///
/// Returns a `Vec<bool>` of length `dims.rows * dims.cols` where `true` indicates the pixel
/// center falls inside the polygon (outside any holes).
///
/// The grid is laid out in row-major order: index `r * cols + c` corresponds to
/// pixel `(row=r, col=c)`.
#[instrument(skip(polygon, geo))]
pub fn rasterize_polygon(polygon: &Polygon<f64>, geo: &GeoTransform, dims: GridDims) -> Vec<bool> {
    rasterize_multi_polygon(&MultiPolygon::new(vec![polygon.clone()]), geo, dims)
}

#[cfg(test)]
mod tests {
    use geo::{LineString, MultiPolygon, Polygon, polygon};

    use super::*;
    use crate::algo::coord::{GeoCoord, GridDims};

    /// Simple 1-unit-per-pixel GeoTransform.
    ///
    /// With this transform:
    /// - pixel (row=r, col=c) center is at x = c + 0.5, y = -(r + 0.5)
    /// - pixel (row=r, col=c) covers x in [c, c+1], y in [-(r+1), -r]
    fn simple_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    #[test]
    fn small_triangle() {
        // Triangle with vertices at (0,0), (3,0), (0,-2) in geo coords.
        // Covers upper-left corner of a 4x4 grid.
        let geo = simple_geo();
        let poly = polygon![
            (x: 0.0, y: 0.0),
            (x: 3.0, y: 0.0),
            (x: 0.0, y: -2.0),
            (x: 0.0, y: 0.0),
        ];
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(4, 4));

        // Row 0 (y=-0.5): scanline intersects x in [0, 2.25) → cols 0,1
        assert!(mask[0 * 4 + 0], "row=0, col=0 should be filled");
        assert!(mask[0 * 4 + 1], "row=0, col=1 should be filled");
        // Row 1 (y=-1.5): scanline intersects x in [0, 1.5) → col 0 only (ceil(0)..ceil(1.5) = 0..2)
        assert!(mask[1 * 4 + 0], "row=1, col=0 should be filled");
        // Col 3 should never be filled
        assert!(!mask[0 * 4 + 3], "row=0, col=3 should not be filled");
        assert!(!mask[1 * 4 + 3], "row=1, col=3 should not be filled");
        // Row 3 is outside the triangle
        assert!(!mask[3 * 4 + 0], "row=3, col=0 should not be filled");
    }

    #[test]
    fn full_coverage() {
        // Polygon covering entire 3x3 tile: x in [0,3], y in [0,-3].
        let geo = simple_geo();
        let poly = polygon![
            (x: 0.0, y: 0.0),
            (x: 3.0, y: 0.0),
            (x: 3.0, y: -3.0),
            (x: 0.0, y: -3.0),
            (x: 0.0, y: 0.0),
        ];
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(3, 3));
        assert!(mask.iter().all(|&v| v), "all cells should be true");
    }

    #[test]
    fn polygon_outside_tile() {
        // Polygon entirely to the right of a 3x3 tile.
        let geo = simple_geo();
        let poly = polygon![
            (x: 10.0, y: 0.0),
            (x: 13.0, y: 0.0),
            (x: 13.0, y: -3.0),
            (x: 10.0, y: -3.0),
            (x: 10.0, y: 0.0),
        ];
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(3, 3));
        assert!(mask.iter().all(|&v| !v), "all cells should be false");
    }

    #[test]
    fn partial_overlap() {
        // Polygon covering x in [-1,2], y in [0,-2] — overlaps first two cols of 3x3.
        let geo = simple_geo();
        let poly = polygon![
            (x: -1.0, y: 0.0),
            (x: 2.0, y: 0.0),
            (x: 2.0, y: -2.0),
            (x: -1.0, y: -2.0),
            (x: -1.0, y: 0.0),
        ];
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(3, 3));

        // Rows 0 and 1, cols 0 and 1 should be filled.
        for r in 0..2usize {
            for c in 0..2usize {
                assert!(mask[r * 3 + c], "row={r}, col={c} should be filled");
            }
            // Col 2 should not be filled.
            assert!(!mask[r * 3 + 2], "row={r}, col=2 should not be filled");
        }
        // Row 2 should not be filled.
        for c in 0..3usize {
            assert!(!mask[2 * 3 + c], "row=2, col={c} should not be filled");
        }
    }

    #[test]
    fn single_pixel() {
        // Polygon tightly around pixel center at (row=1, col=1): center is x=1.5, y=-1.5.
        let geo = simple_geo();
        let poly = polygon![
            (x: 1.1, y: -1.1),
            (x: 1.9, y: -1.1),
            (x: 1.9, y: -1.9),
            (x: 1.1, y: -1.9),
            (x: 1.1, y: -1.1),
        ];
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(3, 3));

        assert!(mask[1 * 3 + 1], "center pixel (1,1) should be filled");
        let filled_count = mask.iter().filter(|&&v| v).count();
        assert_eq!(filled_count, 1, "exactly one pixel should be filled");
    }

    #[test]
    fn l_shape() {
        // L-shaped polygon (non-convex) in a 4x4 grid.
        // Covers rows 0-3, col 0 and rows 2-3, cols 0-2 (an upside-down L).
        //
        // Geo coords (y is negative-down):
        //   x in [0,1] for the vertical bar (rows 0-3)
        //   x in [0,3] for the horizontal bar (rows 2-3)
        let geo = simple_geo();
        let poly = polygon![
            (x: 0.0, y: 0.0),
            (x: 1.0, y: 0.0),
            (x: 1.0, y: -2.0),
            (x: 3.0, y: -2.0),
            (x: 3.0, y: -4.0),
            (x: 0.0, y: -4.0),
            (x: 0.0, y: 0.0),
        ];
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(4, 4));

        // Vertical bar: rows 0-1, col 0
        assert!(
            mask[0 * 4 + 0],
            "row=0, col=0 should be filled (vertical bar)"
        );
        assert!(
            mask[1 * 4 + 0],
            "row=1, col=0 should be filled (vertical bar)"
        );
        // Horizontal bar: rows 2-3, cols 0-2
        for r in 2..4usize {
            for c in 0..3usize {
                assert!(mask[r * 4 + c], "row={r}, col={c} should be filled (L bar)");
            }
            // Col 3 outside the L
            assert!(!mask[r * 4 + 3], "row={r}, col=3 should not be filled");
        }
        // Rows 0-1, cols 1-3 outside the vertical bar
        for r in 0..2usize {
            for c in 1..4usize {
                assert!(!mask[r * 4 + c], "row={r}, col={c} should not be filled");
            }
        }
    }

    #[test]
    fn large_grid_parallel_path() {
        // 400x400 = 160,000 cells, above PARALLEL_THRESHOLD (100,000)
        let geo = simple_geo();
        let poly = polygon![
            (x: 0.0, y: 0.0),
            (x: 400.0, y: 0.0),
            (x: 400.0, y: -400.0),
            (x: 0.0, y: -400.0),
            (x: 0.0, y: 0.0),
        ];
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(400, 400));
        assert!(
            mask.iter().all(|&v| v),
            "all cells should be true for full-coverage polygon"
        );
        assert_eq!(mask.len(), 160_000);
    }

    #[test]
    fn polygon_with_hole() {
        // 4x4 square with a 2x2 hole in the center.
        // Exterior: x in [0,4], y in [0,-4]
        // Hole:     x in [1,3], y in [-1,-3]  (pixel rows 1-2, cols 1-2)
        let geo = simple_geo();
        let exterior = LineString::from(vec![
            (0.0_f64, 0.0_f64),
            (4.0, 0.0),
            (4.0, -4.0),
            (0.0, -4.0),
            (0.0, 0.0),
        ]);
        let hole = LineString::from(vec![
            (1.0_f64, -1.0_f64),
            (3.0, -1.0),
            (3.0, -3.0),
            (1.0, -3.0),
            (1.0, -1.0),
        ]);
        let poly = Polygon::new(exterior, vec![hole]);
        let mask = rasterize_polygon(&poly, &geo, GridDims::new(4, 4));

        // Hole pixels (rows 1-2, cols 1-2) must be false.
        for r in 1..3usize {
            for c in 1..3usize {
                assert!(
                    !mask[r * 4 + c],
                    "hole pixel row={r}, col={c} should be false"
                );
            }
        }
        // Corner pixels (row 0 / row 3, cols all; rows 1-2 col 0 / col 3) must be true.
        for c in 0..4usize {
            assert!(mask[0 * 4 + c], "top row pixel col={c} should be true");
            assert!(mask[3 * 4 + c], "bottom row pixel col={c} should be true");
        }
        for r in 1..3usize {
            assert!(mask[r * 4 + 0], "left col pixel row={r} should be true");
            assert!(mask[r * 4 + 3], "right col pixel row={r} should be true");
        }
    }

    #[test]
    fn multi_polygon_two_components() {
        // Two non-overlapping 1x1 rectangles in a 4x4 grid.
        // Component 1: x in [0,1], y in [0,-1]  → pixel (row=0, col=0)
        // Component 2: x in [3,4], y in [-3,-4] → pixel (row=3, col=3)
        let geo = simple_geo();
        let p1 = Polygon::new(
            LineString::from(vec![
                (0.0_f64, 0.0_f64),
                (1.0, 0.0),
                (1.0, -1.0),
                (0.0, -1.0),
                (0.0, 0.0),
            ]),
            vec![],
        );
        let p2 = Polygon::new(
            LineString::from(vec![
                (3.0_f64, -3.0_f64),
                (4.0, -3.0),
                (4.0, -4.0),
                (3.0, -4.0),
                (3.0, -3.0),
            ]),
            vec![],
        );
        let mp = MultiPolygon::new(vec![p1, p2]);
        let mask = rasterize_multi_polygon(&mp, &geo, GridDims::new(4, 4));

        assert!(mask[0 * 4 + 0], "component 1 pixel (0,0) should be true");
        assert!(mask[3 * 4 + 3], "component 2 pixel (3,3) should be true");

        // All other pixels should be false.
        let filled: Vec<usize> = mask
            .iter()
            .enumerate()
            .filter(|&(_, v)| *v)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(filled, vec![0, 15], "only pixels 0 and 15 should be filled");
    }

    #[test]
    fn multi_polygon_with_hole() {
        // MultiPolygon with one 4x4 square that has a 2x2 hole, plus a separate 1x1 square.
        // Holed square: x in [0,4], y in [0,-4] with hole x in [1,3], y in [-1,-3]
        // Extra square: x in [5,6], y in [0,-1] → pixel (row=0, col=5) — needs 4x6 grid
        let geo = simple_geo();
        let holed = Polygon::new(
            LineString::from(vec![
                (0.0_f64, 0.0_f64),
                (4.0, 0.0),
                (4.0, -4.0),
                (0.0, -4.0),
                (0.0, 0.0),
            ]),
            vec![LineString::from(vec![
                (1.0_f64, -1.0_f64),
                (3.0, -1.0),
                (3.0, -3.0),
                (1.0, -3.0),
                (1.0, -1.0),
            ])],
        );
        let extra = Polygon::new(
            LineString::from(vec![
                (5.0_f64, 0.0_f64),
                (6.0, 0.0),
                (6.0, -1.0),
                (5.0, -1.0),
                (5.0, 0.0),
            ]),
            vec![],
        );
        let mp = MultiPolygon::new(vec![holed, extra]);
        let mask = rasterize_multi_polygon(&mp, &geo, GridDims::new(4, 6));

        // Hole pixels (rows 1-2, cols 1-2) must be false.
        for r in 1..3usize {
            for c in 1..3usize {
                assert!(
                    !mask[r * 6 + c],
                    "hole pixel row={r}, col={c} should be false"
                );
            }
        }
        // Extra component pixel (row=0, col=5) must be true.
        assert!(
            mask[0 * 6 + 5],
            "extra component pixel (0,5) should be true"
        );
    }
}
