//! Scanline polygon rasterizer.

use geo::Polygon;
use rayon::prelude::*;
use tracing::instrument;

use crate::algo::coord::GridDims;
use crate::algo::geo_transform::GeoTransform;

/// Grid cell count above which rasterization runs scanlines in parallel.
const PARALLEL_THRESHOLD: usize = 100_000;

/// Fill a single scanline row using the even-odd rule.
#[inline]
fn fill_row(r: usize, cols: usize, vertices: &[(f64, f64)], row_cells: &mut [bool]) {
    let scan_y = r as f64 + 0.5;
    let mut intersections: Vec<f64> = Vec::new();

    let n = vertices.len();
    for i in 0..n - 1 {
        let (y0, x0) = vertices[i];
        let (y1, x1) = vertices[i + 1];

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

/// Rasterize a polygon into a flat boolean grid using the even-odd scanline fill rule.
///
/// Returns a `Vec<bool>` of length `dims.rows * dims.cols` where `true` indicates the pixel
/// center falls inside the polygon exterior ring. Interior rings are ignored.
///
/// The grid is laid out in row-major order: index `r * cols + c` corresponds to
/// pixel `(row=r, col=c)`.
///
/// # Algorithm
///
/// For each scanline row `r`, the scanline Y is `r + 0.5` in pixel-row space.
/// For each edge of the polygon exterior ring, the algorithm computes the
/// fractional column at which the scanline crosses the edge. Intersections are
/// sorted and pixels between consecutive pairs (even-odd rule) are filled.
#[instrument(skip(polygon, geo))]
pub fn rasterize_polygon(polygon: &Polygon<f64>, geo: &GeoTransform, dims: GridDims) -> Vec<bool> {
    let rows = dims.rows;
    let cols = dims.cols;
    let mut mask = vec![false; rows * cols];

    // Convert exterior ring vertices to pixel-space (row_f64, col_f64).
    let vertices: Vec<(f64, f64)> = polygon
        .exterior()
        .coords()
        .map(|c| geo.coord_to_pixel_f64(c.x, c.y))
        .collect();

    if vertices.len() < 2 {
        return mask;
    }

    if rows * cols >= PARALLEL_THRESHOLD {
        mask.par_chunks_mut(cols)
            .enumerate()
            .for_each(|(r, row_cells)| {
                fill_row(r, cols, &vertices, row_cells);
            });
    } else {
        for (r, row_cells) in mask.chunks_mut(cols).enumerate() {
            fill_row(r, cols, &vertices, row_cells);
        }
    }

    mask
}

#[cfg(test)]
mod tests {
    use geo::polygon;

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
}
