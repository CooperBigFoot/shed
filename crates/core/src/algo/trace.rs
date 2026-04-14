//! D8 upstream trace algorithm.

use tracing::{debug, instrument};

use crate::algo::catchment_mask::CatchmentMask;
use crate::algo::coord::{GridCoord, GridDims};
use crate::algo::flow_dir::FlowDir;
use crate::algo::flow_direction_tile::FlowDirectionTile;

/// Trace all cells upstream of a pour point using D8 flow directions.
///
/// Starting from `pour_point`, performs a DFS over the flow
/// direction grid. A neighbor at `(nr, nc)` is upstream of `(r, c)` when
/// the neighbor's flow direction equals `dir.opposite()` — i.e., the
/// neighbor flows into `(r, c)`.
///
/// The generic state parameter `S` allows this function to operate on both
/// [`Raw`](crate::algo::tile_state::Raw) and
/// [`Masked`](crate::algo::tile_state::Masked) tiles, since only read
/// access is required.
///
/// # Algorithm
///
/// 1. Allocate `visited: Vec<bool>` of size `rows * cols`, all false
/// 2. Mark pour point visited, push to stack
/// 3. Pop `(r, c)`. For each `dir` in `FlowDir::ALL`:
///    - Compute `(nr, nc) = (r as isize + dir.dy(), c as isize + dir.dx())`
///    - Call `flow_dir.get_checked(nr, nc)` → if `Some(neighbor_dir)` and
///      `neighbor_dir == dir.opposite()` and not visited → mark visited, push
/// 4. Return `CatchmentMask { data: visited, rows, cols }`
#[instrument(skip(flow_dir))]
pub fn trace_upstream<S>(pour_point: GridCoord, flow_dir: &FlowDirectionTile<S>) -> CatchmentMask {
    let dims = flow_dir.dims();
    let rows = dims.rows;
    let cols = dims.cols;
    let mut visited = vec![false; rows * cols];

    visited[pour_point.row * cols + pour_point.col] = true;
    let mut stack = vec![(pour_point.row, pour_point.col)];

    while let Some((r, c)) = stack.pop() {
        for dir in FlowDir::ALL {
            let nr = r as isize + dir.dy();
            let nc = c as isize + dir.dx();

            if let Some(neighbor_dir) = flow_dir.get_checked(nr, nc)
                && neighbor_dir == dir.opposite()
            {
                let idx = nr as usize * cols + nc as usize;
                if !visited[idx] {
                    visited[idx] = true;
                    stack.push((nr as usize, nc as usize));
                }
            }
        }
    }

    debug!(
        cell_count = visited.iter().filter(|&&v| v).count(),
        "upstream trace complete"
    );

    CatchmentMask::from_traced(visited, GridDims::new(dims.rows, dims.cols))
}

#[cfg(test)]
mod tests {
    use hfx_core::FlowDirEncoding;

    use super::*;
    use crate::algo::coord::{GeoCoord, GridCoord, GridDims};
    use crate::algo::geo_transform::GeoTransform;

    fn simple_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    /// Build a FlowDirectionTile from a flat slice of ESRI D8 codes.
    fn make_tile(rows: usize, cols: usize, values: &[u8]) -> FlowDirectionTile {
        let dims = GridDims::new(rows, cols);
        let mut tile =
            FlowDirectionTile::new(dims, simple_geo(), FlowDirEncoding::Esri).unwrap();
        for r in 0..rows {
            for c in 0..cols {
                tile.set_raw(GridCoord::new(r, c), values[r * cols + c]);
            }
        }
        tile
    }

    #[test]
    fn star_3x3_all_flow_to_center() {
        // All 8 neighbors point toward (1,1). Center drains south (4).
        // ESRI codes:
        //   (0,0) SE=2, (0,1) S=4,  (0,2) SW=8
        //   (1,0) E=1,  (1,1) S=4,  (1,2) W=16
        //   (2,0) NE=128, (2,1) N=64, (2,2) NW=32
        #[rustfmt::skip]
        let values: [u8; 9] = [
            2,   4,   8,
            1,   4,  16,
            128, 64,  32,
        ];
        let tile = make_tile(3, 3, &values);
        let mask = trace_upstream(GridCoord::new(1, 1), &tile);

        assert_eq!(mask.cell_count(), 9);
        for r in 0..3 {
            for c in 0..3 {
                assert!(
                    mask.contains(GridCoord::new(r, c)),
                    "expected ({r},{c}) to be in mask"
                );
            }
        }
    }

    #[test]
    fn linear_chain_east() {
        // 1x5 grid. All cells flow East (ESRI 1): [1, 1, 1, 1, 1].
        // Pour point at (0,4) — rightmost.
        let values: [u8; 5] = [1, 1, 1, 1, 1];
        let tile = make_tile(1, 5, &values);
        let mask = trace_upstream(GridCoord::new(0, 4), &tile);

        assert_eq!(mask.cell_count(), 5);
        for c in 0..5 {
            assert!(
                mask.contains(GridCoord::new(0, c)),
                "expected (0,{c}) in mask"
            );
        }
    }

    #[test]
    fn convergent_y_shape() {
        // 4x3 grid. Two branches from (0,0) and (0,2) converge at (1,1),
        // then flow down to (3,1). Pour point: (3,1).
        //
        // Layout (ESRI codes):
        //   Row 0: [  2,   0,   8]   // (0,0) SE, (0,1) nodata, (0,2) SW
        //   Row 1: [  0,   4,   0]   // (1,1) S
        //   Row 2: [  0,   4,   0]   // (2,1) S
        //   Row 3: [  0,   4,   0]   // (3,1) S (drains out)
        // 0 = nodata
        #[rustfmt::skip]
        let values: [u8; 12] = [
            2, 0,  8,
            0, 4,  0,
            0, 4,  0,
            0, 4,  0,
        ];
        let tile = make_tile(4, 3, &values);
        let mask = trace_upstream(GridCoord::new(3, 1), &tile);

        // Expected: (0,0), (0,2), (1,1), (2,1), (3,1) = 5 cells
        assert_eq!(mask.cell_count(), 5);
        assert!(mask.contains(GridCoord::new(0, 0)));
        assert!(mask.contains(GridCoord::new(0, 2)));
        assert!(mask.contains(GridCoord::new(1, 1)));
        assert!(mask.contains(GridCoord::new(2, 1)));
        assert!(mask.contains(GridCoord::new(3, 1)));
        assert!(!mask.contains(GridCoord::new(0, 1)));
    }

    #[test]
    fn nodata_stops_trace() {
        // Same 3x3 star as test 1, but (0,0) and (2,2) are set to nodata (0).
        // Those cells should not appear in the mask.
        #[rustfmt::skip]
        let values: [u8; 9] = [
            0,   4,   8,
            1,   4,  16,
            128, 64,   0,
        ];
        let tile = make_tile(3, 3, &values);
        let mask = trace_upstream(GridCoord::new(1, 1), &tile);

        assert_eq!(mask.cell_count(), 7);
        assert!(
            !mask.contains(GridCoord::new(0, 0)),
            "(0,0) should be excluded (nodata)"
        );
        assert!(
            !mask.contains(GridCoord::new(2, 2)),
            "(2,2) should be excluded (nodata)"
        );
    }

    #[test]
    fn edge_pour_point() {
        // 3x3 grid. Cells draining toward (0,0):
        //   (0,1) W=16, (1,0) N=64, (1,1) NW=32. Others nodata.
        // Pour point: (0,0). OOB neighbors must be skipped safely.
        #[rustfmt::skip]
        let values: [u8; 9] = [
            0,  16,   0,
            64,  32,   0,
            0,   0,   0,
        ];
        let tile = make_tile(3, 3, &values);
        let mask = trace_upstream(GridCoord::new(0, 0), &tile);

        assert_eq!(mask.cell_count(), 4);
        assert!(mask.contains(GridCoord::new(0, 0)));
        assert!(mask.contains(GridCoord::new(0, 1)));
        assert!(mask.contains(GridCoord::new(1, 0)));
        assert!(mask.contains(GridCoord::new(1, 1)));
    }

    #[test]
    fn disconnected_excluded() {
        // 3x3 grid. Only vertical column flows to center:
        //   (0,1) S=4, (2,1) N=64. Center drains S=4.
        // Side cells flow away: (1,0) W=16, (1,2) E=1. Corners nodata.
        #[rustfmt::skip]
        let values: [u8; 9] = [
            0,  4,  0,
            16, 4,  1,
            0,  64, 0,
        ];
        let tile = make_tile(3, 3, &values);
        let mask = trace_upstream(GridCoord::new(1, 1), &tile);

        assert_eq!(mask.cell_count(), 3);
        assert!(mask.contains(GridCoord::new(0, 1)));
        assert!(mask.contains(GridCoord::new(1, 1)));
        assert!(mask.contains(GridCoord::new(2, 1)));
        assert!(!mask.contains(GridCoord::new(1, 0)));
        assert!(!mask.contains(GridCoord::new(1, 2)));
    }

    #[test]
    fn single_cell() {
        // 1x1 grid, nodata value. Pour point is the only cell.
        let values: [u8; 1] = [255];
        let tile = make_tile(1, 1, &values);
        let mask = trace_upstream(GridCoord::new(0, 0), &tile);

        assert_eq!(mask.cell_count(), 1);
        assert!(mask.contains(GridCoord::new(0, 0)));
    }

    #[test]
    fn five_by_five_partial_watershed() {
        // 5x5 grid with two branches merging at (3,2), then flowing to pour point (4,2).
        //
        // Branch A: (0,1) -> (1,1) -> (2,2) -> (3,2)
        //   (0,1) S=4  flows south to (1,1)
        //   (1,1) SE=2 flows southeast to (2,2)
        //   (2,2) S=4  flows south to (3,2)
        //
        // Branch B: (0,3) -> (1,3) -> (2,3) -> (3,3) -> (3,2)
        //   (0,3) S=4, (1,3) S=4, (2,3) S=4, (3,3) W=16
        //
        // Trunk: (3,2) S=4, (4,2) S=4 (pour point, drains out)
        //
        // Nodata barriers (0) break connectivity at corners and sides.
        // Disconnected: (2,0) E=1 — flows east into nodata (2,1), stays out of mask.
        // (2,4) is nodata so it is excluded.
        //
        // Expected cells in mask: (0,1),(1,1),(2,2),(0,3),(1,3),(2,3),(3,3),(3,2),(4,2) = 9
        #[rustfmt::skip]
        let values: [u8; 25] = [
            // col:  0    1    2    3    4
            /* r0 */ 0,   4,   0,   4,   0,
            /* r1 */ 0,   2,   0,   4,   0,
            /* r2 */ 1,   0,   4,   4,   0,
            /* r3 */ 0,   0,   4,  16,   0,
            /* r4 */ 0,   0,   4,   0,   0,
        ];
        let tile = make_tile(5, 5, &values);
        let mask = trace_upstream(GridCoord::new(4, 2), &tile);

        assert_eq!(mask.cell_count(), 9);
        // Branch A
        assert!(mask.contains(GridCoord::new(0, 1)));
        assert!(mask.contains(GridCoord::new(1, 1)));
        assert!(mask.contains(GridCoord::new(2, 2)));
        // Branch B
        assert!(mask.contains(GridCoord::new(0, 3)));
        assert!(mask.contains(GridCoord::new(1, 3)));
        assert!(mask.contains(GridCoord::new(2, 3)));
        assert!(mask.contains(GridCoord::new(3, 3)));
        // Trunk
        assert!(mask.contains(GridCoord::new(3, 2)));
        assert!(mask.contains(GridCoord::new(4, 2)));
        // Disconnected cells not in mask
        assert!(!mask.contains(GridCoord::new(2, 0)));
        assert!(!mask.contains(GridCoord::new(2, 4)));
    }

}
