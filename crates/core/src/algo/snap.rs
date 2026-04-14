//! Pour-point snapping to the nearest high-accumulation cell.

use tracing::{debug, info, instrument};

use crate::algo::accumulation_tile::AccumulationTile;
use crate::algo::coord::{GeoCoord, GridCoord};
use crate::algo::snap_threshold::SnapThreshold;
use crate::algo::tile_state::Masked;

/// Errors from pour-point snapping.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SnapError {
    /// No flow-accumulation cell within the catchment mask exceeds the threshold.
    #[error("no cell above threshold {threshold} within catchment mask near {outlet}")]
    NoCellAboveThreshold {
        /// Minimum accumulation pixel count required.
        threshold: u32,
        /// Geographic coordinate of the input outlet.
        outlet: GeoCoord,
    },
    /// The outlet point falls outside the raster tile extent.
    #[error("outlet {outlet} outside tile extent ({rows}x{cols})")]
    OutletOutOfBounds {
        /// Geographic coordinate of the input outlet.
        outlet: GeoCoord,
        /// Number of rows in the tile.
        rows: usize,
        /// Number of columns in the tile.
        cols: usize,
    },
}

/// Result of a successful pour-point snap.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SnappedPoint {
    cell: GridCoord,
    coord: GeoCoord,
    accumulation: f32,
}

impl SnappedPoint {
    /// Returns the row index of the snapped cell.
    pub fn row(&self) -> usize {
        self.cell.row
    }

    /// Returns the column index of the snapped cell.
    pub fn col(&self) -> usize {
        self.cell.col
    }

    /// Returns the x geographic coordinate of the snapped cell center.
    pub fn x(&self) -> f64 {
        self.coord.lon
    }

    /// Returns the y geographic coordinate of the snapped cell center.
    pub fn y(&self) -> f64 {
        self.coord.lat
    }

    /// Returns the pixel position as [`GridCoord`].
    pub fn pixel(&self) -> GridCoord {
        self.cell
    }

    /// Returns the geographic coordinates as [`GeoCoord`].
    pub fn coord(&self) -> GeoCoord {
        self.coord
    }

    /// Returns the flow accumulation value at the snapped cell.
    pub fn accumulation(&self) -> f32 {
        self.accumulation
    }
}

/// Snap an outlet to the nearest high-accumulation cell within a masked accumulation tile.
///
/// Converts `outlet` to fractional pixel coordinates, then scans all cells
/// where the accumulation value is not NaN and `>= threshold.as_f32()`. Picks
/// the nearest cell by squared Euclidean distance in pixel space. Ties are
/// broken by higher accumulation.
///
/// The `accumulation` tile must already be masked — cells outside the
/// catchment have been set to NaN by [`AccumulationTile::apply_mask`], so no
/// separate mask parameter is required.
///
/// # Errors
///
/// | Condition | Error |
/// |-----------|-------|
/// | Outlet falls outside raster extent | [`SnapError::OutletOutOfBounds`] |
/// | No masked cell exceeds threshold | [`SnapError::NoCellAboveThreshold`] |
#[instrument(skip(accumulation))]
pub fn snap_pour_point(
    outlet: GeoCoord,
    accumulation: &AccumulationTile<Masked>,
    threshold: SnapThreshold,
) -> Result<SnappedPoint, SnapError> {
    let dims = accumulation.dims();
    let rows = dims.rows;
    let cols = dims.cols;
    let geo = accumulation.geo();

    // Convert outlet to fractional pixel coordinates
    let (frac_row, frac_col) = geo.coord_to_pixel_f64(outlet.lon, outlet.lat);

    // Check bounds — fractional coords must be within [0, rows) x [0, cols)
    if frac_row < 0.0 || frac_col < 0.0 || frac_row >= rows as f64 || frac_col >= cols as f64 {
        return Err(SnapError::OutletOutOfBounds { outlet, rows, cols });
    }

    debug!(frac_row, frac_col, "outlet pixel coordinates");

    let threshold_f32 = threshold.as_f32();
    let mut best: Option<(usize, usize, f64, f32)> = None; // (row, col, dist_sq, acc)

    for r in 0..rows {
        for c in 0..cols {
            // Get accumulation, skip NaN (masked-out cells are already NaN)
            let acc = accumulation.get_raw(GridCoord::new(r, c));
            if acc.is_nan() || acc < threshold_f32 {
                continue;
            }

            // Squared Euclidean distance in pixel space (use pixel centers: r+0.5, c+0.5)
            let dr = (r as f64 + 0.5) - frac_row;
            let dc = (c as f64 + 0.5) - frac_col;
            let dist_sq = dr * dr + dc * dc;

            let is_better = match best {
                None => true,
                Some((_, _, best_dist, best_acc)) => {
                    dist_sq < best_dist || (dist_sq == best_dist && acc > best_acc)
                }
            };

            if is_better {
                best = Some((r, c, dist_sq, acc));
            }
        }
    }

    match best {
        Some((row, col, _, acc)) => {
            let coord = geo.pixel_to_coord(GridCoord::new(row, col));
            info!(
                row,
                col,
                x = coord.lon,
                y = coord.lat,
                accumulation = acc,
                "pour point snapped"
            );
            Ok(SnappedPoint {
                cell: GridCoord::new(row, col),
                coord,
                accumulation: acc,
            })
        }
        None => Err(SnapError::NoCellAboveThreshold {
            threshold: threshold.pixels(),
            outlet,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::catchment_mask::CatchmentMask;
    use crate::algo::coord::{GeoCoord, GridCoord, GridDims};
    use crate::algo::geo_transform::GeoTransform;
    use crate::algo::raster_tile::RasterTile;

    fn simple_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    // Test 1: single candidate above threshold is selected
    #[test]
    fn single_candidate() {
        let mut tile = AccumulationTile::new(GridDims::new(3, 3), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(1, 1), 1000.0);
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        let result =
            snap_pour_point(GeoCoord::new(1.5, -1.5), &masked, SnapThreshold::new(500)).unwrap();
        assert_eq!(result.pixel(), GridCoord::new(1, 1));
        assert_eq!(result.accumulation(), 1000.0);
    }

    // Test 2: nearest of multiple candidates above threshold is selected
    #[test]
    fn nearest_of_multiple() {
        let data = vec![
            600.0_f32,
            f32::NAN,
            700.0, // row 0: (0,0)=600, (0,1)=NaN, (0,2)=700
            f32::NAN,
            f32::NAN,
            f32::NAN, // row 1: all NaN
            f32::NAN,
            f32::NAN,
            800.0, // row 2: (2,2)=800
        ];
        let raw = RasterTile::from_vec(data, GridDims::new(3, 3), f32::NAN, simple_geo()).unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        // Outlet very close to (0,2): outlet_x=2.5, outlet_y=-0.5
        let result =
            snap_pour_point(GeoCoord::new(2.5, -0.5), &masked, SnapThreshold::new(500)).unwrap();
        assert_eq!(result.pixel(), GridCoord::new(0, 2));
    }

    // Test 3: tie between equidistant cells is broken by higher accumulation
    #[test]
    fn tie_break_by_accumulation() {
        let data = vec![
            f32::NAN,
            f32::NAN,
            f32::NAN, // row 0
            600.0,
            f32::NAN,
            800.0, // row 1: (1,0)=600, (1,2)=800
            f32::NAN,
            f32::NAN,
            f32::NAN, // row 2
        ];
        let raw = RasterTile::from_vec(data, GridDims::new(3, 3), f32::NAN, simple_geo()).unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        // Outlet at center of grid: outlet_x=1.5, outlet_y=-1.5
        // (1,0) center is at (0.5, -1.5), (1,2) center is at (2.5, -1.5)
        // Both are equidistant from outlet (1.5, -1.5) — dist_sq = 1.0 each
        let result =
            snap_pour_point(GeoCoord::new(1.5, -1.5), &masked, SnapThreshold::new(500)).unwrap();
        assert_eq!(
            result.pixel(),
            GridCoord::new(1, 2),
            "should prefer higher accumulation on tie"
        );
    }

    // Test 4: mask constrains which cells are eligible
    #[test]
    fn mask_constrains_search() {
        let data = vec![
            1000.0,
            f32::NAN,
            f32::NAN, // row 0: (0,0)=1000
            f32::NAN,
            f32::NAN,
            f32::NAN, // row 1
            f32::NAN,
            f32::NAN,
            900.0, // row 2: (2,2)=900
        ];
        let raw = RasterTile::from_vec(data, GridDims::new(3, 3), f32::NAN, simple_geo()).unwrap();
        let tile = AccumulationTile::from_raw(raw);
        // (0,0) is masked out; only (2,2) is eligible
        let mut mask_data = vec![false; 9];
        mask_data[8] = true; // only (2,2) is eligible
        let mask = CatchmentMask::new(mask_data, GridDims::new(3, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        let result =
            snap_pour_point(GeoCoord::new(0.5, -0.5), &masked, SnapThreshold::new(500)).unwrap();
        assert_eq!(result.pixel(), GridCoord::new(2, 2));
    }

    // Test 5: no candidates returns NoCellAboveThreshold error
    #[test]
    fn no_candidates_error() {
        let data = vec![100.0_f32; 9];
        let raw = RasterTile::from_vec(data, GridDims::new(3, 3), f32::NAN, simple_geo()).unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        let err = snap_pour_point(GeoCoord::new(1.5, -1.5), &masked, SnapThreshold::new(500))
            .unwrap_err();
        assert!(matches!(err, SnapError::NoCellAboveThreshold { .. }));
    }

    // Test 6: outlet already on a stream cell
    #[test]
    fn outlet_already_on_stream() {
        let mut tile = AccumulationTile::new(GridDims::new(3, 3), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(1, 1), 1000.0);
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        let result =
            snap_pour_point(GeoCoord::new(1.5, -1.5), &masked, SnapThreshold::new(500)).unwrap();
        assert_eq!(result.pixel(), GridCoord::new(1, 1));
        assert_eq!(result.accumulation(), 1000.0);
    }

    // Test 7: NaN cells are skipped
    #[test]
    fn nan_skipped() {
        let data = vec![
            f32::NAN,
            f32::NAN, // row 0: all NaN
            600.0,
            f32::NAN, // row 1: (1,0)=600, (1,1)=NaN
        ];
        let raw = RasterTile::from_vec(data, GridDims::new(2, 2), f32::NAN, simple_geo()).unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = CatchmentMask::new(vec![true; 4], GridDims::new(2, 2));
        let masked = tile.apply_mask(&mask).unwrap();
        // Outlet near center: outlet_x=1.0, outlet_y=-1.0
        let result =
            snap_pour_point(GeoCoord::new(1.0, -1.0), &masked, SnapThreshold::new(500)).unwrap();
        assert_eq!(result.pixel(), GridCoord::new(1, 0));
    }

    // Test 8: exact threshold boundary — value exactly equal to threshold is accepted
    #[test]
    fn exact_threshold() {
        let data = vec![499.0_f32, 500.0, 501.0];
        let raw = RasterTile::from_vec(data, GridDims::new(1, 3), f32::NAN, simple_geo()).unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = CatchmentMask::new(vec![true; 3], GridDims::new(1, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        // Outlet at center of (0,1): outlet_x=1.5, outlet_y=-0.5
        let result =
            snap_pour_point(GeoCoord::new(1.5, -0.5), &masked, SnapThreshold::new(500)).unwrap();
        assert_eq!(
            result.pixel(),
            GridCoord::new(0, 1),
            "cell at threshold should be accepted (>=)"
        );
    }

    // Test 9: outlet outside raster bounds returns OutletOutOfBounds error
    #[test]
    fn outlet_out_of_bounds() {
        let mut tile = AccumulationTile::new(GridDims::new(3, 3), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(1, 1), 1000.0);
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        let masked = tile.apply_mask(&mask).unwrap();
        // outlet_x=10.0, outlet_y=10.0 → frac_row = -10.0 (negative = OOB)
        let err = snap_pour_point(GeoCoord::new(10.0, 10.0), &masked, SnapThreshold::new(500))
            .unwrap_err();
        assert!(matches!(err, SnapError::OutletOutOfBounds { .. }));
    }

    // Test 10: all mask entries false → NoCellAboveThreshold even if values are high
    #[test]
    fn empty_mask_error() {
        let data = vec![1000.0_f32; 4];
        let raw = RasterTile::from_vec(data, GridDims::new(2, 2), f32::NAN, simple_geo()).unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = CatchmentMask::new(vec![false; 4], GridDims::new(2, 2));
        let masked = tile.apply_mask(&mask).unwrap();
        let err = snap_pour_point(GeoCoord::new(1.0, -1.0), &masked, SnapThreshold::new(500))
            .unwrap_err();
        assert!(matches!(err, SnapError::NoCellAboveThreshold { .. }));
    }
}
