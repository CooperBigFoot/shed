//! Terminal atom raster refinement.
//!
//! Refines a coarse terminal polygon into a precise watershed by:
//! 1. Rasterizing the terminal polygon onto the flow raster grid.
//! 2. Masking both flow-direction and accumulation tiles to that polygon.
//! 3. Snapping the outlet to the nearest high-accumulation cell.
//! 4. Tracing all upstream cells from the snapped outlet.
//! 5. Polygonizing the upstream trace mask back to geographic coordinates.

use std::path::Path;

use geo::{BoundingRect, MultiPolygon};
use tracing::{debug, info, instrument};

use crate::algo::accumulation_tile::AccumulationTile;
use crate::algo::catchment_mask::CatchmentMask;
use crate::algo::coord::GeoCoord;
use crate::algo::flow_direction_tile::FlowDirectionTile;
use crate::algo::polygonize::polygonize;
use crate::algo::raster_tile::RasterTileError;
use crate::algo::rasterize::rasterize_multi_polygon;
use crate::algo::snap::{SnapError, SnappedPoint, snap_pour_point};
use crate::algo::snap_threshold::SnapThreshold;
use crate::algo::tile_state::Raw;
use crate::algo::trace::trace_upstream;
use crate::algo::traits::{RasterSource, RasterSourceError};

/// Errors from terminal atom raster refinement.
#[derive(Debug, thiserror::Error)]
pub enum RefinementError {
    /// Flow direction and accumulation tiles have mismatched dimensions or geo-transforms.
    #[error("tile mismatch: flow_dir is {fd_rows}x{fd_cols}, accumulation is {acc_rows}x{acc_cols}")]
    TileMismatch {
        /// Number of rows in the flow direction tile.
        fd_rows: usize,
        /// Number of columns in the flow direction tile.
        fd_cols: usize,
        /// Number of rows in the accumulation tile.
        acc_rows: usize,
        /// Number of columns in the accumulation tile.
        acc_cols: usize,
    },

    /// Rasterizing the terminal polygon produced an all-false mask.
    #[error("terminal polygon produced an empty raster mask ({rows}x{cols} tile)")]
    EmptyRasterMask {
        /// Number of rows in the tile.
        rows: usize,
        /// Number of columns in the tile.
        cols: usize,
    },

    /// Masking a raster tile failed due to a dimension mismatch.
    #[error("mask application failed: {source}")]
    MaskFailed {
        /// The underlying raster tile error.
        source: RasterTileError,
    },

    /// Pour-point snapping failed within the masked accumulation tile.
    #[error("pour-point snap failed: {source}")]
    SnapFailed {
        /// The underlying snap error.
        source: SnapError,
    },

    /// Polygonizing the traced catchment mask produced no geometry.
    #[error("trace mask polygonization produced no geometry")]
    EmptyPolygonization,

    /// Raster source failed to load a tile (only from loader wrapper).
    #[error("failed to load raster tile: {source}")]
    RasterLoad {
        /// The underlying raster source error.
        source: RasterSourceError,
    },
}

impl From<SnapError> for RefinementError {
    fn from(source: SnapError) -> Self {
        RefinementError::SnapFailed { source }
    }
}

impl From<RasterSourceError> for RefinementError {
    fn from(source: RasterSourceError) -> Self {
        RefinementError::RasterLoad { source }
    }
}

/// Result of a successful terminal atom refinement.
#[derive(Debug, Clone)]
pub struct RefinementResult {
    snapped_point: SnappedPoint,
    polygon: MultiPolygon<f64>,
}

impl RefinementResult {
    /// Returns a reference to the snapped pour point.
    pub fn snapped_point(&self) -> &SnappedPoint {
        &self.snapped_point
    }

    /// Returns the geographic coordinate of the snapped pour point.
    pub fn snapped_coord(&self) -> GeoCoord {
        self.snapped_point.coord()
    }

    /// Returns a reference to the refined watershed polygon.
    pub fn polygon(&self) -> &MultiPolygon<f64> {
        &self.polygon
    }

    /// Consumes `self` and returns the refined watershed polygon.
    pub fn into_polygon(self) -> MultiPolygon<f64> {
        self.polygon
    }
}

/// Refine a terminal polygon into a precise watershed polygon.
///
/// Rasterizes `terminal_polygon` onto the raster grid, masks both tiles to that
/// footprint, snaps `outlet` to the nearest high-accumulation cell, traces all
/// upstream cells, and polygonizes the trace mask back to geographic coordinates.
///
/// # Errors
///
/// | Condition | Error |
/// |-----------|-------|
/// | Flow-dir and accumulation tiles have different dims or geo-transforms | [`RefinementError::TileMismatch`] |
/// | Terminal polygon rasterizes to an empty mask | [`RefinementError::EmptyRasterMask`] |
/// | Tile masking fails due to dimension mismatch | [`RefinementError::MaskFailed`] |
/// | No cell above threshold near outlet | [`RefinementError::SnapFailed`] |
/// | Trace mask polygonizes to nothing | [`RefinementError::EmptyPolygonization`] |
#[instrument(skip(terminal_polygon, flow_dir, accumulation))]
pub fn refine_terminal(
    terminal_polygon: &MultiPolygon<f64>,
    outlet: GeoCoord,
    flow_dir: FlowDirectionTile<Raw>,
    accumulation: AccumulationTile<Raw>,
    threshold: SnapThreshold,
) -> Result<RefinementResult, RefinementError> {
    // Step 1: Validate tile alignment
    let fd_dims = flow_dir.dims();
    let acc_dims = accumulation.dims();
    if fd_dims != acc_dims || flow_dir.geo() != accumulation.geo() {
        return Err(RefinementError::TileMismatch {
            fd_rows: fd_dims.rows,
            fd_cols: fd_dims.cols,
            acc_rows: acc_dims.rows,
            acc_cols: acc_dims.cols,
        });
    }

    // Step 2: Save geo and dims before consuming tiles
    let geo = *flow_dir.geo();
    let dims = flow_dir.dims();

    // Step 3: Rasterize terminal polygon
    let mask_data = rasterize_multi_polygon(terminal_polygon, &geo, dims);

    // Step 4: Check mask is non-empty
    if !mask_data.iter().any(|&v| v) {
        return Err(RefinementError::EmptyRasterMask {
            rows: dims.rows,
            cols: dims.cols,
        });
    }

    let mask_cell_count = mask_data.iter().filter(|&&v| v).count();
    debug!(
        mask_cell_count,
        rows = dims.rows,
        cols = dims.cols,
        "rasterized terminal polygon"
    );

    // Step 5: Build CatchmentMask
    let catchment_mask = CatchmentMask::new(mask_data, dims);

    // Step 6: Mask BOTH tiles (consume Raw, produce Masked)
    let masked_flow_dir = flow_dir
        .apply_mask(&catchment_mask)
        .map_err(|e| RefinementError::MaskFailed { source: e })?;
    let masked_acc = accumulation
        .apply_mask(&catchment_mask)
        .map_err(|e| RefinementError::MaskFailed { source: e })?;

    // Step 7: Snap pour point
    let snapped = snap_pour_point(outlet, &masked_acc, threshold)?;
    debug!(
        row = snapped.row(),
        col = snapped.col(),
        x = snapped.x(),
        y = snapped.y(),
        accumulation = snapped.accumulation(),
        "snapped pour point"
    );

    // Step 8: Trace upstream on MASKED flow_dir
    let trace_mask = trace_upstream(snapped.pixel(), &masked_flow_dir);

    // Step 9: Polygonize
    let polygon = polygonize(&trace_mask, &geo).ok_or(RefinementError::EmptyPolygonization)?;

    info!(
        polygon_components = polygon.0.len(),
        snapped_x = snapped.x(),
        snapped_y = snapped.y(),
        "terminal refinement complete"
    );

    Ok(RefinementResult {
        snapped_point: snapped,
        polygon,
    })
}

/// Load raster tiles from a [`RasterSource`] and refine a terminal polygon.
///
/// Computes the bounding box of `terminal_polygon`, loads windowed tiles from
/// `source`, then delegates to [`refine_terminal`].
///
/// # Errors
///
/// | Condition | Error |
/// |-----------|-------|
/// | Terminal polygon has no bounding rect | [`RefinementError::EmptyRasterMask`] |
/// | Raster source fails to load a tile | [`RefinementError::RasterLoad`] |
/// | Any error from [`refine_terminal`] | (propagated) |
#[instrument(skip(source, terminal_polygon))]
pub fn refine_terminal_from_source(
    source: &dyn RasterSource,
    flow_dir_path: &Path,
    flow_acc_path: &Path,
    terminal_polygon: &MultiPolygon<f64>,
    outlet: GeoCoord,
    threshold: SnapThreshold,
) -> Result<RefinementResult, RefinementError> {
    let bbox = terminal_polygon
        .bounding_rect()
        .ok_or(RefinementError::EmptyRasterMask { rows: 0, cols: 0 })?;

    let flow_dir = source.load_flow_direction(flow_dir_path, &bbox)?;
    let accumulation = source.load_accumulation(flow_acc_path, &bbox)?;

    refine_terminal(terminal_polygon, outlet, flow_dir, accumulation, threshold)
}

#[cfg(test)]
mod tests {
    use geo::{LineString, Polygon, Rect};
    use hfx_core::FlowDirEncoding;

    use super::*;
    use crate::algo::coord::{GeoCoord, GridCoord, GridDims};
    use crate::algo::geo_transform::GeoTransform;
    use crate::algo::raster_tile::RasterTile;
    use crate::algo::traits::RasterSourceError;

    fn simple_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    fn make_flow_tile(rows: usize, cols: usize, values: &[u8]) -> FlowDirectionTile<Raw> {
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

    fn make_flow_tile_with(
        rows: usize,
        cols: usize,
        values: &[u8],
        geo: GeoTransform,
        encoding: FlowDirEncoding,
    ) -> FlowDirectionTile<Raw> {
        let dims = GridDims::new(rows, cols);
        let mut tile = FlowDirectionTile::new(dims, geo, encoding).unwrap();
        for r in 0..rows {
            for c in 0..cols {
                tile.set_raw(GridCoord::new(r, c), values[r * cols + c]);
            }
        }
        tile
    }

    fn make_acc_tile(rows: usize, cols: usize, values: &[f32]) -> AccumulationTile<Raw> {
        let dims = GridDims::new(rows, cols);
        let raw =
            RasterTile::from_vec(values.to_vec(), dims, f32::NAN, simple_geo()).unwrap();
        AccumulationTile::from_raw(raw)
    }

    fn make_acc_tile_with(
        rows: usize,
        cols: usize,
        values: &[f32],
        geo: GeoTransform,
    ) -> AccumulationTile<Raw> {
        let dims = GridDims::new(rows, cols);
        let raw = RasterTile::from_vec(values.to_vec(), dims, f32::NAN, geo).unwrap();
        AccumulationTile::from_raw(raw)
    }

    fn rect_polygon(x0: f64, y0: f64, x1: f64, y1: f64) -> MultiPolygon<f64> {
        let poly = Polygon::new(
            LineString::from(vec![
                (x0, y0),
                (x1, y0),
                (x1, y1),
                (x0, y1),
                (x0, y0),
            ]),
            vec![],
        );
        MultiPolygon::new(vec![poly])
    }

    // ── Group A: Happy-path refinement ────────────────────────────────────────

    #[test]
    fn simple_convergent_5x5() {
        #[rustfmt::skip]
        let fd_values: [u8; 25] = [
            // col:  0   1   2   3   4
            /* r0 */ 2,  4,  4,  4,  8,
            /* r1 */ 1,  2,  4,  8, 16,
            /* r2 */ 1,  1,  4, 16, 16,
            /* r3 */ 0,  0,  0,  0,  0,
            /* r4 */ 0,  0,  0,  0,  0,
        ];
        let mut acc_values = [1.0_f32; 25];
        acc_values[2 * 5 + 2] = 800.0; // (2,2) = 800

        let flow_dir = make_flow_tile(5, 5, &fd_values);
        let accumulation = make_acc_tile(5, 5, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 5.0, -5.0);
        let outlet = GeoCoord::new(2.5, -2.5);
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        let coord = result.snapped_coord();
        assert!(
            (coord.lon - 2.5).abs() < 1e-9,
            "expected lon=2.5, got {}",
            coord.lon
        );
        assert!(
            (coord.lat - (-2.5)).abs() < 1e-9,
            "expected lat=-2.5, got {}",
            coord.lat
        );

        assert_eq!(result.polygon().0.len(), 1, "expected 1 polygon component");

        use geo::algorithm::Area;
        let area = result.polygon().unsigned_area();
        assert!(
            (area - 15.0).abs() < 0.001,
            "expected area ~15.0, got {area}"
        );
    }

    #[test]
    fn snap_to_offset_pour_point() {
        #[rustfmt::skip]
        let fd_values: [u8; 25] = [
            1, 1, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
        ];
        let mut acc_values = [f32::NAN; 25];
        acc_values[0 * 5 + 0] = 1.0;
        acc_values[0 * 5 + 1] = 2.0;
        acc_values[0 * 5 + 2] = 600.0;
        acc_values[1 * 5 + 2] = 700.0;
        acc_values[2 * 5 + 2] = 800.0;
        acc_values[3 * 5 + 2] = 900.0;
        acc_values[4 * 5 + 2] = 1000.0;

        let flow_dir = make_flow_tile(5, 5, &fd_values);
        let accumulation = make_acc_tile(5, 5, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 5.0, -5.0);
        let outlet = GeoCoord::new(0.5, -0.5); // pixel (0,0)
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        // Snaps to (0,2): nearest cell above 500
        let coord = result.snapped_coord();
        assert!(
            (coord.lon - 2.5).abs() < 1e-9,
            "expected lon=2.5, got {}",
            coord.lon
        );
        assert!(
            (coord.lat - (-0.5)).abs() < 1e-9,
            "expected lat=-0.5, got {}",
            coord.lat
        );

        use geo::algorithm::Area;
        let area = result.polygon().unsigned_area();
        // Cells (0,0), (0,1), (0,2) contribute; (0,0) and (0,1) trace upstream of (0,2)
        assert!(
            (area - 3.0).abs() < 0.001,
            "expected area ~3.0, got {area}"
        );
    }

    #[test]
    fn sub_polygon_within_terminal() {
        // All flow south; acc increases downward along col 2
        #[rustfmt::skip]
        let fd_values: [u8; 25] = [
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
        ];
        let mut acc_values = [f32::NAN; 25];
        acc_values[0 * 5 + 2] = 100.0;
        acc_values[1 * 5 + 2] = 200.0;
        acc_values[2 * 5 + 2] = 300.0;
        acc_values[3 * 5 + 2] = 400.0;
        acc_values[4 * 5 + 2] = 600.0;

        let flow_dir = make_flow_tile(5, 5, &fd_values);
        let accumulation = make_acc_tile(5, 5, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 5.0, -5.0);
        let outlet = GeoCoord::new(2.5, -2.5); // pixel (2,2)
        let threshold = SnapThreshold::new(200);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        use geo::algorithm::Area;
        let polygon_area = result.polygon().unsigned_area();
        let terminal_area = terminal_polygon.unsigned_area();

        assert!(
            polygon_area < terminal_area,
            "refined area {polygon_area} should be less than terminal area {terminal_area}"
        );

        // The refined polygon should be within the terminal polygon bounds
        use geo::BoundingRect;
        let terminal_bbox = terminal_polygon.bounding_rect().unwrap();
        let refined_bbox = result.polygon().bounding_rect().unwrap();
        assert!(
            refined_bbox.min().x >= terminal_bbox.min().x - 1e-9,
            "refined min_x {} outside terminal",
            refined_bbox.min().x
        );
        assert!(
            refined_bbox.max().x <= terminal_bbox.max().x + 1e-9,
            "refined max_x {} outside terminal",
            refined_bbox.max().x
        );
    }

    #[test]
    fn single_cell_result() {
        // 3x3, all flow_dir = 0 (nodata), acc: center = 900, rest NaN
        let fd_values = [0u8; 9];
        let mut acc_values = [f32::NAN; 9];
        acc_values[1 * 3 + 1] = 900.0;

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        use geo::algorithm::Area;
        let area = result.polygon().unsigned_area();
        assert!(
            (area - 1.0).abs() < 0.001,
            "expected area ~1.0 (single cell), got {area}"
        );

        let coord = result.snapped_coord();
        assert!(
            (coord.lon - 1.5).abs() < 1e-9,
            "expected snapped lon=1.5, got {}",
            coord.lon
        );
        assert!(
            (coord.lat - (-1.5)).abs() < 1e-9,
            "expected snapped lat=-1.5, got {}",
            coord.lat
        );
    }

    #[test]
    fn full_tile_convergence() {
        // 3x3 star convergence to (1,1)
        #[rustfmt::skip]
        let fd_values: [u8; 9] = [
              2,   4,   8,
              1,   4,  16,
            128,  64,  32,
        ];
        let mut acc_values = [1.0_f32; 9];
        acc_values[1 * 3 + 1] = 900.0;

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        use geo::algorithm::Area;
        let area = result.polygon().unsigned_area();
        assert!(
            (area - 9.0).abs() < 0.001,
            "expected area ~9.0, got {area}"
        );

        let coord = result.snapped_coord();
        assert!(
            (coord.lon - 1.5).abs() < 1e-9,
            "expected snapped lon=1.5, got {}",
            coord.lon
        );
        assert!(
            (coord.lat - (-1.5)).abs() < 1e-9,
            "expected snapped lat=-1.5, got {}",
            coord.lat
        );
    }

    // ── Group B: Snap behavior ────────────────────────────────────────────────

    #[test]
    fn nearest_wins_over_highest() {
        // Column 2 flows south; acc decreasing downward
        #[rustfmt::skip]
        let fd_values: [u8; 25] = [
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
            0, 0, 4, 0, 0,
        ];
        let mut acc_values = [f32::NAN; 25];
        acc_values[0 * 5 + 2] = 1000.0;
        acc_values[1 * 5 + 2] = 900.0;
        acc_values[2 * 5 + 2] = 800.0;
        acc_values[3 * 5 + 2] = 700.0;
        acc_values[4 * 5 + 2] = 600.0;

        let flow_dir = make_flow_tile(5, 5, &fd_values);
        let accumulation = make_acc_tile(5, 5, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 5.0, -5.0);
        // Outlet near pixel (3,2): center is at lon=2.5, lat=-3.5
        let outlet = GeoCoord::new(2.5, -3.5);
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        // Nearest cell above 500 to (3,2) is (3,2) itself with acc=700
        assert!(
            (result.snapped_point().accumulation() - 700.0).abs() < 0.001,
            "expected accumulation=700.0, got {}",
            result.snapped_point().accumulation()
        );
    }

    #[test]
    fn tiebreak_by_accumulation() {
        // 3x3: (1,0) flows E (1), (1,2) flows W (16). Both above threshold.
        #[rustfmt::skip]
        let fd_values: [u8; 9] = [
            0,  0,  0,
            1,  0, 16,
            0,  0,  0,
        ];
        let mut acc_values = [f32::NAN; 9];
        acc_values[1 * 3 + 0] = 600.0;
        acc_values[1 * 3 + 2] = 800.0;

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        // Outlet at center, equidistant from (1,0) and (1,2)
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        // Higher acc wins on tie: (1,2) with 800.0
        assert!(
            (result.snapped_point().accumulation() - 800.0).abs() < 0.001,
            "expected accumulation=800.0, got {}",
            result.snapped_point().accumulation()
        );
    }

    // ── Group C: Edge cases ───────────────────────────────────────────────────

    #[test]
    fn outlet_on_polygon_edge() {
        // 3x3, all flow S (4), acc: all 600
        let fd_values = [4u8; 9];
        let acc_values = [600.0_f32; 9];

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        // Outlet at top-left corner (0.0, 0.0)
        let outlet = GeoCoord::new(0.0, 0.0);
        let threshold = SnapThreshold::new(500);

        // Should succeed: snap finds nearest valid cell
        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold);
        assert!(result.is_ok(), "expected Ok, got {:?}", result);
    }

    #[test]
    fn taudem_encoding() {
        // 3x3 star convergence with TauDEM codes
        // TauDEM: E=1, NE=2, N=3, NW=4, W=5, SW=6, S=7, SE=8
        #[rustfmt::skip]
        let fd_values: [u8; 9] = [
            8, 7, 6,  // SE, S, SW
            1, 7, 5,  // E, S, W
            2, 3, 4,  // NE, N, NW
        ];
        let mut acc_values = [1.0_f32; 9];
        acc_values[1 * 3 + 1] = 900.0;

        let geo = simple_geo();
        let flow_dir = make_flow_tile_with(3, 3, &fd_values, geo, FlowDirEncoding::Taudem);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        use geo::algorithm::Area;
        let area = result.polygon().unsigned_area();
        assert!(
            (area - 9.0).abs() < 0.001,
            "expected area ~9.0 (TauDEM), got {area}"
        );
    }

    #[test]
    fn non_unit_geo_transform() {
        // 3x3 star convergence, ESRI, non-unit pixels
        #[rustfmt::skip]
        let fd_values: [u8; 9] = [
              2,   4,   8,
              1,   4,  16,
            128,  64,  32,
        ];
        let mut acc_values = [1.0_f32; 9];
        acc_values[1 * 3 + 1] = 900.0;

        let geo = GeoTransform::new(GeoCoord::new(10.0, 50.0), 0.001, -0.001);
        let flow_dir = make_flow_tile_with(3, 3, &fd_values, geo, FlowDirEncoding::Esri);
        let accumulation = make_acc_tile_with(3, 3, &acc_values, geo);

        // Terminal polygon covers [10.0, 10.003] x [49.997, 50.0]
        let poly = Polygon::new(
            LineString::from(vec![
                (10.0_f64, 50.0_f64),
                (10.003, 50.0),
                (10.003, 49.997),
                (10.0, 49.997),
                (10.0, 50.0),
            ]),
            vec![],
        );
        let terminal_polygon = MultiPolygon::new(vec![poly]);

        // Center of pixel (1,1): lon = 10.0 + 1.5 * 0.001 = 10.0015, lat = 50.0 - 1.5 * 0.001 = 49.9985
        let outlet = GeoCoord::new(10.0015, 49.9985);
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        let coord = result.snapped_coord();
        assert!(
            (coord.lon - 10.0015).abs() < 1e-9,
            "expected lon~10.0015, got {}",
            coord.lon
        );

        use geo::algorithm::Area;
        let area = result.polygon().unsigned_area();
        let expected_area = 9.0 * 0.001 * 0.001;
        assert!(
            (area - expected_area).abs() < 1e-9,
            "expected area~{expected_area}, got {area}"
        );
    }

    // ── Group D: Complex topology ─────────────────────────────────────────────

    #[test]
    fn y_shaped_watershed() {
        // 5x5: two branches merge at (2,2) then flow south
        #[rustfmt::skip]
        let fd_values: [u8; 25] = [
            //  0   1   2   3   4
               2,  0,  0,  0,  8,   // (0,0) SE, (0,4) SW
               0,  2,  0,  8,  0,   // (1,1) SE, (1,3) SW
               0,  0,  4,  0,  0,   // (2,2) S
               0,  0,  4,  0,  0,
               0,  0,  4,  0,  0,
        ];
        let mut acc_values = [f32::NAN; 25];
        acc_values[0 * 5 + 0] = 1.0;
        acc_values[0 * 5 + 4] = 1.0;
        acc_values[1 * 5 + 1] = 2.0;
        acc_values[1 * 5 + 3] = 2.0;
        acc_values[2 * 5 + 2] = 600.0;
        acc_values[3 * 5 + 2] = 700.0;
        acc_values[4 * 5 + 2] = 800.0;

        let flow_dir = make_flow_tile(5, 5, &fd_values);
        let accumulation = make_acc_tile(5, 5, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 5.0, -5.0);
        let outlet = GeoCoord::new(2.5, -2.5); // pixel (2,2)
        let threshold = SnapThreshold::new(500);

        let result =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold).unwrap();

        use geo::algorithm::Area;
        let area = result.polygon().unsigned_area();
        assert!(
            (area - 5.0).abs() < 0.001,
            "expected Y-shape area ~5.0, got {area}"
        );
        // The five Y-shape cells are diagonally connected but not edge-adjacent,
        // so polygonize produces one component per disconnected group.
        assert!(
            !result.polygon().0.is_empty(),
            "expected at least 1 polygon component"
        );
    }

    // ── Group E: Error paths ──────────────────────────────────────────────────

    #[test]
    fn no_cell_above_threshold() {
        let fd_values = [4u8; 9];
        let acc_values = [100.0_f32; 9];

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let err =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold)
                .unwrap_err();
        assert!(
            matches!(err, RefinementError::SnapFailed { .. }),
            "expected SnapFailed, got {err:?}"
        );
    }

    #[test]
    fn outlet_outside_tile() {
        let fd_values = [4u8; 9];
        let mut acc_values = [f32::NAN; 9];
        acc_values[1 * 3 + 1] = 900.0;

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        // Outlet way outside
        let outlet = GeoCoord::new(10.0, 10.0);
        let threshold = SnapThreshold::new(500);

        let err =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold)
                .unwrap_err();
        assert!(
            matches!(err, RefinementError::SnapFailed { .. }),
            "expected SnapFailed, got {err:?}"
        );
    }

    #[test]
    fn all_masked_out_nan() {
        // 3x3; acc: all NaN except (2,2)=800
        // Terminal polygon covers only top-left 2x2: [0,2] x [0,-2]
        let fd_values = [4u8; 9];
        let mut acc_values = [f32::NAN; 9];
        acc_values[2 * 3 + 2] = 800.0;

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        // Terminal polygon covers only top-left 2x2 — masks out the one cell with acc
        let terminal_polygon = rect_polygon(0.0, 0.0, 2.0, -2.0);
        let outlet = GeoCoord::new(0.5, -0.5);
        let threshold = SnapThreshold::new(500);

        let err =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold)
                .unwrap_err();
        assert!(
            matches!(err, RefinementError::SnapFailed { .. }),
            "expected SnapFailed, got {err:?}"
        );
    }

    #[test]
    fn empty_raster_mask() {
        // Terminal polygon entirely outside tile extent: [10,13] x [10,13]
        let fd_values = [4u8; 9];
        let acc_values = [1000.0_f32; 9];

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        let accumulation = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(10.0, 10.0, 13.0, 13.0);
        let outlet = GeoCoord::new(11.5, 11.5);
        let threshold = SnapThreshold::new(500);

        let err =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold)
                .unwrap_err();
        assert!(
            matches!(err, RefinementError::EmptyRasterMask { .. }),
            "expected EmptyRasterMask, got {err:?}"
        );
    }

    #[test]
    fn tile_dimension_mismatch() {
        // flow_dir: 3x3, accumulation: 5x5 (different dims)
        let fd_values = [4u8; 9];
        let acc_values = [1000.0_f32; 25];

        let flow_dir = make_flow_tile(3, 3, &fd_values);
        // Use simple_geo for both but different dims
        let acc_dims = GridDims::new(5, 5);
        let raw = RasterTile::from_vec(acc_values.to_vec(), acc_dims, f32::NAN, simple_geo()).unwrap();
        let accumulation = AccumulationTile::from_raw(raw);

        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let err =
            refine_terminal(&terminal_polygon, outlet, flow_dir, accumulation, threshold)
                .unwrap_err();
        assert!(
            matches!(err, RefinementError::TileMismatch { .. }),
            "expected TileMismatch, got {err:?}"
        );
    }

    // ── Group F: Loader wrapper ───────────────────────────────────────────────

    #[test]
    fn loader_delegates_to_pure_function() {
        // 3x3 star convergence (same as full_tile_convergence / A5)
        #[rustfmt::skip]
        let fd_values: [u8; 9] = [
              2,   4,   8,
              1,   4,  16,
            128,  64,  32,
        ];
        let mut acc_values = [1.0_f32; 9];
        acc_values[1 * 3 + 1] = 900.0;

        let flow_dir_direct = make_flow_tile(3, 3, &fd_values);
        let accumulation_direct = make_acc_tile(3, 3, &acc_values);
        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let direct_result = refine_terminal(
            &terminal_polygon,
            outlet,
            flow_dir_direct,
            accumulation_direct,
            threshold,
        )
        .unwrap();

        struct MockRasterSource {
            flow_dir: FlowDirectionTile<Raw>,
            accumulation: AccumulationTile<Raw>,
        }

        impl RasterSource for MockRasterSource {
            fn load_flow_direction(
                &self,
                _path: &Path,
                _bbox: &Rect<f64>,
            ) -> Result<FlowDirectionTile<Raw>, RasterSourceError> {
                Ok(self.flow_dir.clone())
            }

            fn load_accumulation(
                &self,
                _path: &Path,
                _bbox: &Rect<f64>,
            ) -> Result<AccumulationTile<Raw>, RasterSourceError> {
                Ok(self.accumulation.clone())
            }
        }

        let source = MockRasterSource {
            flow_dir: make_flow_tile(3, 3, &fd_values),
            accumulation: make_acc_tile(3, 3, &acc_values),
        };

        let loader_result = refine_terminal_from_source(
            &source,
            Path::new("flow.tif"),
            Path::new("acc.tif"),
            &terminal_polygon,
            outlet,
            threshold,
        )
        .unwrap();

        let direct_coord = direct_result.snapped_coord();
        let loader_coord = loader_result.snapped_coord();
        assert!(
            (direct_coord.lon - loader_coord.lon).abs() < 1e-9,
            "snapped lon mismatch: direct={}, loader={}",
            direct_coord.lon,
            loader_coord.lon
        );
        assert!(
            (direct_coord.lat - loader_coord.lat).abs() < 1e-9,
            "snapped lat mismatch: direct={}, loader={}",
            direct_coord.lat,
            loader_coord.lat
        );

        use geo::algorithm::Area;
        let direct_area = direct_result.polygon().unsigned_area();
        let loader_area = loader_result.polygon().unsigned_area();
        assert!(
            (direct_area - loader_area).abs() < 0.001,
            "area mismatch: direct={direct_area}, loader={loader_area}"
        );
    }

    #[test]
    fn loader_propagates_raster_source_error() {
        struct FailingRasterSource;

        impl RasterSource for FailingRasterSource {
            fn load_flow_direction(
                &self,
                _path: &Path,
                _bbox: &Rect<f64>,
            ) -> Result<FlowDirectionTile<Raw>, RasterSourceError> {
                Err(RasterSourceError::FileNotFound {
                    path: "flow.tif".into(),
                })
            }

            fn load_accumulation(
                &self,
                _path: &Path,
                _bbox: &Rect<f64>,
            ) -> Result<AccumulationTile<Raw>, RasterSourceError> {
                Err(RasterSourceError::FileNotFound {
                    path: "acc.tif".into(),
                })
            }
        }

        let terminal_polygon = rect_polygon(0.0, 0.0, 3.0, -3.0);
        let outlet = GeoCoord::new(1.5, -1.5);
        let threshold = SnapThreshold::new(500);

        let err = refine_terminal_from_source(
            &FailingRasterSource,
            Path::new("flow.tif"),
            Path::new("acc.tif"),
            &terminal_polygon,
            outlet,
            threshold,
        )
        .unwrap_err();

        assert!(
            matches!(err, RefinementError::RasterLoad { .. }),
            "expected RasterLoad, got {err:?}"
        );
    }
}
