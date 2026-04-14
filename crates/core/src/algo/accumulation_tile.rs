//! Typed wrapper for flow accumulation rasters.

use std::marker::PhantomData;

use tracing::instrument;

use crate::algo::catchment_mask::CatchmentMask;
use crate::algo::coord::{GridCoord, GridDims};
use crate::algo::geo_transform::GeoTransform;
use crate::algo::raster_tile::{RasterTile, RasterTileError};
use crate::algo::snap_threshold::SnapThreshold;
use crate::algo::tile_state::{Masked, Raw};

/// Typed wrapper around a [`RasterTile<f32>`] holding upstream pixel counts.
///
/// The nodata sentinel is `f32::NAN`. Because `NaN != NaN`, the [`is_nodata`]
/// method uses [`f32::is_nan`] rather than the equality check in
/// [`RasterTile::is_nodata`]. All `get` accessors return `None` for NaN cells.
///
/// The typestate parameter `State` tracks whether this tile has been masked:
/// - [`Raw`]: unmasked tile; `apply_mask`, `set_raw`, and other mutating
///   operations are available.
/// - [`Masked`]: masked tile; ready for use in snapping and downstream analysis.
///
/// [`is_nodata`]: AccumulationTile::is_nodata
#[derive(Debug, Clone)]
pub struct AccumulationTile<State = Raw> {
    inner: RasterTile<f32>,
    _state: PhantomData<State>,
}

// ── Generic impl: read-only access available for any state ────────────────────

impl<S> AccumulationTile<S> {
    /// Returns the accumulation value at `cell`, or `None` for NaN cells.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows()` or `cell.col >= self.cols()`.
    pub fn get(&self, cell: GridCoord) -> Option<f32> {
        let value = self.inner.get(cell);
        if value.is_nan() { None } else { Some(value) }
    }

    /// Returns the accumulation value at a signed `(row, col)`, or `None` for
    /// out-of-bounds positions and NaN cells.
    pub fn get_checked(&self, row: isize, col: isize) -> Option<f32> {
        let value = self.inner.get_checked(row, col);
        if value.is_nan() { None } else { Some(value) }
    }

    /// Returns the raw `f32` value at `cell` without NaN filtering.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows()` or `cell.col >= self.cols()`.
    pub fn get_raw(&self, cell: GridCoord) -> f32 {
        self.inner.get(cell)
    }

    /// Returns `true` when `value` is NaN (the nodata sentinel).
    ///
    /// This method does **not** delegate to [`RasterTile::is_nodata`] because
    /// that uses `==`, which is always `false` for NaN values.
    pub fn is_nodata(&self, value: f32) -> bool {
        value.is_nan()
    }

    /// Returns [`GridDims`] with the tile dimensions.
    pub fn dims(&self) -> GridDims {
        self.inner.dims()
    }

    /// Returns the number of rows.
    pub fn rows(&self) -> usize {
        self.inner.rows()
    }

    /// Returns the number of columns.
    pub fn cols(&self) -> usize {
        self.inner.cols()
    }

    /// Returns a reference to the geo-transform.
    pub fn geo(&self) -> &GeoTransform {
        self.inner.geo()
    }

    /// Returns a reference to the underlying [`RasterTile<f32>`].
    pub fn inner(&self) -> &RasterTile<f32> {
        &self.inner
    }

    /// Consumes `self` and returns the underlying [`RasterTile<f32>`].
    pub fn into_inner(self) -> RasterTile<f32> {
        self.inner
    }
}

// ── Raw-only impl: mutation and masking ───────────────────────────────────────

impl AccumulationTile<Raw> {
    /// Creates a `dims.rows × dims.cols` tile filled with `f32::NAN` (nodata).
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`RasterTileError::EmptyTile`] | `dims.rows == 0` or `dims.cols == 0` |
    pub fn new(dims: GridDims, geo: GeoTransform) -> Result<Self, RasterTileError> {
        let inner = RasterTile::new(dims, f32::NAN, geo)?;
        Ok(Self {
            inner,
            _state: PhantomData,
        })
    }

    /// Wraps an existing [`RasterTile<f32>`] without copying.
    pub fn from_raw(tile: RasterTile<f32>) -> Self {
        Self {
            inner: tile,
            _state: PhantomData,
        }
    }

    /// Sets the raw value at `cell`.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows()` or `cell.col >= self.cols()`.
    pub fn set_raw(&mut self, cell: GridCoord, value: f32) {
        self.inner.set(cell, value);
    }

    /// Returns a boolean mask the same length as the tile data.
    ///
    /// Each element is `true` when the corresponding cell is not NaN and its
    /// value is greater than or equal to `threshold.as_f32()`.
    pub fn stream_mask(&self, threshold: SnapThreshold) -> Vec<bool> {
        let t = threshold.as_f32();
        self.inner
            .data()
            .iter()
            .map(|&value| !value.is_nan() && value >= t)
            .collect()
    }

    /// Returns `true` when the value at `cell` is not NaN and is greater
    /// than or equal to `threshold.as_f32()`.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows()` or `cell.col >= self.cols()`.
    pub fn exceeds_threshold(&self, cell: GridCoord, threshold: SnapThreshold) -> bool {
        let value = self.inner.get(cell);
        !value.is_nan() && value >= threshold.as_f32()
    }

    /// Mask out cells where `mask` is `false` by setting them to NaN (nodata).
    ///
    /// Consumes `self` and returns an [`AccumulationTile<Masked>`].
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`RasterTileError::DimensionMismatch`] | `mask.dims()` does not equal `self.dims()` |
    #[instrument(skip(self, mask))]
    pub fn apply_mask(
        mut self,
        mask: &CatchmentMask,
    ) -> Result<AccumulationTile<Masked>, RasterTileError> {
        let dims = self.dims();
        if mask.dims() != dims {
            return Err(RasterTileError::DimensionMismatch {
                expected: dims.rows * dims.cols,
                rows: dims.rows,
                cols: dims.cols,
                actual: mask.rows() * mask.cols(),
            });
        }
        for (cell, &keep) in self.inner.data_mut().iter_mut().zip(mask.data().iter()) {
            if !keep {
                *cell = f32::NAN;
            }
        }
        Ok(AccumulationTile {
            inner: self.inner,
            _state: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::catchment_mask::CatchmentMask;
    use crate::algo::coord::{GeoCoord, GridCoord, GridDims};

    fn simple_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    #[test]
    fn new_creates_nan_tile() {
        let tile = AccumulationTile::new(GridDims::new(3, 3), simple_geo()).unwrap();
        for row in 0..3 {
            for col in 0..3 {
                assert_eq!(
                    tile.get(GridCoord::new(row, col)),
                    None,
                    "expected None at ({row},{col})"
                );
            }
        }
    }

    #[test]
    fn set_and_get_valid() {
        let mut tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(0, 1), 42.5);
        assert_eq!(tile.get(GridCoord::new(0, 1)), Some(42.5));
    }

    #[test]
    fn get_nan_returns_none() {
        let mut tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(1, 0), f32::NAN);
        assert_eq!(tile.get(GridCoord::new(1, 0)), None);
    }

    #[test]
    fn get_checked_oob_returns_none() {
        let tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        // OOB returns the nodata sentinel (NaN), which our wrapper converts to None.
        assert_eq!(tile.get_checked(-1, 0), None);
        assert_eq!(tile.get_checked(0, -1), None);
        assert_eq!(tile.get_checked(10, 0), None);
        assert_eq!(tile.get_checked(0, 10), None);
    }

    #[test]
    fn is_nodata_nan_aware() {
        let tile = AccumulationTile::new(GridDims::new(1, 1), simple_geo()).unwrap();
        assert!(tile.is_nodata(f32::NAN));
        assert!(!tile.is_nodata(42.0));
        assert!(!tile.is_nodata(0.0));
    }

    #[test]
    fn stream_mask_basic() {
        // 2x2 tile with values [100.0, NaN, 600.0, 400.0], threshold 500.
        // Expected mask: [false, false, true, false]
        let raw = RasterTile::from_vec(
            vec![100.0_f32, f32::NAN, 600.0, 400.0],
            GridDims::new(2, 2),
            f32::NAN,
            simple_geo(),
        )
        .unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = tile.stream_mask(SnapThreshold::new(500));
        assert_eq!(mask, vec![false, false, true, false]);
    }

    #[test]
    fn stream_mask_all_above() {
        let raw = RasterTile::from_vec(
            vec![600.0_f32, 700.0, 800.0, 900.0],
            GridDims::new(2, 2),
            f32::NAN,
            simple_geo(),
        )
        .unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = tile.stream_mask(SnapThreshold::new(500));
        assert!(
            mask.iter().all(|&v| v),
            "all values should be above threshold"
        );
    }

    #[test]
    fn stream_mask_threshold_exact() {
        // Value exactly equal to threshold should be true (uses >=).
        let raw =
            RasterTile::from_vec(vec![500.0_f32], GridDims::new(1, 1), f32::NAN, simple_geo())
                .unwrap();
        let tile = AccumulationTile::from_raw(raw);
        let mask = tile.stream_mask(SnapThreshold::new(500));
        assert_eq!(mask, vec![true]);
    }

    #[test]
    fn exceeds_threshold_true() {
        let mut tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(0, 0), 1000.0);
        assert!(tile.exceeds_threshold(GridCoord::new(0, 0), SnapThreshold::new(500)));
    }

    #[test]
    fn exceeds_threshold_false() {
        let mut tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(0, 0), 100.0);
        assert!(!tile.exceeds_threshold(GridCoord::new(0, 0), SnapThreshold::new(500)));
    }

    #[test]
    fn exceeds_threshold_nan() {
        // NaN cells should always return false.
        let tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        assert!(!tile.exceeds_threshold(GridCoord::new(0, 0), SnapThreshold::new(0)));
    }

    #[test]
    fn from_raw_wraps_tile() {
        let mut raw = RasterTile::new(GridDims::new(2, 2), f32::NAN, simple_geo()).unwrap();
        raw.set(GridCoord::new(1, 1), 250.0);
        let tile = AccumulationTile::from_raw(raw);
        assert_eq!(tile.get(GridCoord::new(1, 1)), Some(250.0));
        assert_eq!(tile.get(GridCoord::new(0, 0)), None);
    }

    #[test]
    fn apply_mask_all_true() {
        let mut tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(0, 0), 100.0);
        tile.set_raw(GridCoord::new(0, 1), 200.0);
        tile.set_raw(GridCoord::new(1, 0), 300.0);
        tile.set_raw(GridCoord::new(1, 1), 400.0);
        let mask = CatchmentMask::new(vec![true; 4], GridDims::new(2, 2));
        let tile = tile.apply_mask(&mask).unwrap();
        assert_eq!(tile.get(GridCoord::new(0, 0)), Some(100.0));
        assert_eq!(tile.get(GridCoord::new(0, 1)), Some(200.0));
        assert_eq!(tile.get(GridCoord::new(1, 0)), Some(300.0));
        assert_eq!(tile.get(GridCoord::new(1, 1)), Some(400.0));
    }

    #[test]
    fn apply_mask_all_false() {
        let mut tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(0, 0), 100.0);
        tile.set_raw(GridCoord::new(0, 1), 200.0);
        tile.set_raw(GridCoord::new(1, 0), 300.0);
        tile.set_raw(GridCoord::new(1, 1), 400.0);
        let mask = CatchmentMask::new(vec![false; 4], GridDims::new(2, 2));
        let tile = tile.apply_mask(&mask).unwrap();
        for r in 0..2 {
            for c in 0..2 {
                assert_eq!(
                    tile.get(GridCoord::new(r, c)),
                    None,
                    "expected None at ({r},{c})"
                );
                assert!(tile.get_raw(GridCoord::new(r, c)).is_nan());
            }
        }
    }

    #[test]
    fn apply_mask_checkerboard() {
        let mut tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        tile.set_raw(GridCoord::new(0, 0), 100.0);
        tile.set_raw(GridCoord::new(0, 1), 200.0);
        tile.set_raw(GridCoord::new(1, 0), 300.0);
        tile.set_raw(GridCoord::new(1, 1), 400.0);
        let mask = CatchmentMask::new(vec![true, false, false, true], GridDims::new(2, 2));
        let tile = tile.apply_mask(&mask).unwrap();
        assert_eq!(tile.get(GridCoord::new(0, 0)), Some(100.0));
        assert!(tile.get_raw(GridCoord::new(0, 1)).is_nan());
        assert!(tile.get_raw(GridCoord::new(1, 0)).is_nan());
        assert_eq!(tile.get(GridCoord::new(1, 1)), Some(400.0));
    }

    #[test]
    fn apply_mask_dimension_mismatch() {
        let tile = AccumulationTile::new(GridDims::new(2, 2), simple_geo()).unwrap();
        let mask = CatchmentMask::new(vec![true; 6], GridDims::new(2, 3));
        let err = tile.apply_mask(&mask).unwrap_err();
        assert!(matches!(err, RasterTileError::DimensionMismatch { .. }));
    }
}
