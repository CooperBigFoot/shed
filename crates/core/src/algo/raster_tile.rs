//! Generic raster tile with sentinel nodata.

use std::fmt::Debug;
use std::ops::Index;

use crate::algo::coord::{GridCoord, GridDims};
use crate::algo::geo_transform::GeoTransform;

/// Errors from raster tile construction.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RasterTileError {
    /// The data length does not match rows × cols.
    #[error(
        "dimension mismatch: expected {expected} elements (rows={rows}, cols={cols}), got {actual}"
    )]
    DimensionMismatch {
        /// Expected number of elements.
        expected: usize,
        /// Number of rows.
        rows: usize,
        /// Number of columns.
        cols: usize,
        /// Actual data length.
        actual: usize,
    },
    /// Tile has zero rows or zero cols.
    #[error("empty tile: rows={rows}, cols={cols}")]
    EmptyTile {
        /// Number of rows.
        rows: usize,
        /// Number of columns.
        cols: usize,
    },
}

/// Row-major raster tile backed by a flat `Vec<T>` with a sentinel nodata value.
///
/// Indexing with `(isize, isize)` returns `&nodata` for out-of-bounds positions,
/// which makes neighbourhood traversal safe without explicit bounds checks at the
/// call site.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterTile<T: Copy + PartialEq + Debug> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
    nodata: T,
    geo: GeoTransform,
}

impl<T: Copy + PartialEq + Debug> RasterTile<T> {
    /// Creates a tile of size `dims.rows × dims.cols` filled with `nodata`.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EmptyTile`](RasterTileError::EmptyTile) | `dims.rows == 0` or `dims.cols == 0` |
    pub fn new(dims: GridDims, nodata: T, geo: GeoTransform) -> Result<Self, RasterTileError> {
        let rows = dims.rows;
        let cols = dims.cols;
        if rows == 0 || cols == 0 {
            return Err(RasterTileError::EmptyTile { rows, cols });
        }
        Ok(Self {
            data: vec![nodata; rows * cols],
            rows,
            cols,
            nodata,
            geo,
        })
    }

    /// Creates a tile from an existing flat data buffer.
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`EmptyTile`](RasterTileError::EmptyTile) | `dims.rows == 0` or `dims.cols == 0` |
    /// | [`DimensionMismatch`](RasterTileError::DimensionMismatch) | `data.len() != dims.rows * dims.cols` |
    pub fn from_vec(
        data: Vec<T>,
        dims: GridDims,
        nodata: T,
        geo: GeoTransform,
    ) -> Result<Self, RasterTileError> {
        let rows = dims.rows;
        let cols = dims.cols;
        if rows == 0 || cols == 0 {
            return Err(RasterTileError::EmptyTile { rows, cols });
        }
        let expected = rows * cols;
        if data.len() != expected {
            return Err(RasterTileError::DimensionMismatch {
                expected,
                rows,
                cols,
                actual: data.len(),
            });
        }
        Ok(Self {
            data,
            rows,
            cols,
            nodata,
            geo,
        })
    }

    /// Returns the value at `cell`.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows` or `cell.col >= self.cols`.
    pub fn get(&self, cell: GridCoord) -> T {
        self.data[cell.row * self.cols + cell.col]
    }

    /// Returns the value at `(row, col)`, or `nodata` for any out-of-bounds index.
    pub fn get_checked(&self, row: isize, col: isize) -> T {
        if row < 0 || col < 0 || row as usize >= self.rows || col as usize >= self.cols {
            self.nodata
        } else {
            self.data[row as usize * self.cols + col as usize]
        }
    }

    /// Sets the value at `cell`.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows` or `cell.col >= self.cols`.
    pub fn set(&mut self, cell: GridCoord, value: T) {
        self.data[cell.row * self.cols + cell.col] = value;
    }

    /// Returns `true` when `value` equals the nodata sentinel.
    pub fn is_nodata(&self, value: T) -> bool {
        value == self.nodata
    }

    /// Returns `GridDims` with the tile dimensions.
    pub fn dims(&self) -> GridDims {
        GridDims {
            rows: self.rows,
            cols: self.cols,
        }
    }

    /// Returns the number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Returns the nodata sentinel value.
    pub fn nodata(&self) -> T {
        self.nodata
    }

    /// Returns a reference to the geo-transform.
    pub fn geo(&self) -> &GeoTransform {
        &self.geo
    }

    /// Returns the flat data slice in row-major order.
    pub fn data(&self) -> &[T] {
        &self.data
    }

    /// Returns a mutable reference to the flat data slice in row-major order.
    pub fn data_mut(&mut self) -> &mut [T] {
        &mut self.data
    }
}

impl<T: Copy + PartialEq + Debug> Index<(isize, isize)> for RasterTile<T> {
    type Output = T;

    fn index(&self, (row, col): (isize, isize)) -> &T {
        if row < 0 || col < 0 || row as usize >= self.rows || col as usize >= self.cols {
            &self.nodata
        } else {
            &self.data[row as usize * self.cols + col as usize]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::coord::{GeoCoord, GridCoord, GridDims};

    fn simple_geo() -> GeoTransform {
        GeoTransform::new(GeoCoord::new(0.0, 0.0), 1.0, -1.0)
    }

    #[test]
    fn new_fills_with_nodata() {
        let tile = RasterTile::new(GridDims::new(3, 4), -999.0f32, simple_geo()).unwrap();
        assert!(tile.data().iter().all(|&v| v == -999.0f32));
    }

    #[test]
    fn from_vec_valid() {
        let data: Vec<i32> = (0..6).collect();
        let tile = RasterTile::from_vec(data, GridDims::new(2, 3), -1, simple_geo()).unwrap();
        assert_eq!(tile.get(GridCoord::new(0, 0)), 0);
        assert_eq!(tile.get(GridCoord::new(0, 2)), 2);
        assert_eq!(tile.get(GridCoord::new(1, 0)), 3);
        assert_eq!(tile.get(GridCoord::new(1, 2)), 5);
    }

    #[test]
    fn from_vec_dimension_mismatch() {
        let data = vec![1i32; 5];
        let err = RasterTile::from_vec(data, GridDims::new(2, 3), -1, simple_geo()).unwrap_err();
        assert!(matches!(
            err,
            RasterTileError::DimensionMismatch {
                expected: 6,
                rows: 2,
                cols: 3,
                actual: 5
            }
        ));
    }

    #[test]
    fn from_vec_empty_tile() {
        let err =
            RasterTile::from_vec(vec![1i32; 0], GridDims::new(0, 3), -1, simple_geo()).unwrap_err();
        assert!(matches!(
            err,
            RasterTileError::EmptyTile { rows: 0, cols: 3 }
        ));

        let err =
            RasterTile::from_vec(vec![1i32; 0], GridDims::new(3, 0), -1, simple_geo()).unwrap_err();
        assert!(matches!(
            err,
            RasterTileError::EmptyTile { rows: 3, cols: 0 }
        ));
    }

    #[test]
    fn get_checked_in_bounds() {
        let data: Vec<f32> = (0..9).map(|v| v as f32).collect();
        let tile = RasterTile::from_vec(data, GridDims::new(3, 3), -1.0f32, simple_geo()).unwrap();
        assert_eq!(tile.get_checked(1, 1), 4.0f32);
        assert_eq!(tile.get_checked(2, 2), 8.0f32);
    }

    #[test]
    fn get_checked_oob_negative() {
        let tile = RasterTile::new(GridDims::new(3, 3), -999.0f32, simple_geo()).unwrap();
        assert_eq!(tile.get_checked(-1, 0), -999.0f32);
        assert_eq!(tile.get_checked(0, -1), -999.0f32);
        assert_eq!(tile.get_checked(-5, -5), -999.0f32);
    }

    #[test]
    fn get_checked_oob_positive() {
        let tile = RasterTile::new(GridDims::new(3, 3), -999.0f32, simple_geo()).unwrap();
        assert_eq!(tile.get_checked(3, 0), -999.0f32);
        assert_eq!(tile.get_checked(0, 3), -999.0f32);
        assert_eq!(tile.get_checked(100, 100), -999.0f32);
    }

    #[test]
    fn set_and_get() {
        let mut tile = RasterTile::new(GridDims::new(3, 3), 0i32, simple_geo()).unwrap();
        tile.set(GridCoord::new(1, 2), 42);
        assert_eq!(tile.get(GridCoord::new(1, 2)), 42);
    }

    #[test]
    fn index_in_bounds() {
        let data: Vec<i32> = (0..6).collect();
        let tile = RasterTile::from_vec(data, GridDims::new(2, 3), -1, simple_geo()).unwrap();
        assert_eq!(tile[(1isize, 2isize)], 5);
    }

    #[test]
    fn index_oob_returns_nodata() {
        let tile = RasterTile::new(GridDims::new(3, 3), -999i32, simple_geo()).unwrap();
        assert_eq!(tile[(-1isize, 0isize)], -999);
        assert_eq!(tile[(100isize, 0isize)], -999);
        assert_eq!(tile[(0isize, -1isize)], -999);
        assert_eq!(tile[(0isize, 100isize)], -999);
    }

    #[test]
    fn is_nodata_check() {
        let tile = RasterTile::new(GridDims::new(2, 2), -1.0f32, simple_geo()).unwrap();
        assert!(tile.is_nodata(-1.0f32));
        assert!(!tile.is_nodata(0.0f32));
        assert!(!tile.is_nodata(42.0f32));
    }

    #[test]
    fn dims_accessors() {
        let tile = RasterTile::new(GridDims::new(5, 7), 0u8, simple_geo()).unwrap();
        assert_eq!(tile.rows(), 5);
        assert_eq!(tile.cols(), 7);
        assert_eq!(tile.dims(), GridDims::new(5, 7));
    }

    #[test]
    fn one_by_one_tile() {
        let tile = RasterTile::from_vec(vec![42u32], GridDims::new(1, 1), 0, simple_geo()).unwrap();
        assert_eq!(tile.get(GridCoord::new(0, 0)), 42);
        assert_eq!(tile.get_checked(0, 0), 42);
        assert_eq!(tile[(0isize, 0isize)], 42);
        assert_eq!(tile.rows(), 1);
        assert_eq!(tile.cols(), 1);
    }

    #[test]
    fn geo_accessor() {
        let geo = GeoTransform::new(GeoCoord::new(10.0, 50.0), 0.5, -0.5);
        let tile = RasterTile::new(GridDims::new(2, 2), 0i32, geo).unwrap();
        assert_eq!(tile.geo().origin_x(), 10.0);
        assert_eq!(tile.geo().origin_y(), 50.0);
        assert_eq!(tile.geo().pixel_width(), 0.5);
        assert_eq!(tile.geo().pixel_height(), -0.5);
    }
}
