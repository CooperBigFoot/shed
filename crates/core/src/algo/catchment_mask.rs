//! Boolean catchment mask identifying upstream cells.

use crate::algo::coord::{GridCoord, GridDims};

/// Boolean mask identifying all cells upstream of a pour point.
#[derive(Debug, Clone, PartialEq)]
pub struct CatchmentMask {
    data: Vec<bool>,
    rows: usize,
    cols: usize,
}

impl CatchmentMask {
    /// Construct a mask from traced upstream data and dimensions.
    pub(crate) fn from_traced(data: Vec<bool>, dims: GridDims) -> Self {
        debug_assert_eq!(
            data.len(),
            dims.rows * dims.cols,
            "mask data length must equal rows * cols"
        );
        Self {
            data,
            rows: dims.rows,
            cols: dims.cols,
        }
    }

    /// Construct a mask from raw boolean data and dimensions.
    pub fn new(data: Vec<bool>, dims: GridDims) -> Self {
        debug_assert_eq!(
            data.len(),
            dims.rows * dims.cols,
            "mask data length must equal rows * cols"
        );
        Self {
            data,
            rows: dims.rows,
            cols: dims.cols,
        }
    }

    /// Returns whether a cell is included in the upstream mask.
    pub fn contains(&self, cell: GridCoord) -> bool {
        self.data[cell.row * self.cols + cell.col]
    }

    /// Returns the grid dimensions.
    pub fn dims(&self) -> GridDims {
        GridDims::new(self.rows, self.cols)
    }

    /// Returns the number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Returns the count of cells included in the upstream mask.
    pub fn cell_count(&self) -> usize {
        self.data.iter().filter(|&&v| v).count()
    }

    /// Returns a slice over the underlying boolean data.
    pub fn data(&self) -> &[bool] {
        &self.data
    }

    /// Consumes `self` and returns the underlying boolean data vector.
    pub fn into_data(self) -> Vec<bool> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::coord::{GridCoord, GridDims};

    #[test]
    fn new_and_contains() {
        let data = vec![true, false, false, true];
        let mask = CatchmentMask::new(data, GridDims::new(2, 2));
        assert!(mask.contains(GridCoord::new(0, 0)));
        assert!(!mask.contains(GridCoord::new(0, 1)));
        assert!(!mask.contains(GridCoord::new(1, 0)));
        assert!(mask.contains(GridCoord::new(1, 1)));
    }

    #[test]
    fn dims_accessors() {
        let mask = CatchmentMask::new(vec![false; 6], GridDims::new(2, 3));
        assert_eq!(mask.rows(), 2);
        assert_eq!(mask.cols(), 3);
        assert_eq!(mask.dims(), GridDims::new(2, 3));
    }

    #[test]
    fn cell_count_all_true() {
        let mask = CatchmentMask::new(vec![true; 9], GridDims::new(3, 3));
        assert_eq!(mask.cell_count(), 9);
    }

    #[test]
    fn cell_count_all_false() {
        let mask = CatchmentMask::new(vec![false; 4], GridDims::new(2, 2));
        assert_eq!(mask.cell_count(), 0);
    }

    #[test]
    fn cell_count_mixed() {
        let data = vec![true, false, true, false, true, false];
        let mask = CatchmentMask::new(data, GridDims::new(2, 3));
        assert_eq!(mask.cell_count(), 3);
    }

    #[test]
    fn data_slice() {
        let data = vec![true, false, true];
        let mask = CatchmentMask::new(data.clone(), GridDims::new(1, 3));
        assert_eq!(mask.data(), data.as_slice());
    }

    #[test]
    fn into_data_round_trip() {
        let data = vec![true, false, false, true];
        let mask = CatchmentMask::new(data.clone(), GridDims::new(2, 2));
        assert_eq!(mask.into_data(), data);
    }

    #[test]
    fn from_traced_constructs_mask() {
        let data = vec![false, true, true, false];
        let mask = CatchmentMask::from_traced(data, GridDims::new(2, 2));
        assert!(!mask.contains(GridCoord::new(0, 0)));
        assert!(mask.contains(GridCoord::new(0, 1)));
        assert!(mask.contains(GridCoord::new(1, 0)));
        assert!(!mask.contains(GridCoord::new(1, 1)));
    }
}
