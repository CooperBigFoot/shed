//! Geographic and grid coordinate types.

use std::fmt;

/// A geographic coordinate in EPSG:4326.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoCoord {
    /// Longitude (x).
    pub lon: f64,
    /// Latitude (y).
    pub lat: f64,
}

impl GeoCoord {
    /// Create a new geographic coordinate.
    pub fn new(lon: f64, lat: f64) -> Self {
        Self { lon, lat }
    }
}

impl fmt::Display for GeoCoord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.lon, self.lat)
    }
}

impl From<GeoCoord> for geo::Point<f64> {
    fn from(coord: GeoCoord) -> Self {
        geo::Point::new(coord.lon, coord.lat)
    }
}

impl From<geo::Point<f64>> for GeoCoord {
    fn from(point: geo::Point<f64>) -> Self {
        Self {
            lon: point.x(),
            lat: point.y(),
        }
    }
}

/// A cell position in raster grid space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GridCoord {
    /// Row index.
    pub row: usize,
    /// Column index.
    pub col: usize,
}

impl GridCoord {
    /// Create a new grid coordinate.
    pub fn new(row: usize, col: usize) -> Self {
        Self { row, col }
    }
}

/// Grid dimensions (rows × cols).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GridDims {
    /// Number of rows.
    pub rows: usize,
    /// Number of columns.
    pub cols: usize,
}

impl GridDims {
    /// Create new grid dimensions.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self { rows, cols }
    }

    /// Return the total number of cells.
    pub fn cell_count(&self) -> usize {
        self.rows * self.cols
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geo_coord_new() {
        let c = GeoCoord::new(10.5, 50.3);
        assert_eq!(c.lon, 10.5);
        assert_eq!(c.lat, 50.3);
    }

    #[test]
    fn geo_coord_display() {
        let c = GeoCoord::new(1.0, 2.0);
        assert_eq!(format!("{c}"), "(1, 2)");
    }

    #[test]
    fn grid_coord_new() {
        let c = GridCoord::new(3, 7);
        assert_eq!(c.row, 3);
        assert_eq!(c.col, 7);
    }

    #[test]
    fn grid_dims_cell_count() {
        let d = GridDims::new(5, 10);
        assert_eq!(d.cell_count(), 50);
    }

    #[test]
    fn grid_dims_equality() {
        assert_eq!(GridDims::new(3, 4), GridDims::new(3, 4));
        assert_ne!(GridDims::new(3, 4), GridDims::new(4, 3));
    }

    #[test]
    fn geo_coord_to_point_round_trip() {
        let coord = GeoCoord::new(10.5, 50.3);
        let point: geo::Point<f64> = coord.into();
        assert_eq!(point.x(), 10.5);
        assert_eq!(point.y(), 50.3);
    }

    #[test]
    fn point_to_geo_coord_round_trip() {
        let point = geo::Point::new(10.5, 50.3);
        let coord: GeoCoord = point.into();
        assert_eq!(coord.lon, 10.5);
        assert_eq!(coord.lat, 50.3);
    }
}
