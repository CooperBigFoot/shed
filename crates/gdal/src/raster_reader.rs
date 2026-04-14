//! Windowed raster reader backed by GDAL.
//!
//! Implements [`RasterSource`] for GeoTIFF files. Reads only the geographic
//! sub-region specified by a bounding box, reducing memory pressure for large
//! basins.

use std::path::Path;

use gdal::Dataset;
use geo::Rect;
use hfx_core::FlowDirEncoding;
use tracing::{debug, instrument};

use shed_core::algo::accumulation_tile::AccumulationTile;
use shed_core::algo::coord::{GeoCoord, GridDims};
use shed_core::algo::flow_direction_tile::FlowDirectionTile;
use shed_core::algo::geo_transform::GeoTransform;
use shed_core::algo::raster_tile::RasterTile;
use shed_core::algo::tile_state::Raw;
use shed_core::algo::traits::{RasterSource, RasterSourceError};

use crate::error::RasterReadError;

/// GDAL-backed implementation of [`RasterSource`].
///
/// Reads windowed GeoTIFF tiles using the GDAL raster I/O API. The encoding
/// used to decode D8 flow direction bytes is configurable at construction time;
/// it defaults to [`FlowDirEncoding::Esri`].
#[derive(Debug, Clone)]
pub struct GdalRasterSource {
    encoding: FlowDirEncoding,
}

impl GdalRasterSource {
    /// Create a new `GdalRasterSource` with the default ESRI flow direction encoding.
    pub fn new() -> Self {
        Self { encoding: FlowDirEncoding::Esri }
    }

    /// Set the flow direction encoding for this source (builder method).
    pub fn with_encoding(encoding: FlowDirEncoding) -> Self {
        Self { encoding }
    }
}

impl Default for GdalRasterSource {
    fn default() -> Self {
        Self::new()
    }
}

impl RasterSource for GdalRasterSource {
    #[instrument(skip(self, path, bbox), fields(path = %path.display()))]
    fn load_flow_direction(
        &self,
        path: &Path,
        bbox: &Rect<f64>,
    ) -> Result<FlowDirectionTile<Raw>, RasterSourceError> {
        let path_str = path.display().to_string();

        let ds = open_dataset(path).map_err(|e| map_raster_read_error(e, &path_str))?;

        let raw_gt = ds
            .geo_transform()
            .map_err(|e| RasterSourceError::ReadFailed {
                path: path_str.clone(),
                reason: e.to_string(),
            })?;
        let gt = gdal_to_geo_transform(&raw_gt);

        let (raster_width, raster_height) = ds.raster_size();
        let (x_off, y_off, x_size, y_size) =
            bbox_to_pixel_window(&gt, bbox, raster_width, raster_height);

        if x_size == 0 || y_size == 0 {
            return Err(RasterSourceError::EmptyWindow { path: path_str });
        }

        let band = ds.rasterband(1).map_err(|e| RasterSourceError::ReadFailed {
            path: path_str.clone(),
            reason: e.to_string(),
        })?;

        let buf = band
            .read_as::<u8>((x_off, y_off), (x_size, y_size), (x_size, y_size), None)
            .map_err(|e| RasterSourceError::ReadFailed {
                path: path_str.clone(),
                reason: e.to_string(),
            })?;

        let window_gt = window_geo_transform(&gt, x_off, y_off);
        let dims = GridDims::new(y_size, x_size);

        debug!(x_off, y_off, x_size, y_size, "read windowed u8 band");

        let tile = RasterTile::from_vec(buf.data().to_vec(), dims, 255u8, window_gt).map_err(
            |e| RasterSourceError::TileConstruction { reason: e.to_string() },
        )?;

        Ok(FlowDirectionTile::from_raw(tile, self.encoding))
    }

    #[instrument(skip(self, path, bbox), fields(path = %path.display()))]
    fn load_accumulation(
        &self,
        path: &Path,
        bbox: &Rect<f64>,
    ) -> Result<AccumulationTile<Raw>, RasterSourceError> {
        let path_str = path.display().to_string();

        let ds = open_dataset(path).map_err(|e| map_raster_read_error(e, &path_str))?;

        let raw_gt = ds
            .geo_transform()
            .map_err(|e| RasterSourceError::ReadFailed {
                path: path_str.clone(),
                reason: e.to_string(),
            })?;
        let gt = gdal_to_geo_transform(&raw_gt);

        let (raster_width, raster_height) = ds.raster_size();
        let (x_off, y_off, x_size, y_size) =
            bbox_to_pixel_window(&gt, bbox, raster_width, raster_height);

        if x_size == 0 || y_size == 0 {
            return Err(RasterSourceError::EmptyWindow { path: path_str });
        }

        let band = ds.rasterband(1).map_err(|e| RasterSourceError::ReadFailed {
            path: path_str.clone(),
            reason: e.to_string(),
        })?;

        let gdal_nodata = band.no_data_value();

        let buf = band
            .read_as::<f32>((x_off, y_off), (x_size, y_size), (x_size, y_size), None)
            .map_err(|e| RasterSourceError::ReadFailed {
                path: path_str.clone(),
                reason: e.to_string(),
            })?;

        let data = replace_nodata_with_nan(buf.data().to_vec(), gdal_nodata);
        let window_gt = window_geo_transform(&gt, x_off, y_off);
        let dims = GridDims::new(y_size, x_size);

        debug!(x_off, y_off, x_size, y_size, "read windowed f32 band");

        let tile =
            RasterTile::from_vec(data, dims, f32::NAN, window_gt).map_err(|e| {
                RasterSourceError::TileConstruction { reason: e.to_string() }
            })?;

        Ok(AccumulationTile::from_raw(tile))
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Open a GDAL dataset, returning a typed error on failure.
fn open_dataset(path: &Path) -> Result<Dataset, RasterReadError> {
    if !path.exists() {
        return Err(RasterReadError::FileNotFound {
            path: path.display().to_string(),
        });
    }
    Dataset::open(path).map_err(|e| RasterReadError::GdalOpen {
        path: path.display().to_string(),
        reason: e.to_string(),
    })
}

/// Map a GDAL 6-element affine array to a [`GeoTransform`].
///
/// GDAL layout: `[origin_x, pixel_w, skew_x, origin_y, skew_y, pixel_h]`
/// Indices 0 and 3 are the top-left corner; 1 is the pixel width; 5 is the
/// pixel height (negative for north-up rasters).
fn gdal_to_geo_transform(gt: &[f64; 6]) -> GeoTransform {
    GeoTransform::new(GeoCoord::new(gt[0], gt[3]), gt[1], gt[5])
}

/// Convert a geographic [`Rect`] bounding box to a pixel window `(x_off, y_off, x_size, y_size)`.
///
/// The returned offsets and sizes are clamped to `[0, raster_width)` ×
/// `[0, raster_height)` so that a bbox that slightly overshoots the raster
/// edge is handled gracefully.
fn bbox_to_pixel_window(
    gt: &GeoTransform,
    bbox: &Rect<f64>,
    raster_width: usize,
    raster_height: usize,
) -> (isize, isize, usize, usize) {
    // Column range — x increases left to right, pixel_width is positive.
    let min_col_f = (bbox.min().x - gt.origin_x()) / gt.pixel_width();
    let max_col_f = (bbox.max().x - gt.origin_x()) / gt.pixel_width();
    let min_col = min_col_f.floor() as isize;
    let max_col = max_col_f.ceil() as isize;

    // Row range — y decreases top to bottom, pixel_height is negative.
    // Larger y → smaller row (closer to top).
    let min_row_f = (bbox.max().y - gt.origin_y()) / gt.pixel_height();
    let max_row_f = (bbox.min().y - gt.origin_y()) / gt.pixel_height();
    let min_row = min_row_f.floor() as isize;
    let max_row = max_row_f.ceil() as isize;

    // Clamp to raster bounds.
    let rw = raster_width as isize;
    let rh = raster_height as isize;

    let x_off = min_col.max(0).min(rw);
    let y_off = min_row.max(0).min(rh);
    let x_end = max_col.max(0).min(rw);
    let y_end = max_row.max(0).min(rh);

    let x_size = (x_end - x_off).max(0) as usize;
    let y_size = (y_end - y_off).max(0) as usize;

    (x_off, y_off, x_size, y_size)
}

/// Adjust the geo-transform origin to reflect a windowed read starting at
/// `(x_off, y_off)` pixels from the raster top-left.
fn window_geo_transform(gt: &GeoTransform, x_off: isize, y_off: isize) -> GeoTransform {
    let origin = GeoCoord::new(
        gt.origin_x() + x_off as f64 * gt.pixel_width(),
        gt.origin_y() + y_off as f64 * gt.pixel_height(),
    );
    GeoTransform::new(origin, gt.pixel_width(), gt.pixel_height())
}

/// Replace any occurrence of `gdal_nodata` in `data` with `f32::NAN`.
///
/// When the GDAL nodata is already NaN, or absent, data is returned unchanged.
fn replace_nodata_with_nan(mut data: Vec<f32>, gdal_nodata: Option<f64>) -> Vec<f32> {
    if let Some(nd) = gdal_nodata {
        let nd_f32 = nd as f32;
        if !nd_f32.is_nan() {
            for v in data.iter_mut() {
                if *v == nd_f32 {
                    *v = f32::NAN;
                }
            }
        }
    }
    data
}

/// Map a `RasterReadError` to the trait's `RasterSourceError`.
fn map_raster_read_error(e: RasterReadError, _path: &str) -> RasterSourceError {
    match e {
        RasterReadError::FileNotFound { path } => RasterSourceError::FileNotFound { path },
        RasterReadError::GdalOpen { path, reason } => {
            RasterSourceError::OpenFailed { path, reason }
        }
        RasterReadError::GdalRead { path, reason } => {
            RasterSourceError::ReadFailed { path, reason }
        }
        RasterReadError::EmptyWindow { path } => RasterSourceError::EmptyWindow { path },
        RasterReadError::TileConstruction { reason } => {
            RasterSourceError::TileConstruction { reason }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use geo::coord;

    use super::*;

    fn standard_gt() -> GeoTransform {
        // origin (10.0, 50.0), pixel_width 0.5, pixel_height -0.5
        gdal_to_geo_transform(&[10.0, 0.5, 0.0, 50.0, 0.0, -0.5])
    }

    // ── gdal_to_geo_transform ────────────────────────────────────────────────

    #[test]
    fn gdal_to_geo_transform_extracts_fields() {
        let gt = gdal_to_geo_transform(&[10.0, 0.5, 0.0, 50.0, 0.0, -0.5]);
        assert_eq!(gt.origin_x(), 10.0);
        assert_eq!(gt.origin_y(), 50.0);
        assert_eq!(gt.pixel_width(), 0.5);
        assert_eq!(gt.pixel_height(), -0.5);
    }

    #[test]
    fn gdal_to_geo_transform_merit_resolution() {
        let pixel = 1.0_f64 / 1200.0;
        let gt = gdal_to_geo_transform(&[-180.0, pixel, 0.0, 90.0, 0.0, -pixel]);
        assert!((gt.origin_x() - (-180.0)).abs() < f64::EPSILON);
        assert!((gt.origin_y() - 90.0).abs() < f64::EPSILON);
        assert!((gt.pixel_width() - pixel).abs() < 1e-15);
        assert!((gt.pixel_height() - (-pixel)).abs() < 1e-15);
    }

    // ── bbox_to_pixel_window ─────────────────────────────────────────────────

    #[test]
    fn bbox_to_pixel_window_full_raster() {
        // Raster: origin (10, 50), pixel 0.5 × -0.5, 20 × 10 pixels.
        // Geographic extent: x [10, 20], y [45, 50].
        let gt = standard_gt();
        let bbox = Rect::new(
            coord! { x: 10.0, y: 45.0 },
            coord! { x: 20.0, y: 50.0 },
        );
        let (x_off, y_off, x_size, y_size) = bbox_to_pixel_window(&gt, &bbox, 20, 10);
        assert_eq!(x_off, 0);
        assert_eq!(y_off, 0);
        assert_eq!(x_size, 20);
        assert_eq!(y_size, 10);
    }

    #[test]
    fn bbox_to_pixel_window_subset() {
        // Same raster; bbox covers cols 2–6 and rows 2–5.
        let gt = standard_gt(); // origin (10, 50), pixel (0.5, -0.5)
        // x: col 2 → x=11, col 6 → x=13   => bbox x [11.0, 13.0]
        // y: row 2 → y=49, row 5 → y=47.5  => bbox y [47.5, 49.0]
        let bbox = Rect::new(
            coord! { x: 11.0, y: 47.5 },
            coord! { x: 13.0, y: 49.0 },
        );
        let (x_off, y_off, x_size, y_size) = bbox_to_pixel_window(&gt, &bbox, 20, 10);
        assert_eq!(x_off, 2);
        assert_eq!(y_off, 2);
        assert_eq!(x_size, 4);
        assert_eq!(y_size, 3);
    }

    #[test]
    fn bbox_to_pixel_window_clamps_to_bounds() {
        // Bbox extending beyond the raster in all directions should clamp.
        let gt = standard_gt();
        let bbox = Rect::new(
            coord! { x: 5.0, y: 40.0 },
            coord! { x: 30.0, y: 60.0 },
        );
        let (x_off, y_off, x_size, y_size) = bbox_to_pixel_window(&gt, &bbox, 20, 10);
        assert_eq!(x_off, 0);
        assert_eq!(y_off, 0);
        assert_eq!(x_size, 20);
        assert_eq!(y_size, 10);
    }

    #[test]
    fn bbox_to_pixel_window_zero_size_when_outside() {
        // Bbox entirely to the left of the raster: x_size should be 0.
        let gt = standard_gt(); // origin x = 10
        let bbox = Rect::new(
            coord! { x: 0.0, y: 45.0 },
            coord! { x: 5.0, y: 50.0 },
        );
        let (_, _, x_size, _) = bbox_to_pixel_window(&gt, &bbox, 20, 10);
        assert_eq!(x_size, 0);
    }

    // ── window_geo_transform ─────────────────────────────────────────────────

    #[test]
    fn window_geo_transform_adjusts_origin() {
        let gt = standard_gt(); // origin (10, 50), pixel (0.5, -0.5)
        let wgt = window_geo_transform(&gt, 4, 3);
        // new origin_x = 10 + 4 * 0.5 = 12
        assert!((wgt.origin_x() - 12.0).abs() < f64::EPSILON);
        // new origin_y = 50 + 3 * (-0.5) = 48.5
        assert!((wgt.origin_y() - 48.5).abs() < f64::EPSILON);
        // pixel sizes unchanged
        assert_eq!(wgt.pixel_width(), gt.pixel_width());
        assert_eq!(wgt.pixel_height(), gt.pixel_height());
    }

    #[test]
    fn window_geo_transform_zero_offset_is_identity() {
        let gt = standard_gt();
        let wgt = window_geo_transform(&gt, 0, 0);
        assert_eq!(wgt.origin_x(), gt.origin_x());
        assert_eq!(wgt.origin_y(), gt.origin_y());
        assert_eq!(wgt.pixel_width(), gt.pixel_width());
        assert_eq!(wgt.pixel_height(), gt.pixel_height());
    }

    // ── replace_nodata_with_nan ──────────────────────────────────────────────

    #[test]
    fn replace_nodata_with_nan_replaces_sentinel() {
        let data = vec![1.0_f32, -9999.0, 3.0, -9999.0];
        let result = replace_nodata_with_nan(data, Some(-9999.0));
        assert_eq!(result[0], 1.0);
        assert!(result[1].is_nan());
        assert_eq!(result[2], 3.0);
        assert!(result[3].is_nan());
    }

    #[test]
    fn replace_nodata_with_nan_no_nodata_unchanged() {
        let data = vec![1.0_f32, 2.0, 3.0];
        let result = replace_nodata_with_nan(data.clone(), None);
        assert_eq!(result, data);
    }

    #[test]
    fn replace_nodata_with_nan_gdal_nodata_already_nan() {
        // When GDAL reports NaN as nodata, data must be returned unchanged.
        let data = vec![1.0_f32, f32::NAN, 3.0];
        let result = replace_nodata_with_nan(data, Some(f64::NAN));
        assert_eq!(result[0], 1.0);
        assert!(result[1].is_nan()); // was already NaN
        assert_eq!(result[2], 3.0);
    }

    #[test]
    fn replace_nodata_with_nan_no_matching_values() {
        let data = vec![1.0_f32, 2.0, 3.0];
        let result = replace_nodata_with_nan(data.clone(), Some(99.0));
        assert_eq!(result, data);
    }

    // ── File-not-found error path ────────────────────────────────────────────

    #[test]
    fn load_flow_direction_file_not_found() {
        let src = GdalRasterSource::new();
        let bbox = Rect::new(coord! { x: 0.0, y: 0.0 }, coord! { x: 1.0, y: 1.0 });
        let err = src
            .load_flow_direction(
                std::path::Path::new("/nonexistent/path/that/will/never/exist.tif"),
                &bbox,
            )
            .expect_err("expected FileNotFound error");
        assert!(
            matches!(err, RasterSourceError::FileNotFound { .. }),
            "expected FileNotFound, got: {err}"
        );
    }

    #[test]
    fn load_accumulation_file_not_found() {
        let src = GdalRasterSource::new();
        let bbox = Rect::new(coord! { x: 0.0, y: 0.0 }, coord! { x: 1.0, y: 1.0 });
        let err = src
            .load_accumulation(
                std::path::Path::new("/nonexistent/path/that/will/never/exist.tif"),
                &bbox,
            )
            .expect_err("expected FileNotFound error");
        assert!(
            matches!(err, RasterSourceError::FileNotFound { .. }),
            "expected FileNotFound, got: {err}"
        );
    }
}
