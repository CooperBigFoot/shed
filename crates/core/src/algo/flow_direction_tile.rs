//! Typed wrapper for flow direction rasters with encoding-aware decoding.

use std::marker::PhantomData;

use hfx_core::FlowDirEncoding;
use tracing::instrument;

use crate::algo::catchment_mask::CatchmentMask;
use crate::algo::coord::{GridCoord, GridDims};
use crate::algo::flow_dir::FlowDir;
use crate::algo::geo_transform::GeoTransform;
use crate::algo::raster_tile::{RasterTile, RasterTileError};
use crate::algo::tile_state::{Masked, Raw};

/// Nodata sentinel for D8 flow direction tiles.
///
/// 255 is the conventional nodata value for byte-encoded D8 rasters.
const NODATA: u8 = 255;

/// Typed wrapper around a [`RasterTile<u8>`] holding D8 flow direction bytes.
///
/// Raw bytes are decoded on read via [`FlowDir::from_encoded`], dispatching to
/// ESRI or TauDEM decoding based on the stored [`FlowDirEncoding`]. Cells with
/// value 255 (nodata sentinel), 0 (nodata for both conventions), or any
/// unrecognised byte are surfaced as `None` from the accessor methods.
///
/// The typestate parameter `State` tracks whether this tile has been masked:
/// - [`Raw`]: unmasked tile; `apply_mask`, `set_raw`, and other mutating
///   operations are available.
/// - [`Masked`]: masked tile; unmasked cells have been set to 255 (nodata).
#[derive(Debug, Clone)]
pub struct FlowDirectionTile<State = Raw> {
    inner: RasterTile<u8>,
    encoding: FlowDirEncoding,
    _state: PhantomData<State>,
}

// ── Generic impl: read-only access available for any state ────────────────────

impl<S> FlowDirectionTile<S> {
    /// Returns the decoded [`FlowDir`] at `cell`, or `None` when the
    /// cell is nodata or carries an invalid encoding.
    ///
    /// - 255 → `None` (nodata sentinel)
    /// - 0 → `None` (nodata for both conventions)
    /// - Invalid byte → `None` (treated as nodata)
    /// - Valid code → `Some(dir)`
    pub fn get(&self, cell: GridCoord) -> Option<FlowDir> {
        let raw = self.inner.get(cell);
        decode(raw, self.encoding)
    }

    /// Returns the decoded [`FlowDir`] at a signed `(row, col)`, or `None`
    /// for out-of-bounds positions or nodata / invalid bytes.
    pub fn get_checked(&self, row: isize, col: isize) -> Option<FlowDir> {
        let raw = self.inner.get_checked(row, col);
        decode(raw, self.encoding)
    }

    /// Returns the raw byte at `cell` without decoding.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows()` or `cell.col >= self.cols()`.
    pub fn get_raw(&self, cell: GridCoord) -> u8 {
        self.inner.get(cell)
    }

    /// Returns the encoding convention used to decode raw bytes.
    pub fn encoding(&self) -> FlowDirEncoding {
        self.encoding
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

    /// Returns a reference to the underlying [`RasterTile<u8>`].
    pub fn inner(&self) -> &RasterTile<u8> {
        &self.inner
    }

    /// Consumes `self` and returns the underlying [`RasterTile<u8>`].
    pub fn into_inner(self) -> RasterTile<u8> {
        self.inner
    }
}

// ── Raw-only impl: mutation and masking ───────────────────────────────────────

impl FlowDirectionTile<Raw> {
    /// Creates a `dims.rows × dims.cols` tile filled with the nodata sentinel (255).
    ///
    /// # Errors
    ///
    /// | Variant | When |
    /// |---|---|
    /// | [`RasterTileError::EmptyTile`] | `dims.rows == 0` or `dims.cols == 0` |
    pub fn new(
        dims: GridDims,
        geo: GeoTransform,
        encoding: FlowDirEncoding,
    ) -> Result<Self, RasterTileError> {
        let inner = RasterTile::new(dims, NODATA, geo)?;
        Ok(Self {
            inner,
            encoding,
            _state: PhantomData,
        })
    }

    /// Wraps an existing [`RasterTile<u8>`] with a known encoding without copying.
    pub fn from_raw(tile: RasterTile<u8>, encoding: FlowDirEncoding) -> Self {
        Self {
            inner: tile,
            encoding,
            _state: PhantomData,
        }
    }

    /// Sets the raw byte at `cell` without validation.
    ///
    /// # Panics
    ///
    /// Panics if `cell.row >= self.rows()` or `cell.col >= self.cols()`.
    pub fn set_raw(&mut self, cell: GridCoord, value: u8) {
        self.inner.set(cell, value);
    }

    /// Mask out cells where `mask` is `false` by setting them to the nodata sentinel (255).
    ///
    /// Consumes `self` and returns a [`FlowDirectionTile<Masked>`].
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
    ) -> Result<FlowDirectionTile<Masked>, RasterTileError> {
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
                *cell = 255;
            }
        }
        Ok(FlowDirectionTile {
            inner: self.inner,
            encoding: self.encoding,
            _state: PhantomData,
        })
    }
}

/// Decodes a raw D8 byte into an `Option<FlowDir>` using the given encoding.
///
/// Returns `None` for the nodata sentinel (255), the shared nodata encoding (0),
/// and any unrecognised byte value.
fn decode(raw: u8, encoding: FlowDirEncoding) -> Option<FlowDir> {
    if raw == NODATA {
        return None;
    }
    match FlowDir::from_encoded(raw, encoding) {
        Ok(dir) => dir,
        Err(_) => None, // treat invalid encodings as nodata
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

    // ── Ported ESRI tests ────────────────────────────────────────────────────

    #[test]
    fn new_creates_nodata_tile() {
        let tile =
            FlowDirectionTile::new(GridDims::new(3, 3), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
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
    fn set_and_get_valid_dir() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(3, 3), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        // ESRI code 4 = South
        tile.set_raw(GridCoord::new(1, 1), 4);
        assert_eq!(tile.get(GridCoord::new(1, 1)), Some(FlowDir::South));
    }

    #[test]
    fn get_nodata_returns_none() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 255);
        assert_eq!(tile.get(GridCoord::new(0, 0)), None);
    }

    #[test]
    fn get_zero_returns_none() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 0);
        assert_eq!(tile.get(GridCoord::new(0, 0)), None);
    }

    #[test]
    fn get_invalid_returns_none() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        // 3 is not a valid ESRI D8 code
        tile.set_raw(GridCoord::new(0, 0), 3);
        assert_eq!(tile.get(GridCoord::new(0, 0)), None);
    }

    #[test]
    fn get_checked_oob_returns_none() {
        let tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        assert_eq!(tile.get_checked(-1, 0), None);
        assert_eq!(tile.get_checked(0, -1), None);
        assert_eq!(tile.get_checked(10, 0), None);
        assert_eq!(tile.get_checked(0, 10), None);
    }

    #[test]
    fn get_checked_valid() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(3, 3), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        // ESRI code 64 = North
        tile.set_raw(GridCoord::new(2, 2), 64);
        assert_eq!(tile.get_checked(2, 2), Some(FlowDir::North));
    }

    #[test]
    fn from_raw_wraps_tile() {
        let mut raw = RasterTile::new(GridDims::new(2, 2), 255u8, simple_geo()).unwrap();
        // ESRI code 1 = East
        raw.set(GridCoord::new(0, 1), 1);
        let tile = FlowDirectionTile::from_raw(raw, FlowDirEncoding::Esri);
        assert_eq!(tile.get(GridCoord::new(0, 1)), Some(FlowDir::East));
        assert_eq!(tile.get(GridCoord::new(0, 0)), None);
    }

    #[test]
    fn inner_escape_hatch() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 128);
        let inner = tile.inner();
        assert_eq!(inner.get(GridCoord::new(0, 0)), 128u8);
    }

    #[test]
    fn apply_mask_all_true() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 1); // East
        tile.set_raw(GridCoord::new(0, 1), 4); // South
        tile.set_raw(GridCoord::new(1, 0), 16); // West
        tile.set_raw(GridCoord::new(1, 1), 64); // North
        let mask = CatchmentMask::new(vec![true; 4], GridDims::new(2, 2));
        let tile = tile.apply_mask(&mask).unwrap();
        assert_eq!(tile.get(GridCoord::new(0, 0)), Some(FlowDir::East));
        assert_eq!(tile.get(GridCoord::new(0, 1)), Some(FlowDir::South));
        assert_eq!(tile.get(GridCoord::new(1, 0)), Some(FlowDir::West));
        assert_eq!(tile.get(GridCoord::new(1, 1)), Some(FlowDir::North));
    }

    #[test]
    fn apply_mask_all_false() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 1);
        tile.set_raw(GridCoord::new(0, 1), 4);
        tile.set_raw(GridCoord::new(1, 0), 16);
        tile.set_raw(GridCoord::new(1, 1), 64);
        let mask = CatchmentMask::new(vec![false; 4], GridDims::new(2, 2));
        let tile = tile.apply_mask(&mask).unwrap();
        for r in 0..2 {
            for c in 0..2 {
                assert_eq!(
                    tile.get(GridCoord::new(r, c)),
                    None,
                    "expected None at ({r},{c})"
                );
                assert_eq!(tile.get_raw(GridCoord::new(r, c)), 255);
            }
        }
    }

    #[test]
    fn apply_mask_checkerboard() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 1);
        tile.set_raw(GridCoord::new(0, 1), 4);
        tile.set_raw(GridCoord::new(1, 0), 16);
        tile.set_raw(GridCoord::new(1, 1), 64);
        let mask = CatchmentMask::new(vec![true, false, false, true], GridDims::new(2, 2));
        let tile = tile.apply_mask(&mask).unwrap();
        assert_eq!(tile.get(GridCoord::new(0, 0)), Some(FlowDir::East));
        assert_eq!(tile.get_raw(GridCoord::new(0, 1)), 255);
        assert_eq!(tile.get_raw(GridCoord::new(1, 0)), 255);
        assert_eq!(tile.get(GridCoord::new(1, 1)), Some(FlowDir::North));
    }

    #[test]
    fn apply_mask_dimension_mismatch() {
        let tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        let mask = CatchmentMask::new(vec![true; 6], GridDims::new(2, 3));
        let err = tile.apply_mask(&mask).unwrap_err();
        assert!(matches!(err, RasterTileError::DimensionMismatch { .. }));
    }

    // ── New TauDEM encoding tests ────────────────────────────────────────────

    #[test]
    fn taudem_new_creates_nodata_tile() {
        let tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        for row in 0..2 {
            for col in 0..2 {
                assert_eq!(
                    tile.get(GridCoord::new(row, col)),
                    None,
                    "expected None at ({row},{col})"
                );
            }
        }
    }

    #[test]
    fn taudem_get_decodes_correctly() {
        // TauDEM: 1=E, 2=NE, 3=N, 4=NW, 5=W, 6=SW, 7=S, 8=SE
        let cases: &[(u8, FlowDir)] = &[
            (1, FlowDir::East),
            (2, FlowDir::Northeast),
            (3, FlowDir::North),
            (4, FlowDir::Northwest),
            (5, FlowDir::West),
            (6, FlowDir::Southwest),
            (7, FlowDir::South),
            (8, FlowDir::Southeast),
        ];
        let mut tile =
            FlowDirectionTile::new(GridDims::new(1, 8), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        for (col, &(byte, _)) in cases.iter().enumerate() {
            tile.set_raw(GridCoord::new(0, col), byte);
        }
        for (col, &(_, expected)) in cases.iter().enumerate() {
            assert_eq!(
                tile.get(GridCoord::new(0, col)),
                Some(expected),
                "TauDEM byte {} at col {col}",
                cases[col].0
            );
        }
    }

    #[test]
    fn taudem_get_checked_decodes_correctly() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(3, 3), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        // TauDEM code 7 = South
        tile.set_raw(GridCoord::new(1, 1), 7);
        assert_eq!(tile.get_checked(1, 1), Some(FlowDir::South));
        // OOB still returns None
        assert_eq!(tile.get_checked(-1, 0), None);
        assert_eq!(tile.get_checked(0, 10), None);
    }

    #[test]
    fn taudem_get_zero_returns_none() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 0);
        assert_eq!(tile.get(GridCoord::new(0, 0)), None);
    }

    #[test]
    fn taudem_invalid_byte_returns_none() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        // 9 is not valid in TauDEM (only 1–8)
        tile.set_raw(GridCoord::new(0, 0), 9);
        assert_eq!(tile.get(GridCoord::new(0, 0)), None);
    }

    #[test]
    fn encoding_accessor_esri() {
        let tile =
            FlowDirectionTile::new(GridDims::new(1, 1), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        assert_eq!(tile.encoding(), FlowDirEncoding::Esri);
    }

    #[test]
    fn encoding_accessor_taudem() {
        let tile =
            FlowDirectionTile::new(GridDims::new(1, 1), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        assert_eq!(tile.encoding(), FlowDirEncoding::Taudem);
    }

    #[test]
    fn esri_and_taudem_tiles_decode_independently() {
        // ESRI byte 2 = Southeast; TauDEM byte 2 = Northeast.
        // Both tiles contain raw byte 2 but must decode to different directions.
        let mut esri_tile =
            FlowDirectionTile::new(GridDims::new(1, 1), simple_geo(), FlowDirEncoding::Esri)
                .unwrap();
        esri_tile.set_raw(GridCoord::new(0, 0), 2);

        let mut taudem_tile =
            FlowDirectionTile::new(GridDims::new(1, 1), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        taudem_tile.set_raw(GridCoord::new(0, 0), 2);

        assert_eq!(
            esri_tile.get(GridCoord::new(0, 0)),
            Some(FlowDir::Southeast)
        );
        assert_eq!(
            taudem_tile.get(GridCoord::new(0, 0)),
            Some(FlowDir::Northeast)
        );
    }

    #[test]
    fn taudem_apply_mask_preserves_encoding() {
        let mut tile =
            FlowDirectionTile::new(GridDims::new(2, 2), simple_geo(), FlowDirEncoding::Taudem)
                .unwrap();
        tile.set_raw(GridCoord::new(0, 0), 3); // TauDEM North
        tile.set_raw(GridCoord::new(0, 1), 7); // TauDEM South
        tile.set_raw(GridCoord::new(1, 0), 1); // TauDEM East
        tile.set_raw(GridCoord::new(1, 1), 5); // TauDEM West
        let mask = CatchmentMask::new(vec![true, false, false, true], GridDims::new(2, 2));
        let masked = tile.apply_mask(&mask).unwrap();
        assert_eq!(masked.encoding(), FlowDirEncoding::Taudem);
        assert_eq!(masked.get(GridCoord::new(0, 0)), Some(FlowDir::North));
        assert_eq!(masked.get_raw(GridCoord::new(0, 1)), 255);
        assert_eq!(masked.get_raw(GridCoord::new(1, 0)), 255);
        assert_eq!(masked.get(GridCoord::new(1, 1)), Some(FlowDir::West));
    }
}
