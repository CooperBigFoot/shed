//! WKB geometry decoding for HFX catchment geometries.
//!
//! Converts opaque [`WkbGeometry`] bytes into `geo` types using `geozero`.
//! This is pure Rust with no GDAL dependency.

use geo::{Geometry, LineString, MultiPolygon, Polygon};
use geozero::ToGeo;
use geozero::wkb::Wkb;
use geozero::{GeomProcessor, GeozeroGeometry};
use hfx_core::WkbGeometry;

/// Errors from WKB decoding.
#[derive(Debug, thiserror::Error)]
pub enum WkbDecodeError {
    /// geozero failed to decode WKB bytes.
    #[error("WKB decoding failed: {reason}")]
    DecodeFailed {
        /// Reason reported by the decoder.
        reason: String,
    },

    /// Decoded geometry was not the expected type.
    #[error("expected {expected}, got {actual}")]
    UnexpectedType {
        /// The geometry type that was expected.
        expected: &'static str,
        /// The geometry type that was actually decoded.
        actual: String,
    },
}

/// Decode a [`WkbGeometry`] into a generic [`Geometry`].
///
/// # Errors
///
/// | Variant | When |
/// |---|---|
/// | [`WkbDecodeError::DecodeFailed`] | The geozero WKB decoder fails |
pub fn decode_wkb(wkb: &WkbGeometry) -> Result<Geometry<f64>, WkbDecodeError> {
    Wkb(wkb.as_bytes())
        .to_geo()
        .map_err(|e| WkbDecodeError::DecodeFailed {
            reason: e.to_string(),
        })
}

/// Decode a [`WkbGeometry`] that encodes a `Polygon` or `MultiPolygon`.
///
/// A `Polygon` is promoted to a single-element `MultiPolygon`.
///
/// # Errors
///
/// | Variant | When |
/// |---|---|
/// | [`WkbDecodeError::DecodeFailed`] | The geozero WKB decoder fails |
/// | [`WkbDecodeError::UnexpectedType`] | Decoded geometry is not a polygon type |
pub fn decode_wkb_multi_polygon(wkb: &WkbGeometry) -> Result<MultiPolygon<f64>, WkbDecodeError> {
    let geom = decode_wkb(wkb)?;
    match geom {
        Geometry::Polygon(p) => Ok(MultiPolygon::new(vec![p])),
        Geometry::MultiPolygon(mp) => Ok(mp),
        other => Err(WkbDecodeError::UnexpectedType {
            expected: "Polygon or MultiPolygon",
            actual: geometry_type_name(&other).to_owned(),
        }),
    }
}

/// Decode a [`WkbGeometry`] that encodes a single `Polygon`.
///
/// # Errors
///
/// | Variant | When |
/// |---|---|
/// | [`WkbDecodeError::DecodeFailed`] | The geozero WKB decoder fails |
/// | [`WkbDecodeError::UnexpectedType`] | Decoded geometry is not a `Polygon` |
pub fn decode_wkb_polygon(wkb: &WkbGeometry) -> Result<Polygon<f64>, WkbDecodeError> {
    let geom = decode_wkb(wkb)?;
    match geom {
        Geometry::Polygon(p) => Ok(p),
        other => Err(WkbDecodeError::UnexpectedType {
            expected: "Polygon",
            actual: geometry_type_name(&other).to_owned(),
        }),
    }
}

/// Errors from WKB encoding.
#[derive(Debug, thiserror::Error)]
pub enum WkbEncodeError {
    /// geozero failed to encode the geometry to WKB.
    #[error("WKB encoding failed: {reason}")]
    EncodeFailed {
        /// Reason reported by the encoder.
        reason: String,
    },
}

/// Encode a [`MultiPolygon`] to OGC WKB bytes (little-endian, 2D).
pub fn encode_wkb_multi_polygon(mp: &MultiPolygon<f64>) -> Result<Vec<u8>, WkbEncodeError> {
    use geozero::{CoordDimensions, ToWkb};
    MultiPolygonRef(mp)
        .to_wkb(CoordDimensions::xy())
        .map_err(|e| WkbEncodeError::EncodeFailed {
            reason: e.to_string(),
        })
}

struct MultiPolygonRef<'a>(&'a MultiPolygon<f64>);

impl GeozeroGeometry for MultiPolygonRef<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()> {
        processor.multipolygon_begin(self.0.0.len(), 0)?;
        for (idx, polygon) in self.0.0.iter().enumerate() {
            process_polygon_ref(polygon, false, idx, processor)?;
        }
        processor.multipolygon_end(0)
    }
}

fn process_polygon_ref<P: GeomProcessor>(
    polygon: &Polygon<f64>,
    tagged: bool,
    idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.polygon_begin(tagged, polygon.interiors().len() + 1, idx)?;
    process_line_string_ref(polygon.exterior(), false, 0, processor)?;
    for (ring_idx, ring) in polygon.interiors().iter().enumerate() {
        process_line_string_ref(ring, false, ring_idx + 1, processor)?;
    }
    processor.polygon_end(tagged, idx)
}

fn process_line_string_ref<P: GeomProcessor>(
    line: &LineString<f64>,
    tagged: bool,
    idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    let multi = processor.multi_dim();
    processor.linestring_begin(tagged, line.0.len(), idx)?;
    for (coord_idx, coord) in line.0.iter().enumerate() {
        if multi {
            processor.coordinate(coord.x, coord.y, None, None, None, None, coord_idx)?;
        } else {
            processor.xy(coord.x, coord.y, coord_idx)?;
        }
    }
    processor.linestring_end(tagged, idx)
}

/// Return a static name string for a [`Geometry`] variant.
fn geometry_type_name(geom: &Geometry<f64>) -> &'static str {
    match geom {
        Geometry::Point(_) => "Point",
        Geometry::Line(_) => "Line",
        Geometry::LineString(_) => "LineString",
        Geometry::Polygon(_) => "Polygon",
        Geometry::MultiPoint(_) => "MultiPoint",
        Geometry::MultiLineString(_) => "MultiLineString",
        Geometry::MultiPolygon(_) => "MultiPolygon",
        Geometry::GeometryCollection(_) => "GeometryCollection",
        Geometry::Rect(_) => "Rect",
        Geometry::Triangle(_) => "Triangle",
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{LineString, Polygon};

    // WKB for a simple unit square polygon (WKB Little-Endian):
    // Polygon with one ring: (0,0), (1,0), (1,1), (0,1), (0,0)
    //
    // Layout:
    //   01            byte order (little-endian)
    //   03000000      wkbPolygon = 3
    //   01000000      ring count = 1
    //   05000000      point count = 5
    //   [5 × 2 × f64 coordinates]
    fn unit_square_polygon_wkb() -> Vec<u8> {
        let mut bytes = Vec::new();
        // byte order
        bytes.push(0x01);
        // type: wkbPolygon = 3
        bytes.extend_from_slice(&3u32.to_le_bytes());
        // ring count = 1
        bytes.extend_from_slice(&1u32.to_le_bytes());
        // point count = 5
        bytes.extend_from_slice(&5u32.to_le_bytes());
        // coords: (0,0), (1,0), (1,1), (0,1), (0,0)
        let coords: &[(f64, f64)] = &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        for (x, y) in coords {
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
        }
        bytes
    }

    // WKB for a MultiPolygon with one unit square.
    //
    // Layout:
    //   01            byte order (little-endian)
    //   06000000      wkbMultiPolygon = 6
    //   01000000      geometry count = 1
    //   [embedded Polygon WKB]
    fn unit_square_multi_polygon_wkb() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(0x01);
        // type: wkbMultiPolygon = 6
        bytes.extend_from_slice(&6u32.to_le_bytes());
        // geometry count = 1
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend(unit_square_polygon_wkb());
        bytes
    }

    // WKB for a LineString with two points — used to test unexpected-type errors.
    fn linestring_wkb() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(0x01);
        // type: wkbLineString = 2
        bytes.extend_from_slice(&2u32.to_le_bytes());
        // point count = 2
        bytes.extend_from_slice(&2u32.to_le_bytes());
        for (x, y) in &[(0.0f64, 0.0f64), (1.0, 1.0)] {
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
        }
        bytes
    }

    fn make_wkb(raw: Vec<u8>) -> WkbGeometry {
        WkbGeometry::new(raw).expect("test WKB must not be empty")
    }

    // ── decode_wkb ────────────────────────────────────────────────────────────

    #[test]
    fn decode_wkb_polygon_variant() {
        let wkb = make_wkb(unit_square_polygon_wkb());
        let geom = decode_wkb(&wkb).expect("polygon WKB should decode");
        assert!(matches!(geom, Geometry::Polygon(_)));
    }

    #[test]
    fn decode_wkb_multi_polygon_variant() {
        let wkb = make_wkb(unit_square_multi_polygon_wkb());
        let geom = decode_wkb(&wkb).expect("multipolygon WKB should decode");
        assert!(matches!(geom, Geometry::MultiPolygon(_)));
    }

    #[test]
    fn decode_wkb_invalid_bytes_returns_error() {
        let wkb = make_wkb(vec![0xFF, 0xFF, 0xFF]);
        assert!(matches!(
            decode_wkb(&wkb),
            Err(WkbDecodeError::DecodeFailed { .. })
        ));
    }

    // ── decode_wkb_multi_polygon ─────────────────────────────────────────────

    #[test]
    fn decode_wkb_multi_polygon_from_polygon() {
        // A Polygon WKB should be promoted to a single-element MultiPolygon.
        let wkb = make_wkb(unit_square_polygon_wkb());
        let mp = decode_wkb_multi_polygon(&wkb).expect("polygon should promote to MultiPolygon");
        assert_eq!(mp.0.len(), 1);
    }

    #[test]
    fn decode_wkb_multi_polygon_from_multi_polygon() {
        let wkb = make_wkb(unit_square_multi_polygon_wkb());
        let mp = decode_wkb_multi_polygon(&wkb).expect("MultiPolygon WKB should decode directly");
        assert_eq!(mp.0.len(), 1);
    }

    #[test]
    fn decode_wkb_multi_polygon_wrong_type_returns_error() {
        let wkb = make_wkb(linestring_wkb());
        let err = decode_wkb_multi_polygon(&wkb).expect_err("LineString should fail");
        assert!(
            matches!(
                err,
                WkbDecodeError::UnexpectedType {
                    expected: "Polygon or MultiPolygon",
                    ..
                }
            ),
            "unexpected error variant: {err:?}"
        );
    }

    // ── decode_wkb_polygon ───────────────────────────────────────────────────

    #[test]
    fn decode_wkb_polygon_succeeds() {
        let wkb = make_wkb(unit_square_polygon_wkb());
        let poly = decode_wkb_polygon(&wkb).expect("polygon WKB should decode");
        // The unit square has 5 coords (ring closed).
        assert_eq!(poly.exterior().coords().count(), 5);
    }

    #[test]
    fn decode_wkb_polygon_wrong_type_returns_error() {
        let wkb = make_wkb(linestring_wkb());
        let err = decode_wkb_polygon(&wkb).expect_err("LineString should fail");
        assert!(
            matches!(
                err,
                WkbDecodeError::UnexpectedType {
                    expected: "Polygon",
                    ..
                }
            ),
            "unexpected error variant: {err:?}"
        );
    }

    #[test]
    fn decode_wkb_polygon_from_multi_polygon_returns_error() {
        // MultiPolygon is not a Polygon.
        let wkb = make_wkb(unit_square_multi_polygon_wkb());
        let err = decode_wkb_polygon(&wkb).expect_err("MultiPolygon should not decode as Polygon");
        assert!(
            matches!(
                err,
                WkbDecodeError::UnexpectedType {
                    expected: "Polygon",
                    ..
                }
            ),
            "unexpected error variant: {err:?}"
        );
    }

    // ── encode_wkb_multi_polygon ─────────────────────────────────────────────

    #[test]
    fn encode_decode_round_trip() {
        let polygon = Polygon::new(
            LineString::from(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            vec![],
        );
        let mp = MultiPolygon::new(vec![polygon]);
        let wkb_bytes = encode_wkb_multi_polygon(&mp).unwrap();
        assert!(!wkb_bytes.is_empty());
        // First byte is endianness: 0x01 = LE
        assert_eq!(wkb_bytes[0], 0x01);

        // Round-trip: decode back
        let wkb = make_wkb(wkb_bytes);
        let decoded = decode_wkb_multi_polygon(&wkb).unwrap();
        assert_eq!(decoded.0.len(), 1);
        let exterior = decoded.0[0].exterior();
        assert_eq!(exterior.0.len(), 5); // closed ring
    }

    #[test]
    fn encode_empty_multi_polygon() {
        let mp = MultiPolygon::new(vec![]);
        let result = encode_wkb_multi_polygon(&mp);
        assert!(result.is_ok());
    }
}
