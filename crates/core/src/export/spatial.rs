//! Spatial helpers for basin GeoParquet export rows.

use geo::{Area, BoundingRect, Centroid, MultiPolygon};

use crate::export::{BasinId, DelineationLabel, ExportError};

const HILBERT_BITS: u32 = 16;
const HILBERT_AXIS_SIZE: u32 = 1 << HILBERT_BITS;
const HILBERT_AXIS_MAX: u32 = HILBERT_AXIS_SIZE - 1;
const MIN_LON: f64 = -180.0;
const MIN_LAT: f64 = -90.0;
const MAX_LON: f64 = 180.0;
const MAX_LAT: f64 = 90.0;

/// True f64 bounds for a basin geometry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BasinBbox {
    /// Western bound.
    pub minx: f64,
    /// Southern bound.
    pub miny: f64,
    /// Eastern bound.
    pub maxx: f64,
    /// Northern bound.
    pub maxy: f64,
}

/// Outward-rounded f32 bounds for row-group pruning.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OutwardF32Bbox {
    /// Western bound rounded toward negative infinity.
    pub minx: f32,
    /// Southern bound rounded toward negative infinity.
    pub miny: f32,
    /// Eastern bound rounded toward positive infinity.
    pub maxx: f32,
    /// Northern bound rounded toward positive infinity.
    pub maxy: f32,
}

/// Centroid point used for Hilbert ordering.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BasinCentroid {
    /// Longitude in EPSG:4326.
    pub lon: f64,
    /// Latitude in EPSG:4326.
    pub lat: f64,
}

/// Shed-owned 16-bit-per-axis Hilbert sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HilbertIndex(pub u32);

impl HilbertIndex {
    /// Compute a Hilbert index from an EPSG:4326 centroid.
    pub fn from_centroid(centroid: BasinCentroid) -> Self {
        let x = quantize_axis(centroid.lon, MIN_LON, MAX_LON);
        let y = quantize_axis(centroid.lat, MIN_LAT, MAX_LAT);
        Self(xy_to_hilbert_index(x, y))
    }
}

/// Full deterministic spatial sort key for one export row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BasinSpatialSortKey {
    /// Primary Hilbert curve position.
    pub hilbert_index: HilbertIndex,
    /// Secondary basin identity tie-break.
    pub basin_id: BasinId,
    /// Tertiary delineation label tie-break.
    pub delineation: DelineationLabel,
}

impl BasinSpatialSortKey {
    /// Build a deterministic sort key from row identity and geometry.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | geometry has no finite centroid | [`ExportError::CentroidFailure`] |
    pub fn from_geometry(
        basin_id: BasinId,
        delineation: DelineationLabel,
        geometry: &MultiPolygon<f64>,
    ) -> Result<Self, ExportError> {
        let centroid = basin_centroid(geometry)?;
        Ok(Self {
            hilbert_index: HilbertIndex::from_centroid(centroid),
            basin_id,
            delineation,
        })
    }
}

/// Compute true f64 bounds for a basin geometry.
///
/// # Errors
///
/// | Condition | Error variant |
/// |---|---|
/// | geometry is empty, degenerate, or has non-finite bounds | [`ExportError::BboxFailure`] |
pub fn basin_bbox(geometry: &MultiPolygon<f64>) -> Result<BasinBbox, ExportError> {
    if geometry.0.is_empty() || geometry.unsigned_area() <= 0.0 {
        return Err(ExportError::BboxFailure {
            reason: "geometry is empty or degenerate",
        });
    }
    let rect = geometry.bounding_rect().ok_or(ExportError::BboxFailure {
        reason: "geometry has no bounding rectangle",
    })?;
    let bbox = BasinBbox {
        minx: rect.min().x,
        miny: rect.min().y,
        maxx: rect.max().x,
        maxy: rect.max().y,
    };
    if [bbox.minx, bbox.miny, bbox.maxx, bbox.maxy]
        .iter()
        .all(|v| v.is_finite())
        && bbox.minx < bbox.maxx
        && bbox.miny < bbox.maxy
    {
        Ok(bbox)
    } else {
        Err(ExportError::BboxFailure {
            reason: "geometry bounds are non-finite or degenerate",
        })
    }
}

/// Round true f64 bounds outward to f32 precision.
pub fn outward_f32_bbox(bbox: BasinBbox) -> OutwardF32Bbox {
    OutwardF32Bbox {
        minx: round_f32_toward_negative_infinity(bbox.minx),
        miny: round_f32_toward_negative_infinity(bbox.miny),
        maxx: round_f32_toward_positive_infinity(bbox.maxx),
        maxy: round_f32_toward_positive_infinity(bbox.maxy),
    }
}

/// Compute the centroid used for Hilbert ordering.
///
/// # Errors
///
/// | Condition | Error variant |
/// |---|---|
/// | geometry is empty, degenerate, or has no finite centroid | [`ExportError::CentroidFailure`] |
pub fn basin_centroid(geometry: &MultiPolygon<f64>) -> Result<BasinCentroid, ExportError> {
    if geometry.0.is_empty() || geometry.unsigned_area() <= 0.0 {
        return Err(ExportError::CentroidFailure {
            reason: "geometry is empty or degenerate",
        });
    }
    let point = geometry.centroid().ok_or(ExportError::CentroidFailure {
        reason: "geometry has no centroid",
    })?;
    let centroid = BasinCentroid {
        lon: point.x(),
        lat: point.y(),
    };
    if centroid.lon.is_finite() && centroid.lat.is_finite() {
        Ok(centroid)
    } else {
        Err(ExportError::CentroidFailure {
            reason: "geometry centroid is non-finite",
        })
    }
}

fn round_f32_toward_negative_infinity(value: f64) -> f32 {
    let rounded = value as f32;
    if f64::from(rounded) <= value {
        rounded
    } else {
        next_f32_down(rounded)
    }
}

fn round_f32_toward_positive_infinity(value: f64) -> f32 {
    let rounded = value as f32;
    if f64::from(rounded) >= value {
        rounded
    } else {
        next_f32_up(rounded)
    }
}

fn next_f32_down(value: f32) -> f32 {
    if value.is_nan() || value == f32::NEG_INFINITY {
        value
    } else if value == 0.0 {
        f32::from_bits(0x8000_0001)
    } else if value.is_sign_positive() {
        f32::from_bits(value.to_bits() - 1)
    } else {
        f32::from_bits(value.to_bits() + 1)
    }
}

fn next_f32_up(value: f32) -> f32 {
    if value.is_nan() || value == f32::INFINITY {
        value
    } else if value == 0.0 {
        f32::from_bits(1)
    } else if value.is_sign_positive() {
        f32::from_bits(value.to_bits() + 1)
    } else {
        f32::from_bits(value.to_bits() - 1)
    }
}

fn quantize_axis(value: f64, min: f64, max: f64) -> u32 {
    let clamped = if value.is_finite() {
        value.clamp(min, max)
    } else if value.is_sign_negative() {
        min
    } else {
        max
    };
    let normalized = (clamped - min) / (max - min);
    (normalized * f64::from(HILBERT_AXIS_MAX)).round() as u32
}

fn xy_to_hilbert_index(mut x: u32, mut y: u32) -> u32 {
    let mut index = 0u32;
    let mut bits = HILBERT_BITS;
    while bits > 0 {
        let half = 1 << (bits - 1);
        let quadrant_area = half * half;
        if x < half && y < half {
            std::mem::swap(&mut x, &mut y);
        } else if x < half {
            index += quadrant_area;
            y -= half;
        } else if y >= half {
            index += 2 * quadrant_area;
            x -= half;
            y -= half;
        } else {
            index += 3 * quadrant_area;
            let next_x = half - 1 - y;
            let next_y = half - 1 - (x - half);
            x = next_x;
            y = next_y;
        }
        bits -= 1;
    }
    index
}

#[cfg(test)]
mod export_spatial_tests {
    use geo::{LineString, MultiPolygon, Polygon};

    use super::*;

    fn rect(minx: f64, miny: f64, maxx: f64, maxy: f64) -> MultiPolygon<f64> {
        MultiPolygon::new(vec![rect_polygon(minx, miny, maxx, maxy)])
    }

    fn rect_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (minx, miny),
                (maxx, miny),
                (maxx, maxy),
                (minx, maxy),
                (minx, miny),
            ]),
            vec![],
        )
    }

    fn tiny_rect_around(lon: f64, lat: f64) -> MultiPolygon<f64> {
        rect(lon - 1.0e-8, lat - 1.0e-8, lon + 1.0e-8, lat + 1.0e-8)
    }

    fn lon_for_quantized_x(x: u32) -> f64 {
        MIN_LON + f64::from(x) * (MAX_LON - MIN_LON) / f64::from(HILBERT_AXIS_MAX)
    }

    fn lat_for_quantized_y(y: u32) -> f64 {
        MIN_LAT + f64::from(y) * (MAX_LAT - MIN_LAT) / f64::from(HILBERT_AXIS_MAX)
    }

    #[test]
    fn export_spatial_bbox_correctness() {
        let geometry = MultiPolygon::new(vec![
            rect_polygon(-2.0, 1.0, -1.0, 3.0),
            rect_polygon(4.0, -5.0, 6.0, -4.0),
        ]);

        assert_eq!(
            basin_bbox(&geometry).unwrap(),
            BasinBbox {
                minx: -2.0,
                miny: -5.0,
                maxx: 6.0,
                maxy: 3.0
            }
        );
    }

    #[test]
    fn export_spatial_outward_rounding_contains_non_representable_bbox() {
        let bbox = BasinBbox {
            minx: 0.1,
            miny: -0.1,
            maxx: 1.1,
            maxy: -1.0 / 3.0,
        };
        let rounded = outward_f32_bbox(bbox);

        assert!(f64::from(rounded.minx) <= bbox.minx);
        assert!(f64::from(rounded.miny) <= bbox.miny);
        assert!(f64::from(rounded.maxx) >= bbox.maxx);
        assert!(f64::from(rounded.maxy) >= bbox.maxy);
    }

    #[test]
    fn export_spatial_hand_computed_hilbert_indices_for_known_centroids() {
        let cases = [
            // Quantized (x, y) = (0, 0). Every xy2d scale has rx=0, ry=0,
            // so every contribution is 0 and the final index is 0.
            (0, 0, HilbertIndex(0)),
            // Quantized (x, y) = (0, 1). All high-bit scales contribute 0.
            // The lower-left xy2d rotation swaps x/y at each of the 15 larger
            // scales, so the final 2x2 coordinate is (1, 0), whose index is 3.
            (0, 1, HilbertIndex(3)),
            // Quantized (x, y) = (1, 1). The larger-scale lower-left rotations
            // leave equal coordinates unchanged; final 2x2 index is 2.
            (1, 1, HilbertIndex(2)),
            // Quantized (x, y) = (1, 0). All high-bit scales contribute 0.
            // The 15 larger-scale lower-left rotations make the final 2x2
            // coordinate (0, 1), whose index is 1.
            (1, 0, HilbertIndex(1)),
        ];

        for (x, y, expected) in cases {
            let geometry = tiny_rect_around(lon_for_quantized_x(x), lat_for_quantized_y(y));
            let centroid = basin_centroid(&geometry).unwrap();
            assert_eq!(HilbertIndex::from_centroid(centroid), expected);
        }
    }

    #[test]
    fn export_spatial_deterministic_hilbert_order() {
        let west = BasinSpatialSortKey::from_geometry(
            BasinId::parse("west").unwrap(),
            DelineationLabel::parse("fabric/v1/d8").unwrap(),
            &tiny_rect_around(lon_for_quantized_x(0), lat_for_quantized_y(0)),
        )
        .unwrap();
        let east = BasinSpatialSortKey::from_geometry(
            BasinId::parse("east").unwrap(),
            DelineationLabel::parse("fabric/v1/d8").unwrap(),
            &tiny_rect_around(lon_for_quantized_x(1), lat_for_quantized_y(0)),
        )
        .unwrap();

        let mut keys = vec![east.clone(), west.clone()];
        keys.sort();
        assert_eq!(keys, vec![west, east]);
    }

    #[test]
    fn export_spatial_tie_break_stability() {
        let geometry = tiny_rect_around(lon_for_quantized_x(0), lat_for_quantized_y(0));
        let mut keys = vec![
            BasinSpatialSortKey::from_geometry(
                BasinId::parse("b").unwrap(),
                DelineationLabel::parse("fabric/v1/b").unwrap(),
                &geometry,
            )
            .unwrap(),
            BasinSpatialSortKey::from_geometry(
                BasinId::parse("a").unwrap(),
                DelineationLabel::parse("fabric/v1/z").unwrap(),
                &geometry,
            )
            .unwrap(),
            BasinSpatialSortKey::from_geometry(
                BasinId::parse("a").unwrap(),
                DelineationLabel::parse("fabric/v1/a").unwrap(),
                &geometry,
            )
            .unwrap(),
        ];

        keys.sort();
        let labels = keys
            .iter()
            .map(|key| (key.basin_id.as_str(), key.delineation.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec![
                ("a", "fabric/v1/a"),
                ("a", "fabric/v1/z"),
                ("b", "fabric/v1/b")
            ]
        );
    }

    #[test]
    fn export_spatial_fixed_global_extent_does_not_renormalize_when_rows_are_added() {
        let geometry = tiny_rect_around(lon_for_quantized_x(1), lat_for_quantized_y(1));
        let original = HilbertIndex::from_centroid(basin_centroid(&geometry).unwrap());
        let unrelated = tiny_rect_around(179.0, 89.0);
        let _unrelated_index = HilbertIndex::from_centroid(basin_centroid(&unrelated).unwrap());
        let after_unrelated_row = HilbertIndex::from_centroid(basin_centroid(&geometry).unwrap());

        assert_eq!(original, HilbertIndex(2));
        assert_eq!(original, after_unrelated_row);
    }

    #[test]
    fn export_spatial_errors_for_empty_or_centroid_less_geometry() {
        let empty = MultiPolygon::new(vec![]);
        assert!(matches!(
            basin_bbox(&empty),
            Err(ExportError::BboxFailure { .. })
        ));
        assert!(matches!(
            basin_centroid(&empty),
            Err(ExportError::CentroidFailure { .. })
        ));

        let degenerate = MultiPolygon::new(vec![Polygon::new(
            LineString::from(vec![(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (0.0, 0.0)]),
            vec![],
        )]);
        assert!(matches!(
            basin_centroid(&degenerate),
            Err(ExportError::CentroidFailure { .. })
        ));
    }
}
