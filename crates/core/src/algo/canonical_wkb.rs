//! Canonical WKB normalization for parity golden geometries.

use std::cmp::Ordering;

use geo::{Coord, LineString, MultiPolygon, Polygon};

use crate::algo::{WkbEncodeError, encode_wkb_multi_polygon};

/// Canonicalizer contract version recorded in parity goldens.
pub const CANONICAL_WKB_VERSION: &str = "shed-canonical-wkb-v1";

/// Number of decimal coordinate places retained before canonical WKB emission.
pub const CANONICAL_WKB_DECIMAL_PRECISION: u32 = 6;

const COORDINATE_SCALE: f64 = 1_000_000.0;

/// Normalize a [`MultiPolygon`] and encode it as canonical little-endian 2D WKB.
///
/// The canonical form rounds coordinates to [`CANONICAL_WKB_DECIMAL_PRECISION`],
/// explicitly closes rings, orients exteriors CCW and holes CW, rotates every
/// ring to its total-order start vertex, sorts holes, sorts polygon components,
/// and delegates final WKB emission to the shared WKB encoder.
///
/// # Errors
///
/// | Variant | When |
/// |---|---|
/// | [`WkbEncodeError::EncodeFailed`] | The shared WKB encoder fails |
pub fn canonical_wkb_multi_polygon(mp: &MultiPolygon<f64>) -> Result<Vec<u8>, WkbEncodeError> {
    let mut polygons: Vec<CanonicalPolygon> = mp.0.iter().map(normalize_polygon).collect();
    polygons.sort_by(compare_polygon);

    let normalized = MultiPolygon::new(
        polygons
            .into_iter()
            .map(CanonicalPolygon::into_polygon)
            .collect(),
    );
    encode_wkb_multi_polygon(&normalized)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RoundedCoord {
    x: i64,
    y: i64,
}

impl RoundedCoord {
    fn from_coord(coord: &Coord<f64>) -> Self {
        Self {
            x: round_coordinate(coord.x),
            y: round_coordinate(coord.y),
        }
    }

    fn into_coord(self) -> Coord<f64> {
        Coord {
            x: self.x as f64 / COORDINATE_SCALE,
            y: self.y as f64 / COORDINATE_SCALE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalRing {
    vertices: Vec<RoundedCoord>,
}

impl CanonicalRing {
    fn bbox(&self) -> RingBbox {
        self.open_vertices()
            .iter()
            .fold(RingBbox::empty(), |bbox, coord| bbox.include(*coord))
    }

    fn signed_area_twice(&self) -> i128 {
        signed_area_twice(self.open_vertices())
    }

    fn open_vertices(&self) -> &[RoundedCoord] {
        self.vertices
            .split_last()
            .map_or(self.vertices.as_slice(), |(_, open)| open)
    }

    fn into_line_string(self) -> LineString<f64> {
        LineString::new(
            self.vertices
                .into_iter()
                .map(RoundedCoord::into_coord)
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RingBbox {
    min_x: i64,
    min_y: i64,
    max_x: i64,
    max_y: i64,
}

impl RingBbox {
    fn empty() -> Self {
        Self {
            min_x: i64::MAX,
            min_y: i64::MAX,
            max_x: i64::MIN,
            max_y: i64::MIN,
        }
    }

    fn include(self, coord: RoundedCoord) -> Self {
        Self {
            min_x: self.min_x.min(coord.x),
            min_y: self.min_y.min(coord.y),
            max_x: self.max_x.max(coord.x),
            max_y: self.max_y.max(coord.y),
        }
    }
}

#[derive(Debug, Clone)]
struct CanonicalPolygon {
    exterior: CanonicalRing,
    holes: Vec<CanonicalRing>,
}

impl CanonicalPolygon {
    fn area_twice(&self) -> i128 {
        self.exterior.signed_area_twice().abs()
            - self
                .holes
                .iter()
                .map(|hole| hole.signed_area_twice().abs())
                .sum::<i128>()
    }

    fn into_polygon(self) -> Polygon<f64> {
        Polygon::new(
            self.exterior.into_line_string(),
            self.holes
                .into_iter()
                .map(CanonicalRing::into_line_string)
                .collect(),
        )
    }
}

fn normalize_polygon(polygon: &Polygon<f64>) -> CanonicalPolygon {
    let exterior = normalize_ring(polygon.exterior(), RingOrientation::Exterior);
    let mut holes: Vec<CanonicalRing> = polygon
        .interiors()
        .iter()
        .map(|ring| normalize_ring(ring, RingOrientation::Hole))
        .collect();
    holes.sort_by(compare_ring);
    CanonicalPolygon { exterior, holes }
}

#[derive(Debug, Clone, Copy)]
enum RingOrientation {
    Exterior,
    Hole,
}

fn normalize_ring(ring: &LineString<f64>, orientation: RingOrientation) -> CanonicalRing {
    let mut vertices: Vec<RoundedCoord> = ring.coords().map(RoundedCoord::from_coord).collect();
    if vertices.first() == vertices.last() {
        vertices.pop();
    }

    let area_twice = signed_area_twice(&vertices);
    match orientation {
        RingOrientation::Exterior if area_twice < 0 => vertices.reverse(),
        RingOrientation::Hole if area_twice > 0 => vertices.reverse(),
        RingOrientation::Exterior | RingOrientation::Hole => {}
    }

    vertices = rotate_to_total_order_start(vertices);
    if let Some(first) = vertices.first().copied() {
        vertices.push(first);
    }

    CanonicalRing { vertices }
}

fn rotate_to_total_order_start(vertices: Vec<RoundedCoord>) -> Vec<RoundedCoord> {
    if vertices.len() <= 1 {
        return vertices;
    }

    let start = (1..vertices.len()).fold(0, |best, candidate| {
        if compare_cyclic_sequence(&vertices, candidate, best) == Ordering::Less {
            candidate
        } else {
            best
        }
    });

    vertices[start..]
        .iter()
        .chain(vertices[..start].iter())
        .copied()
        .collect()
}

fn compare_cyclic_sequence(
    vertices: &[RoundedCoord],
    left_start: usize,
    right_start: usize,
) -> Ordering {
    (0..vertices.len())
        .map(|offset| {
            let left = vertices[(left_start + offset) % vertices.len()];
            let right = vertices[(right_start + offset) % vertices.len()];
            left.cmp(&right)
        })
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

fn compare_ring(left: &CanonicalRing, right: &CanonicalRing) -> Ordering {
    left.bbox()
        .cmp(&right.bbox())
        .then_with(|| left.signed_area_twice().cmp(&right.signed_area_twice()))
        .then_with(|| left.open_vertices().cmp(right.open_vertices()))
}

fn compare_polygon(left: &CanonicalPolygon, right: &CanonicalPolygon) -> Ordering {
    left.exterior
        .bbox()
        .cmp(&right.exterior.bbox())
        .then_with(|| left.area_twice().cmp(&right.area_twice()))
        .then_with(|| left.holes.len().cmp(&right.holes.len()))
        .then_with(|| {
            left.exterior
                .open_vertices()
                .cmp(right.exterior.open_vertices())
        })
        .then_with(|| compare_hole_sequences(&left.holes, &right.holes))
}

fn compare_hole_sequences(left: &[CanonicalRing], right: &[CanonicalRing]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left_ring, right_ring)| compare_ring(left_ring, right_ring))
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn signed_area_twice(vertices: &[RoundedCoord]) -> i128 {
    if vertices.len() < 3 {
        return 0;
    }

    vertices
        .iter()
        .zip(vertices.iter().cycle().skip(1))
        .take(vertices.len())
        .map(|(left, right)| {
            (left.x as i128 * right.y as i128) - (right.x as i128 * left.y as i128)
        })
        .sum()
}

fn round_coordinate(value: f64) -> i64 {
    let rounded = (value * COORDINATE_SCALE).round();
    if rounded == 0.0 { 0 } else { rounded as i64 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Area, line_string, polygon};
    use geozero::ToGeo;
    use geozero::wkb::Wkb;

    fn canonicalize(mp: &MultiPolygon<f64>) -> Vec<u8> {
        canonical_wkb_multi_polygon(mp).expect("canonical WKB should encode")
    }

    fn decode_multi_polygon(bytes: &[u8]) -> MultiPolygon<f64> {
        match Wkb(bytes).to_geo().expect("canonical WKB should decode") {
            geo::Geometry::MultiPolygon(mp) => mp,
            other => panic!("expected MultiPolygon, got {other:?}"),
        }
    }

    fn assert_idempotent(mp: &MultiPolygon<f64>) {
        let first = canonicalize(mp);
        let decoded = decode_multi_polygon(&first);
        let second = canonicalize(&decoded);
        assert_eq!(first, second);
        assert_eq!(first[0], 0x01);
    }

    #[test]
    fn records_precision_contract() {
        assert_eq!(CANONICAL_WKB_VERSION, "shed-canonical-wkb-v1");
        assert_eq!(CANONICAL_WKB_DECIMAL_PRECISION, 6);
    }

    #[test]
    fn canonicalizes_ring_closure_orientation_and_start_vertex() {
        let mp = MultiPolygon::new(vec![polygon![
            (x: 1.0, y: 0.0),
            (x: 0.0, y: 0.0),
            (x: 0.0, y: 1.0),
            (x: 1.0, y: 1.0)
        ]]);

        let decoded = decode_multi_polygon(&canonicalize(&mp));
        let exterior = decoded.0[0].exterior();
        let coords = exterior.coords().collect::<Vec<_>>();

        assert_eq!(coords.len(), 5);
        assert_eq!((coords[0].x, coords[0].y), (0.0, 0.0));
        assert_eq!((coords[1].x, coords[1].y), (1.0, 0.0));
        assert_eq!((coords[4].x, coords[4].y), (0.0, 0.0));
        assert!(decoded.0[0].signed_area() > 0.0);
    }

    #[test]
    fn canonicalizes_hole_orientation_and_ordering() {
        let first_by_input = line_string![
            (x: 3.0, y: 3.0),
            (x: 3.0, y: 2.0),
            (x: 2.0, y: 2.0),
            (x: 2.0, y: 3.0),
            (x: 3.0, y: 3.0)
        ];
        let second_by_input = line_string![
            (x: 1.0, y: 1.0),
            (x: 2.0, y: 1.0),
            (x: 2.0, y: 2.0),
            (x: 1.0, y: 2.0),
            (x: 1.0, y: 1.0)
        ];
        let mp = MultiPolygon::new(vec![Polygon::new(
            line_string![
                (x: 0.0, y: 0.0),
                (x: 4.0, y: 0.0),
                (x: 4.0, y: 4.0),
                (x: 0.0, y: 4.0),
                (x: 0.0, y: 0.0)
            ],
            vec![first_by_input, second_by_input],
        )]);

        let decoded = decode_multi_polygon(&canonicalize(&mp));
        let holes = decoded.0[0].interiors();

        assert_eq!(holes.len(), 2);
        assert_eq!(
            (
                holes[0].coords().next().unwrap().x,
                holes[0].coords().next().unwrap().y
            ),
            (1.0, 1.0)
        );
        assert!(Polygon::new(holes[0].clone(), vec![]).signed_area() < 0.0);
        assert!(Polygon::new(holes[1].clone(), vec![]).signed_area() < 0.0);
    }

    #[test]
    fn canonicalizes_component_ordering_with_full_sequence_tie_break() {
        let left = polygon![
            (x: 0.0, y: 0.0),
            (x: 2.0, y: 0.0),
            (x: 1.0, y: 1.0),
            (x: 0.0, y: 2.0),
            (x: 0.0, y: 0.0)
        ];
        let right = polygon![
            (x: 0.0, y: 0.0),
            (x: 2.0, y: 0.0),
            (x: 2.0, y: 1.0),
            (x: 0.0, y: 2.0),
            (x: 0.0, y: 0.0)
        ];
        let mp = MultiPolygon::new(vec![right, left]);

        let decoded = decode_multi_polygon(&canonicalize(&mp));
        let first = decoded.0[0].exterior().coords().collect::<Vec<_>>();

        assert_eq!((first[2].x, first[2].y), (1.0, 1.0));
    }

    #[test]
    fn idempotence_survives_duplicate_vertices_after_rounding() {
        let mp = MultiPolygon::new(vec![polygon![
            (x: 0.0, y: 0.0),
            (x: 0.0000004, y: 0.0000004),
            (x: 1.0, y: 0.0),
            (x: 1.0, y: 1.0),
            (x: 0.0, y: 1.0),
            (x: 0.0, y: 0.0)
        ]]);

        let decoded = decode_multi_polygon(&canonicalize(&mp));
        let coords = decoded.0[0].exterior().coords().collect::<Vec<_>>();

        assert_eq!((coords[0].x, coords[0].y), (0.0, 0.0));
        assert_eq!((coords[1].x, coords[1].y), (0.0, 0.0));
        assert_idempotent(&mp);
    }

    #[test]
    fn canonical_wkb_is_idempotent_for_nested_geometry() {
        let mp = MultiPolygon::new(vec![Polygon::new(
            line_string![
                (x: 2.0, y: 0.0),
                (x: 0.0, y: 0.0),
                (x: 0.0, y: 2.0),
                (x: 2.0, y: 2.0),
                (x: 2.0, y: 0.0)
            ],
            vec![line_string![
                (x: 0.5, y: 0.5),
                (x: 0.5, y: 1.5),
                (x: 1.5, y: 1.5),
                (x: 1.5, y: 0.5),
                (x: 0.5, y: 0.5)
            ]],
        )]);

        assert_idempotent(&mp);
    }
}
