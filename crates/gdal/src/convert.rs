//! Conversion helpers between `geo` types and GDAL geometry objects.

use gdal::vector::{Geometry, OGRwkbGeometryType};
use geo::{Coord, LineString, MultiPolygon, Polygon};

use crate::error::GdalRepairError;

/// Convert a [`MultiPolygon`] to a GDAL `wkbMultiPolygon` geometry.
pub(crate) fn multi_polygon_to_gdal(
    mp: &MultiPolygon<f64>,
) -> Result<Geometry, gdal::errors::GdalError> {
    let mut multi = Geometry::empty(OGRwkbGeometryType::wkbMultiPolygon)?;
    for polygon in &mp.0 {
        let gdal_poly = polygon_to_gdal(polygon)?;
        multi.add_geometry(gdal_poly)?;
    }
    Ok(multi)
}

/// Convert a [`Polygon`] to a GDAL `wkbPolygon` geometry.
fn polygon_to_gdal(polygon: &Polygon<f64>) -> Result<Geometry, gdal::errors::GdalError> {
    let mut geom = Geometry::empty(OGRwkbGeometryType::wkbPolygon)?;

    let mut ring = Geometry::empty(OGRwkbGeometryType::wkbLinearRing)?;
    for coord in polygon.exterior().coords() {
        ring.add_point_2d((coord.x, coord.y));
    }
    geom.add_geometry(ring)?;

    for interior in polygon.interiors() {
        let mut hole = Geometry::empty(OGRwkbGeometryType::wkbLinearRing)?;
        for coord in interior.coords() {
            hole.add_point_2d((coord.x, coord.y));
        }
        geom.add_geometry(hole)?;
    }

    Ok(geom)
}

/// Convert a GDAL geometry back to a [`MultiPolygon`].
///
/// Handles `POLYGON`, `MULTIPOLYGON`, and `GEOMETRYCOLLECTION` output,
/// extracting only polygon members from collections.
pub(crate) fn gdal_to_multi_polygon(geom: &Geometry) -> Result<MultiPolygon<f64>, GdalRepairError> {
    let name = geom.geometry_name();
    match name.as_str() {
        "POLYGON" => {
            let poly = gdal_polygon_to_geo(geom);
            Ok(MultiPolygon(vec![poly]))
        }
        "MULTIPOLYGON" => {
            let count = geom.geometry_count();
            let polys = (0..count)
                .map(|i| gdal_polygon_to_geo(&geom.get_geometry(i)))
                .collect();
            Ok(MultiPolygon(polys))
        }
        "GEOMETRYCOLLECTION" => {
            let polys = extract_polygons_from_collection(geom);
            Ok(MultiPolygon(polys))
        }
        other => Err(GdalRepairError::UnexpectedGeometryType {
            geometry_type: other.to_owned(),
        }),
    }
}

/// Convert a GDAL polygon geometry to a [`Polygon`].
///
/// The first sub-geometry is the exterior ring; subsequent sub-geometries are
/// interior rings (holes).
fn gdal_polygon_to_geo(geom: &Geometry) -> Polygon<f64> {
    let ring_count = geom.geometry_count();
    if ring_count == 0 {
        return Polygon::new(LineString(vec![]), vec![]);
    }

    let exterior = gdal_ring_to_coords(&geom.get_geometry(0));
    let holes = (1..ring_count)
        .map(|i| gdal_ring_to_coords(&geom.get_geometry(i)))
        .map(LineString)
        .collect();

    Polygon::new(LineString(exterior), holes)
}

/// Extract coordinate pairs from a GDAL `LinearRing` or `LineString`.
fn gdal_ring_to_coords(geom: &Geometry) -> Vec<Coord<f64>> {
    let n = geom.point_count();
    (0..n)
        .map(|i| {
            let (x, y, _z) = geom.get_point(i as i32);
            Coord { x, y }
        })
        .collect()
}

/// Recursively extract all [`Polygon`]s from a `GEOMETRYCOLLECTION`.
///
/// Used when `make_valid` returns a mixed collection — only polygon members
/// are kept, and nested collections are traversed.
fn extract_polygons_from_collection(geom: &Geometry) -> Vec<Polygon<f64>> {
    let count = geom.geometry_count();
    let mut polys = Vec::new();
    for i in 0..count {
        let sub = geom.get_geometry(i);
        match sub.geometry_name().as_str() {
            "POLYGON" => polys.push(gdal_polygon_to_geo(&sub)),
            "MULTIPOLYGON" => {
                let sub_count = sub.geometry_count();
                for j in 0..sub_count {
                    polys.push(gdal_polygon_to_geo(&sub.get_geometry(j)));
                }
            }
            "GEOMETRYCOLLECTION" => {
                polys.extend(extract_polygons_from_collection(&sub));
            }
            _ => {
                // Non-polygon member (e.g. LINESTRING artefact from make_valid); skip.
            }
        }
    }
    polys
}
