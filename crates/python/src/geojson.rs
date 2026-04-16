//! GeoJSON serialization for [`DelineationResult`].

use shed_core::DelineationResult;
use shed_core::RefinementOutcome;

/// Serialize a [`DelineationResult`] as a GeoJSON Feature string.
pub fn result_to_geojson_feature(result: &DelineationResult) -> Result<String, serde_json::Error> {
    let geometry = multi_polygon_to_geojson(result.geometry());

    let mut properties = serde_json::Map::new();
    properties.insert("area_km2".into(), serde_json::json!(result.area_km2().as_f64()));
    properties.insert(
        "terminal_atom_id".into(),
        serde_json::json!(result.terminal_atom_id().get()),
    );
    properties.insert("input_lon".into(), serde_json::json!(result.input_outlet().lon));
    properties.insert("input_lat".into(), serde_json::json!(result.input_outlet().lat));
    properties.insert("resolved_lon".into(), serde_json::json!(result.resolved_outlet().lon));
    properties.insert("resolved_lat".into(), serde_json::json!(result.resolved_outlet().lat));
    properties.insert(
        "upstream_atom_count".into(),
        serde_json::json!(result.upstream_atom_ids().len()),
    );
    properties.insert(
        "refinement".into(),
        serde_json::json!(format_refinement(result.refinement())),
    );

    let feature = serde_json::json!({
        "type": "Feature",
        "geometry": geometry,
        "properties": properties,
    });

    serde_json::to_string(&feature)
}

fn multi_polygon_to_geojson(mp: &geo::MultiPolygon<f64>) -> serde_json::Value {
    let polygons: Vec<serde_json::Value> = mp
        .0
        .iter()
        .map(|poly| {
            let mut rings = Vec::new();
            rings.push(ring_to_coords(poly.exterior()));
            for hole in poly.interiors() {
                rings.push(ring_to_coords(hole));
            }
            serde_json::json!(rings)
        })
        .collect();

    serde_json::json!({"type": "MultiPolygon", "coordinates": polygons})
}

fn ring_to_coords(ls: &geo::LineString<f64>) -> Vec<[f64; 2]> {
    ls.coords().map(|c| [c.x, c.y]).collect()
}

fn format_refinement(r: &RefinementOutcome) -> &'static str {
    match r {
        RefinementOutcome::Applied { .. } => "applied",
        RefinementOutcome::NoRastersAvailable => "no_rasters",
        RefinementOutcome::NoRasterSourceProvided => "no_raster_source",
        RefinementOutcome::Disabled => "disabled",
    }
}
