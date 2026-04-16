//! GeoJSON serialization for [`DelineationResult`].

use shed_core::DelineationResult;
use shed_core::RefinementOutcome;
use shed_core::ResolutionMethod;

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
        "resolution_method".into(),
        serde_json::json!(format_resolution_method(result.resolution_method())),
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

fn format_resolution_method(method: &ResolutionMethod) -> String {
    match method {
        ResolutionMethod::Snap {
            snap_id,
            distance_m,
            weight,
            mainstem_status,
            candidates_considered,
        } => format!(
            "snap(id={snap_id:?}, dist={distance_m:.1}m, weight={weight:?}, \
             mainstem={mainstem_status:?}, candidates={candidates_considered})"
        ),
        ResolutionMethod::PointInPolygon { candidates_considered, tie_break } => match tie_break {
            Some(tb) => format!("pip(candidates={candidates_considered}, tie_break={tb:?})"),
            None => format!("pip(candidates={candidates_considered})"),
        },
    }
}

fn format_refinement(r: &RefinementOutcome) -> String {
    match r {
        RefinementOutcome::Applied { refined_outlet } => {
            format!("applied(lon={:.6}, lat={:.6})", refined_outlet.lon, refined_outlet.lat)
        }
        RefinementOutcome::NoRastersAvailable => "no_rasters_available".into(),
        RefinementOutcome::NoRasterSourceProvided => "no_raster_source_provided".into(),
        RefinementOutcome::Disabled => "disabled".into(),
    }
}
