//! Arrow schema and GeoParquet footer metadata builders.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;

use crate::export::BasinBbox;

/// Basin export schema profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasinExportSchemaProfile {
    /// Include nullable provenance columns.
    WithProvenance,
    /// Include only required export columns.
    Minimal,
}

/// Build the Arrow schema for basin GeoParquet export rows.
pub fn basin_export_schema(profile: BasinExportSchemaProfile) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("basin_id", DataType::Utf8, false),
        Field::new("delineation", DataType::Utf8, false),
        Field::new("geometry", DataType::Binary, false),
        Field::new("outlet_lon", DataType::Float64, false),
        Field::new("outlet_lat", DataType::Float64, false),
        Field::new("area_km2", DataType::Float64, false),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
    ];

    if profile == BasinExportSchemaProfile::WithProvenance {
        fields.extend([
            Field::new("resolution_method", DataType::Utf8, true),
            Field::new("refinement_status", DataType::Utf8, true),
            Field::new(
                "upstream_unit_ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new("adapter_version", DataType::Utf8, true),
        ]);
    }

    Arc::new(Schema::new(fields))
}

/// Build the Arrow schema for pre-merge unit-bundle GeoParquet export rows.
pub fn unit_bundle_export_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unit_id", DataType::Int64, false),
        Field::new("level", DataType::Int16, false),
        Field::new("area_km2", DataType::Float64, false),
        Field::new("up_area_km2", DataType::Float64, true),
        Field::new("outlet_lon", DataType::Float64, false),
        Field::new("outlet_lat", DataType::Float64, false),
        Field::new("geometry", DataType::Binary, false),
        Field::new("bbox_minx", DataType::Float32, false),
        Field::new("bbox_miny", DataType::Float32, false),
        Field::new("bbox_maxx", DataType::Float32, false),
        Field::new("bbox_maxy", DataType::Float32, false),
        Field::new("terminal_unit_id", DataType::Int64, false),
        Field::new("delineation", DataType::Utf8, false),
        Field::new("refinement_status", DataType::Utf8, true),
    ]))
}

/// Serialize the GeoParquet `geo` footer JSON.
pub fn geo_footer_json(dataset_bbox: BasinBbox) -> String {
    let metadata = json!({
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["MultiPolygon"],
                "crs": epsg_4326_projjson(),
                "bbox": [
                    dataset_bbox.minx,
                    dataset_bbox.miny,
                    dataset_bbox.maxx,
                    dataset_bbox.maxy
                ]
            }
        }
    });
    metadata.to_string()
}

fn epsg_4326_projjson() -> serde_json::Value {
    json!({
        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
        "type": "GeographicCRS",
        "name": "WGS 84",
        "datum_ensemble": {
            "name": "World Geodetic System 1984 ensemble",
            "members": [
                {
                    "name": "World Geodetic System 1984 (Transit)",
                    "id": {
                        "authority": "EPSG",
                        "code": 1166
                    }
                },
                {
                    "name": "World Geodetic System 1984 (G730)",
                    "id": {
                        "authority": "EPSG",
                        "code": 1152
                    }
                },
                {
                    "name": "World Geodetic System 1984 (G873)",
                    "id": {
                        "authority": "EPSG",
                        "code": 1153
                    }
                },
                {
                    "name": "World Geodetic System 1984 (G1150)",
                    "id": {
                        "authority": "EPSG",
                        "code": 1154
                    }
                },
                {
                    "name": "World Geodetic System 1984 (G1674)",
                    "id": {
                        "authority": "EPSG",
                        "code": 1155
                    }
                },
                {
                    "name": "World Geodetic System 1984 (G1762)",
                    "id": {
                        "authority": "EPSG",
                        "code": 1156
                    }
                },
                {
                    "name": "World Geodetic System 1984 (G2139)",
                    "id": {
                        "authority": "EPSG",
                        "code": 1309
                    }
                }
            ],
            "ellipsoid": {
                "name": "WGS 84",
                "semi_major_axis": 6378137,
                "inverse_flattening": 298.257223563
            },
            "accuracy": "2.0",
            "id": {
                "authority": "EPSG",
                "code": 6326
            }
        },
        "coordinate_system": {
            "subtype": "ellipsoidal",
            "axis": [
                {
                    "name": "Geodetic latitude",
                    "abbreviation": "Lat",
                    "direction": "north",
                    "unit": "degree"
                },
                {
                    "name": "Geodetic longitude",
                    "abbreviation": "Lon",
                    "direction": "east",
                    "unit": "degree"
                }
            ]
        },
        "id": {
            "authority": "EPSG",
            "code": 4326
        }
    })
}

#[cfg(test)]
mod export_schema_tests {
    use arrow::datatypes::DataType;
    use serde_json::Value;

    use super::*;

    #[test]
    fn export_schema_exact_required_and_provenance_fields() {
        let schema = basin_export_schema(BasinExportSchemaProfile::WithProvenance);
        let actual = schema
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().as_str(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                ("basin_id", DataType::Utf8, false),
                ("delineation", DataType::Utf8, false),
                ("geometry", DataType::Binary, false),
                ("outlet_lon", DataType::Float64, false),
                ("outlet_lat", DataType::Float64, false),
                ("area_km2", DataType::Float64, false),
                ("bbox_minx", DataType::Float32, false),
                ("bbox_miny", DataType::Float32, false),
                ("bbox_maxx", DataType::Float32, false),
                ("bbox_maxy", DataType::Float32, false),
                ("resolution_method", DataType::Utf8, true),
                ("refinement_status", DataType::Utf8, true),
                (
                    "upstream_unit_ids",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true
                ),
                ("adapter_version", DataType::Utf8, true),
            ]
        );
    }

    #[test]
    fn export_schema_optional_provenance_can_be_excluded() {
        let schema = basin_export_schema(BasinExportSchemaProfile::Minimal);
        let names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "basin_id",
                "delineation",
                "geometry",
                "outlet_lon",
                "outlet_lat",
                "area_km2",
                "bbox_minx",
                "bbox_miny",
                "bbox_maxx",
                "bbox_maxy",
            ]
        );
    }

    #[test]
    fn unit_bundle_export_schema_exact_fields() {
        let schema = unit_bundle_export_schema();
        let actual = schema
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().as_str(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                ("unit_id", DataType::Int64, false),
                ("level", DataType::Int16, false),
                ("area_km2", DataType::Float64, false),
                ("up_area_km2", DataType::Float64, true),
                ("outlet_lon", DataType::Float64, false),
                ("outlet_lat", DataType::Float64, false),
                ("geometry", DataType::Binary, false),
                ("bbox_minx", DataType::Float32, false),
                ("bbox_miny", DataType::Float32, false),
                ("bbox_maxx", DataType::Float32, false),
                ("bbox_maxy", DataType::Float32, false),
                ("terminal_unit_id", DataType::Int64, false),
                ("delineation", DataType::Utf8, false),
                ("refinement_status", DataType::Utf8, true),
            ]
        );
    }

    #[test]
    fn export_schema_geo_footer_json_content() {
        let json = geo_footer_json(BasinBbox {
            minx: -10.0,
            miny: 40.0,
            maxx: 20.0,
            maxy: 60.0,
        });
        let value: Value = serde_json::from_str(&json).unwrap();
        let geometry = &value["columns"]["geometry"];

        assert_eq!(value["version"], "1.1.0");
        assert_eq!(value["primary_column"], "geometry");
        assert_eq!(geometry["encoding"], "WKB");
        assert_eq!(geometry["geometry_types"], json!(["MultiPolygon"]));
        assert_eq!(geometry["bbox"], json!([-10.0, 40.0, 20.0, 60.0]));
        assert_eq!(geometry["crs"]["id"]["authority"], "EPSG");
        assert_eq!(geometry["crs"]["id"]["code"], 4326);
    }

    #[test]
    fn export_schema_geo_footer_omits_covering_bbox_and_orientation() {
        let json = geo_footer_json(BasinBbox {
            minx: -10.0,
            miny: 40.0,
            maxx: 20.0,
            maxy: 60.0,
        });
        let value: Value = serde_json::from_str(&json).unwrap();

        assert!(value.get("covering").is_none());
        assert!(value["columns"]["geometry"].get("covering").is_none());
        assert!(value["columns"]["geometry"].get("orientation").is_none());
    }
}
