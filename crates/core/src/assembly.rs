//! Component 6 orchestration: fetch catchment geometries and assemble the final watershed.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use geo::{MultiPolygon, Polygon};
use hfx_core::AtomId;
use tracing::{debug, instrument};

use crate::algo::{
    AreaKm2, CleanEpsilon, DissolveError, GeometryRepair, GeometryRepairError, HoleFillMode,
    UpstreamAtoms, WatershedAreaError, WatershedGeometry, WkbDecodeError, dissolve,
};
use crate::error::SessionError;
use crate::reader::catchment_store::{
    CatchmentGeometryQueryError, CatchmentStore, DecodedCatchmentGeometryRow,
};

/// Result of a successful internal watershed assembly.
#[derive(Debug, Clone)]
pub(crate) struct AssemblyResult {
    geometry: MultiPolygon<f64>,
    area: AreaKm2,
}

impl AssemblyResult {
    /// Return the canonical assembled watershed geometry.
    pub(crate) fn geometry(&self) -> &MultiPolygon<f64> {
        &self.geometry
    }

    /// Return the geodesic watershed area in km².
    pub(crate) fn area(&self) -> AreaKm2 {
        self.area
    }

    /// Consume the result and return its geometry and area.
    pub(crate) fn into_parts(self) -> (MultiPolygon<f64>, AreaKm2) {
        (self.geometry, self.area)
    }
}

/// Options controlling post-dissolve assembly behavior.
#[derive(Clone, Copy)]
pub(crate) struct AssemblyOptions<'a> {
    hole_fill_mode: HoleFillMode,
    clean_epsilon: CleanEpsilon,
    geometry_repair: Option<&'a dyn GeometryRepair>,
}

impl<'a> AssemblyOptions<'a> {
    /// Create options with explicit hole-fill and cleaning inputs.
    pub(crate) fn new(hole_fill_mode: HoleFillMode, clean_epsilon: CleanEpsilon) -> Self {
        Self {
            hole_fill_mode,
            clean_epsilon,
            geometry_repair: None,
        }
    }

    /// Use a backend geometry-repair implementation instead of pure-Rust cleaning.
    pub(crate) fn with_geometry_repair(self, repairer: &'a dyn GeometryRepair) -> Self {
        Self {
            geometry_repair: Some(repairer),
            ..self
        }
    }
}

/// Errors from final watershed assembly.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AssemblyError {
    /// Querying the catchment store failed.
    #[error("failed to query catchment geometries: {source}")]
    CatchmentQuery {
        /// Underlying catchment read error.
        source: SessionError,
    },

    /// One or more requested upstream atom IDs had no catchment row.
    #[error("catchment rows missing for upstream atoms: {missing_ids:?}")]
    MissingCatchments {
        /// Requested atom IDs that were absent from the catchment store query result.
        missing_ids: Vec<AtomId>,
    },

    /// The catchment query returned the same atom ID more than once.
    #[error("duplicate catchment row returned for atom {atom_id:?}")]
    DuplicateCatchment {
        /// The duplicated atom ID.
        atom_id: AtomId,
    },

    /// A required catchment geometry failed WKB decode or had the wrong type.
    #[error("failed to decode geometry for atom {atom_id:?}: {source}")]
    GeometryDecode {
        /// Atom whose stored geometry failed decode.
        atom_id: AtomId,
        /// Underlying WKB decode error.
        source: WkbDecodeError,
    },

    /// A decoded catchment geometry contained no polygons.
    #[error("catchment geometry for atom {atom_id:?} is empty")]
    EmptyCatchmentGeometry {
        /// Atom whose decoded geometry was empty.
        atom_id: AtomId,
    },

    /// The supplied refined terminal override contained no polygons.
    #[error("refined terminal geometry for atom {atom_id:?} is empty")]
    EmptyRefinedTerminalGeometry {
        /// Terminal atom whose override was empty.
        atom_id: AtomId,
    },

    /// Dissolving the polygon parts failed.
    #[error("failed to dissolve assembled polygons: {source}")]
    Dissolve {
        /// Underlying dissolve error.
        source: DissolveError,
    },

    /// Backend topology repair failed.
    #[error("backend topology repair failed: {source}")]
    TopologyRepair {
        /// Underlying geometry repair error.
        source: GeometryRepairError,
    },

    /// The assembled geometry vanished during cleanup or repair.
    #[error("assembled watershed geometry is empty after cleanup")]
    EmptyAssembledGeometry,

    /// Final geodesic area computation failed.
    #[error("failed to compute final area: {source}")]
    Area {
        /// Underlying area computation error.
        source: WatershedAreaError,
    },
}

/// Assemble the final watershed geometry from traversed catchment atoms.
///
/// Fetches all requested catchment rows, validates full coverage, substitutes
/// the terminal geometry when `refined_terminal_geometry` is present, then
/// dissolves and post-processes the result into a canonical multi-polygon.
#[instrument(
    skip_all,
    fields(atom_count = upstream.len(), terminal = upstream.terminal().get())
)]
pub(crate) fn assemble_watershed(
    catchments: &CatchmentStore,
    upstream: &UpstreamAtoms,
    refined_terminal_geometry: Option<&MultiPolygon<f64>>,
    options: AssemblyOptions<'_>,
) -> Result<AssemblyResult, AssemblyError> {
    let terminal = upstream.terminal();
    let query_ids: Vec<AtomId> = match refined_terminal_geometry {
        Some(_) => upstream
            .atom_ids()
            .iter()
            .copied()
            .filter(|id| *id != terminal)
            .collect(),
        None => upstream.atom_ids().to_vec(),
    };
    let fetched = catchments
        .query_geometries_by_ids(&query_ids)
        .map_err(map_geometry_query_error)?;

    let mut atom_map = index_catchments_by_id(fetched)?;
    if let Some(override_geometry) = refined_terminal_geometry
        && catchments.contains_id(terminal)
    {
        match atom_map.entry(terminal) {
            Entry::Occupied(_) => {
                return Err(AssemblyError::DuplicateCatchment { atom_id: terminal });
            }
            Entry::Vacant(entry) => {
                entry.insert(override_geometry.clone());
            }
        }
    }
    let missing_ids: Vec<AtomId> = upstream
        .atom_ids()
        .iter()
        .copied()
        .filter(|id| !atom_map.contains_key(id))
        .collect();
    if !missing_ids.is_empty() {
        return Err(AssemblyError::MissingCatchments { missing_ids });
    }

    let mut geometries = Vec::with_capacity(upstream.len());

    for atom_id in upstream.atom_ids() {
        let geometry =
            atom_map
                .remove(atom_id)
                .ok_or_else(|| AssemblyError::MissingCatchments {
                    missing_ids: vec![*atom_id],
                })?;

        if *atom_id == terminal && refined_terminal_geometry.is_some() {
            if geometry.0.is_empty() {
                return Err(AssemblyError::EmptyRefinedTerminalGeometry { atom_id: terminal });
            }
        } else if geometry.0.is_empty() {
            return Err(AssemblyError::EmptyCatchmentGeometry { atom_id: *atom_id });
        }
        geometries.push(geometry);
    }

    debug!(
        geometry_count = geometries.len(),
        "assembled catchment geometries"
    );
    assemble_from_geometries(geometries, options)
}

fn index_catchments_by_id(
    fetched: Vec<DecodedCatchmentGeometryRow>,
) -> Result<HashMap<AtomId, MultiPolygon<f64>>, AssemblyError> {
    let mut atom_map = HashMap::with_capacity(fetched.len());
    for atom in fetched {
        let (atom_id, geometry) = atom.into_parts();
        match atom_map.entry(atom_id) {
            Entry::Occupied(_) => {
                return Err(AssemblyError::DuplicateCatchment { atom_id });
            }
            Entry::Vacant(entry) => {
                entry.insert(geometry);
            }
        }
    }
    Ok(atom_map)
}

fn map_geometry_query_error(source: CatchmentGeometryQueryError) -> AssemblyError {
    match source {
        CatchmentGeometryQueryError::Read { source } => AssemblyError::CatchmentQuery { source },
        CatchmentGeometryQueryError::Decode { atom_id, source } => {
            AssemblyError::GeometryDecode { atom_id, source }
        }
    }
}

fn assemble_from_geometries(
    geometries: Vec<MultiPolygon<f64>>,
    options: AssemblyOptions<'_>,
) -> Result<AssemblyResult, AssemblyError> {
    let polygons: Vec<Polygon<f64>> = geometries.into_iter().flat_map(|mp| mp.0).collect();

    let dissolved = dissolve(polygons).map_err(|source| AssemblyError::Dissolve { source })?;

    let filled = match options.geometry_repair {
        Some(repairer) => WatershedGeometry::from_dissolved(dissolved)
            .repair_topology(repairer, options.clean_epsilon)
            .map_err(|source| AssemblyError::TopologyRepair { source })?
            .fill_holes(options.hole_fill_mode),
        None => WatershedGeometry::from_dissolved(dissolved)
            .clean_topology(options.clean_epsilon)
            .fill_holes(options.hole_fill_mode),
    };

    let geometry = filled.into_canonical_multi_polygon();
    if geometry.0.is_empty() {
        return Err(AssemblyError::EmptyAssembledGeometry);
    }

    let area = crate::algo::geodesic_area_multi(&geometry)
        .map_err(|source| AssemblyError::Area { source })?;

    Ok(AssemblyResult { geometry, area })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{BinaryBuilder, Float32Builder, Int64Builder, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use geo::algorithm::winding_order::Winding;
    use geo::{Area, LineString, MultiPolygon, Polygon};
    use hfx_core::{AdjacencyRow, DrainageGraph};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use tempfile::NamedTempFile;

    use super::*;
    use crate::algo::{
        DEFAULT_CLEANING_EPSILON, GeometryRepairError, HoleFillMode, collect_upstream,
    };

    #[derive(Clone)]
    struct CatchmentRow {
        id: i64,
        geometry: Vec<u8>,
        bbox: (f32, f32, f32, f32),
    }

    struct PassthroughRepair;

    impl GeometryRepair for PassthroughRepair {
        fn repair(
            &self,
            geometry: MultiPolygon<f64>,
            _epsilon: CleanEpsilon,
        ) -> Result<MultiPolygon<f64>, GeometryRepairError> {
            Ok(geometry)
        }
    }

    struct CountingRepair<'a> {
        calls: &'a std::cell::Cell<usize>,
    }

    impl GeometryRepair for CountingRepair<'_> {
        fn repair(
            &self,
            geometry: MultiPolygon<f64>,
            _epsilon: CleanEpsilon,
        ) -> Result<MultiPolygon<f64>, GeometryRepairError> {
            self.calls.set(self.calls.get() + 1);
            Ok(geometry)
        }
    }

    struct EmptyRepair;

    impl GeometryRepair for EmptyRepair {
        fn repair(
            &self,
            _geometry: MultiPolygon<f64>,
            _epsilon: CleanEpsilon,
        ) -> Result<MultiPolygon<f64>, GeometryRepairError> {
            Ok(MultiPolygon::new(vec![]))
        }
    }

    #[test]
    fn assemble_watershed_succeeds_with_coarse_catchments_only() {
        let tmp = NamedTempFile::new().unwrap();
        write_catchments_fixture(
            tmp.path(),
            &[
                CatchmentRow {
                    id: 1,
                    geometry: polygon_wkb(0.0, 0.0, 1.0, 1.0),
                    bbox: (0.0, 0.0, 1.0, 1.0),
                },
                CatchmentRow {
                    id: 2,
                    geometry: polygon_wkb(1.0, 0.0, 2.0, 1.0),
                    bbox: (1.0, 0.0, 2.0, 1.0),
                },
            ],
        );

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let upstream = linear_upstream(&[1, 2]);
        let result = assemble_watershed(
            &store,
            &upstream,
            None,
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON),
        )
        .unwrap();

        assert_eq!(result.geometry().0.len(), 1);
        assert!(result.area().as_f64() > 0.0);
    }

    #[test]
    fn terminal_override_replaces_coarse_terminal_geometry() {
        let tmp = NamedTempFile::new().unwrap();
        write_catchments_fixture(
            tmp.path(),
            &[
                CatchmentRow {
                    id: 1,
                    geometry: polygon_wkb(0.0, 0.0, 1.0, 1.0),
                    bbox: (0.0, 0.0, 1.0, 1.0),
                },
                CatchmentRow {
                    id: 2,
                    geometry: polygon_wkb(1.0, 0.0, 3.0, 1.0),
                    bbox: (1.0, 0.0, 3.0, 1.0),
                },
            ],
        );

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let upstream = linear_upstream(&[1, 2]);
        let refined_terminal = MultiPolygon::new(vec![rect(1.0, 0.0, 2.0, 1.0)]);

        let result = assemble_watershed(
            &store,
            &upstream,
            Some(&refined_terminal),
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON),
        )
        .unwrap();

        let expected = 2.0_f64;
        let actual = result.geometry().unsigned_area();
        assert!(
            (actual - expected).abs() < 0.05,
            "expected area ~{expected}, got {actual}"
        );
    }

    #[test]
    fn terminal_override_bypasses_bad_terminal_wkb() {
        let tmp = NamedTempFile::new().unwrap();
        write_catchments_fixture(
            tmp.path(),
            &[
                CatchmentRow {
                    id: 1,
                    geometry: polygon_wkb(0.0, 0.0, 1.0, 1.0),
                    bbox: (0.0, 0.0, 1.0, 1.0),
                },
                CatchmentRow {
                    id: 2,
                    geometry: vec![0xFF, 0xFF, 0xFF],
                    bbox: (1.0, 0.0, 2.0, 1.0),
                },
            ],
        );

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let upstream = linear_upstream(&[1, 2]);
        let refined_terminal = MultiPolygon::new(vec![rect(1.0, 0.0, 2.0, 1.0)]);

        let result = assemble_watershed(
            &store,
            &upstream,
            Some(&refined_terminal),
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON),
        )
        .unwrap();

        assert!(result.area().as_f64() > 0.0);
    }

    #[test]
    fn terminal_override_does_not_rescue_missing_terminal_catchment() {
        let tmp = NamedTempFile::new().unwrap();
        write_catchments_fixture(
            tmp.path(),
            &[
                CatchmentRow {
                    id: 1,
                    geometry: polygon_wkb(0.0, 0.0, 1.0, 1.0),
                    bbox: (0.0, 0.0, 1.0, 1.0),
                },
                CatchmentRow {
                    id: 2,
                    geometry: polygon_wkb(1.0, 0.0, 2.0, 1.0),
                    bbox: (1.0, 0.0, 2.0, 1.0),
                },
            ],
        );

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let upstream = linear_upstream(&[1, 2, 3]);
        let refined_terminal = MultiPolygon::new(vec![rect(2.0, 0.0, 3.0, 1.0)]);

        let err = assemble_watershed(
            &store,
            &upstream,
            Some(&refined_terminal),
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON),
        )
        .unwrap_err();

        match err {
            AssemblyError::MissingCatchments { missing_ids } => {
                assert_eq!(missing_ids, vec![aid(3)]);
            }
            other => panic!("expected MissingCatchments, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_fetched_id_is_hard_failure() {
        let fetched = vec![
            DecodedCatchmentGeometryRow::new(
                aid(1),
                MultiPolygon::new(vec![rect(0.0, 0.0, 1.0, 1.0)]),
            ),
            DecodedCatchmentGeometryRow::new(
                aid(1),
                MultiPolygon::new(vec![rect(1.0, 0.0, 2.0, 1.0)]),
            ),
        ];

        let err = index_catchments_by_id(fetched).unwrap_err();

        assert!(matches!(err, AssemblyError::DuplicateCatchment { atom_id } if atom_id == aid(1)));
    }

    #[test]
    fn decode_failure_is_hard_failure() {
        let tmp = NamedTempFile::new().unwrap();
        write_catchments_fixture(
            tmp.path(),
            &[
                CatchmentRow {
                    id: 1,
                    geometry: vec![0xFF, 0xFF, 0xFF],
                    bbox: (0.0, 0.0, 1.0, 1.0),
                },
                CatchmentRow {
                    id: 2,
                    geometry: polygon_wkb(1.0, 0.0, 2.0, 1.0),
                    bbox: (1.0, 0.0, 2.0, 1.0),
                },
            ],
        );

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let upstream = linear_upstream(&[1, 2]);

        let err = assemble_watershed(
            &store,
            &upstream,
            None,
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON),
        )
        .unwrap_err();

        assert!(matches!(err, AssemblyError::GeometryDecode { atom_id, .. } if atom_id == aid(1)));
    }

    #[test]
    fn wrong_geometry_type_is_hard_failure() {
        let tmp = NamedTempFile::new().unwrap();
        write_catchments_fixture(
            tmp.path(),
            &[
                CatchmentRow {
                    id: 1,
                    geometry: linestring_wkb(),
                    bbox: (0.0, 0.0, 1.0, 1.0),
                },
                CatchmentRow {
                    id: 2,
                    geometry: polygon_wkb(1.0, 0.0, 2.0, 1.0),
                    bbox: (1.0, 0.0, 2.0, 1.0),
                },
            ],
        );

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let upstream = linear_upstream(&[1, 2]);

        let err = assemble_watershed(
            &store,
            &upstream,
            None,
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON),
        )
        .unwrap_err();

        assert!(matches!(err, AssemblyError::GeometryDecode { atom_id, .. } if atom_id == aid(1)));
    }

    #[test]
    fn empty_refined_terminal_override_is_hard_failure() {
        let tmp = NamedTempFile::new().unwrap();
        write_catchments_fixture(
            tmp.path(),
            &[
                CatchmentRow {
                    id: 1,
                    geometry: polygon_wkb(0.0, 0.0, 1.0, 1.0),
                    bbox: (0.0, 0.0, 1.0, 1.0),
                },
                CatchmentRow {
                    id: 2,
                    geometry: polygon_wkb(1.0, 0.0, 2.0, 1.0),
                    bbox: (1.0, 0.0, 2.0, 1.0),
                },
            ],
        );

        let store = CatchmentStore::open(tmp.path()).unwrap();
        let upstream = linear_upstream(&[1, 2]);
        let empty_override = MultiPolygon::new(vec![]);

        let err = assemble_watershed(
            &store,
            &upstream,
            Some(&empty_override),
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            AssemblyError::EmptyRefinedTerminalGeometry { atom_id } if atom_id == aid(2)
        ));
    }

    #[test]
    fn disjoint_inputs_remain_multipolygon_and_area_uses_all_parts() {
        let repairer = PassthroughRepair;
        let result = assemble_from_geometries(
            vec![MultiPolygon::new(vec![
                rect(0.0, 0.0, 1.0, 1.0),
                rect(2.0, 0.0, 3.0, 1.0),
            ])],
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON)
                .with_geometry_repair(&repairer),
        )
        .unwrap();

        assert_eq!(result.geometry().0.len(), 2);
        assert!(result.area().as_f64() > 20_000.0);
    }

    #[test]
    fn repairer_is_invoked_when_present() {
        let calls = std::cell::Cell::new(0usize);
        let repairer = CountingRepair { calls: &calls };

        let _ = assemble_from_geometries(
            vec![MultiPolygon::new(vec![rect(0.0, 0.0, 1.0, 1.0)])],
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON)
                .with_geometry_repair(&repairer),
        )
        .unwrap();

        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn repair_returning_empty_geometry_is_hard_failure() {
        let repairer = EmptyRepair;

        let err = assemble_from_geometries(
            vec![MultiPolygon::new(vec![rect(0.0, 0.0, 1.0, 1.0)])],
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON)
                .with_geometry_repair(&repairer),
        )
        .unwrap_err();

        assert!(matches!(err, AssemblyError::EmptyAssembledGeometry));
    }

    #[test]
    fn final_geometry_is_canonicalized_before_storage() {
        let repairer = PassthroughRepair;
        let input = MultiPolygon::new(vec![clockwise_polygon_with_ccw_hole()]);

        let result = assemble_from_geometries(
            vec![input],
            AssemblyOptions::new(
                HoleFillMode::BelowThreshold {
                    threshold_pixels: 1,
                    pixel_area: 0.0,
                },
                DEFAULT_CLEANING_EPSILON,
            )
            .with_geometry_repair(&repairer),
        )
        .unwrap();

        let polygon = &result.geometry().0[0];
        assert!(polygon.exterior().is_ccw());
        assert!(polygon.interiors().iter().all(|ring| ring.is_cw()));
    }

    #[test]
    fn canonicalization_prevents_bad_geodesic_area_behavior_on_reversed_input() {
        let repairer = PassthroughRepair;
        let mut expected = rect(0.0, 0.0, 1.0, 1.0);
        expected.exterior_mut(|line| line.make_ccw_winding());

        let result = assemble_from_geometries(
            vec![MultiPolygon::new(vec![clockwise_rect(0.0, 0.0, 1.0, 1.0)])],
            AssemblyOptions::new(HoleFillMode::RemoveAll, DEFAULT_CLEANING_EPSILON)
                .with_geometry_repair(&repairer),
        )
        .unwrap();

        let expected_area =
            crate::algo::geodesic_area_multi(&MultiPolygon::new(vec![expected])).unwrap();
        let diff = (result.area().as_f64() - expected_area.as_f64()).abs();
        assert!(diff < 1.0, "expected area diff < 1km², got {diff}");
    }

    fn write_catchments_fixture(path: &Path, rows: &[CatchmentRow]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let props = WriterProperties::builder()
            .set_max_row_group_size(1024)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        let mut id_b = Int64Builder::new();
        let mut area_b = Float32Builder::new();
        let mut up_area_b = Float32Builder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();

        for row in rows {
            id_b.append_value(row.id);
            area_b.append_value(1.0);
            up_area_b.append_null();
            minx_b.append_value(row.bbox.0);
            miny_b.append_value(row.bbox.1);
            maxx_b.append_value(row.bbox.2);
            maxy_b.append_value(row.bbox.3);
            geom_b.append_value(&row.geometry);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_b.finish()),
                Arc::new(area_b.finish()),
                Arc::new(up_area_b.finish()),
                Arc::new(minx_b.finish()),
                Arc::new(miny_b.finish()),
                Arc::new(maxx_b.finish()),
                Arc::new(maxy_b.finish()),
                Arc::new(geom_b.finish()),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn linear_upstream(ids: &[i64]) -> UpstreamAtoms {
        let rows: Vec<AdjacencyRow> = ids
            .iter()
            .enumerate()
            .map(|(idx, raw)| {
                let upstream_ids = if idx == 0 {
                    vec![]
                } else {
                    vec![aid(ids[idx - 1])]
                };
                AdjacencyRow::new(aid(*raw), upstream_ids)
            })
            .collect();
        let graph = DrainageGraph::new(rows).unwrap();
        collect_upstream(aid(*ids.last().unwrap()), &graph).unwrap()
    }

    fn aid(raw: i64) -> AtomId {
        AtomId::new(raw).unwrap()
    }

    fn rect(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]),
            vec![],
        )
    }

    fn clockwise_rect(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![(x0, y0), (x0, y1), (x1, y1), (x1, y0), (x0, y0)]),
            vec![],
        )
    }

    fn clockwise_polygon_with_ccw_hole() -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![
                (0.0, 0.0),
                (0.0, 2.0),
                (2.0, 2.0),
                (2.0, 0.0),
                (0.0, 0.0),
            ]),
            vec![LineString::from(vec![
                (0.5, 0.5),
                (1.5, 0.5),
                (1.5, 1.5),
                (0.5, 1.5),
                (0.5, 0.5),
            ])],
        )
    }

    fn polygon_wkb(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(0x01);
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&5u32.to_le_bytes());
        for (x, y) in &[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ] {
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
        }
        bytes
    }

    fn linestring_wkb() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(0x01);
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        for (x, y) in &[(0.0f64, 0.0f64), (1.0, 1.0)] {
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
        }
        bytes
    }
}
