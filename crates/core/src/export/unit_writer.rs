//! Batch GeoParquet writer for pre-merge drainage-unit bundles.

use std::collections::BTreeSet;
use std::io::Write;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int64Builder,
    StringBuilder,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tracing::instrument;

use crate::algo::encode_wkb_multi_polygon;
use crate::export::{
    BasinBbox, DelineationLabel, ExportError, ExportMethod, FabricIdentity,
    UnitBundleSpatialSortKey, basin_bbox, geo_footer_json, outward_f32_bbox, plan_row_groups,
    unit_bundle_export_schema,
};
use crate::staged::{PreMergeDrainageUnits, TerminalRefinement};

/// Writer options for unit-bundle GeoParquet export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitBundleExportOptions {
    target_row_group_size: usize,
    delineation_label_override: Option<DelineationLabel>,
}

impl UnitBundleExportOptions {
    /// Return the target row-group size passed to Parquet writer properties.
    pub fn target_row_group_size(&self) -> usize {
        self.target_row_group_size
    }

    /// Return a copy with a caller-provided delineation label for all rows.
    pub fn with_delineation_label_override(mut self, label: DelineationLabel) -> Self {
        self.delineation_label_override = Some(label);
        self
    }
}

impl Default for UnitBundleExportOptions {
    fn default() -> Self {
        Self {
            target_row_group_size: 8_192,
            delineation_label_override: None,
        }
    }
}

/// One input bundle for the unit-bundle GeoParquet batch writer.
#[derive(Debug, Clone)]
pub struct UnitBundleExportInput<'a> {
    units: &'a PreMergeDrainageUnits,
    identity: &'a FabricIdentity,
    method: ExportMethod,
    refinement: &'a TerminalRefinement,
}

impl<'a> UnitBundleExportInput<'a> {
    /// Build a unit-bundle export input.
    pub fn new(
        units: &'a PreMergeDrainageUnits,
        identity: &'a FabricIdentity,
        method: ExportMethod,
        refinement: &'a TerminalRefinement,
    ) -> Self {
        Self {
            units,
            identity,
            method,
            refinement,
        }
    }
}

/// Writes pre-merge drainage-unit bundle rows to one GeoParquet file.
#[derive(Debug, Clone)]
pub struct UnitBundleGeoParquetWriter {
    options: UnitBundleExportOptions,
}

impl UnitBundleGeoParquetWriter {
    /// Create a writer with explicit options.
    pub fn new(options: UnitBundleExportOptions) -> Self {
        Self { options }
    }

    /// Write a complete batch to a Parquet sink.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | any input bundle is empty | [`ExportError::EmptyUnitBundle`] |
    /// | rows duplicate `(unit_id, delineation)` | [`ExportError::DuplicateUnitBundleRow`] |
    /// | geometry, row-group, Arrow, Parquet, or footer metadata fails | [`ExportError`] |
    #[instrument(skip_all, fields(bundle_count = inputs.len()))]
    pub fn write<W: Write + Send>(
        &self,
        sink: W,
        inputs: &[UnitBundleExportInput<'_>],
    ) -> Result<(), ExportError> {
        let mut rows = self.materialize_rows(inputs)?;
        if rows.is_empty() {
            return Err(ExportError::EmptyUnitBundle);
        }
        rows.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));

        let dataset_bbox = dataset_bbox(&rows);
        let geo_kv = KeyValue {
            key: "geo".to_owned(),
            value: Some(geo_footer_json(dataset_bbox)),
        };
        let schema = unit_bundle_export_schema();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.options.target_row_group_size()))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_key_value_metadata(Some(vec![geo_kv]))
            .build();
        let mut writer = ArrowWriter::try_new(sink, schema.clone(), Some(props))
            .map_err(|source| ExportError::ParquetWriteFailure { source })?;

        let row_group_plan = plan_row_groups(rows.len())?;
        let mut start = 0;
        for size in row_group_plan.sizes() {
            let end = start + *size;
            let batch = build_record_batch(schema.clone(), &rows[start..end])?;
            writer
                .write(&batch)
                .map_err(|source| ExportError::ParquetWriteFailure { source })?;
            writer
                .flush()
                .map_err(|source| ExportError::ParquetWriteFailure { source })?;
            start = end;
        }

        let metadata = writer
            .close()
            .map_err(|source| ExportError::ParquetWriteFailure { source })?;
        let footer_has_geo = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|entries| entries.iter().find(|entry| entry.key == "geo"))
            .and_then(|entry| entry.value.as_deref())
            .is_some();
        if footer_has_geo {
            Ok(())
        } else {
            Err(ExportError::FooterMetadataFailure {
                reason: "missing geo key in file-level key_value_metadata after close",
            })
        }
    }

    fn materialize_rows(
        &self,
        inputs: &[UnitBundleExportInput<'_>],
    ) -> Result<Vec<MaterializedUnitRow>, ExportError> {
        let mut seen_rows = BTreeSet::<(i64, String)>::new();
        let mut rows = Vec::new();

        for input in inputs {
            if input.units.units().is_empty() {
                return Err(ExportError::EmptyUnitBundle);
            }
            let delineation = self.delineation_label(input.identity, &input.method)?;
            let refinement_status = refinement_status(input.refinement).to_owned();
            let terminal_unit_id = input.units.terminal().get();

            for unit in input.units.units() {
                let row_key = (unit.id().get(), delineation.as_str().to_owned());
                if !seen_rows.insert(row_key.clone()) {
                    return Err(ExportError::DuplicateUnitBundleRow {
                        unit_id: row_key.0,
                        delineation: row_key.1,
                    });
                }

                let true_bbox = basin_bbox(unit.geometry())?;
                let bbox = outward_f32_bbox(true_bbox);
                let sort_key = UnitBundleSpatialSortKey::from_geometry(
                    unit.id(),
                    delineation.clone(),
                    unit.geometry(),
                )?;
                let geometry = encode_wkb_multi_polygon(unit.geometry()).map_err(|source| {
                    ExportError::UnitGeometryEncodingFailure {
                        unit_id: unit.id().get(),
                        source,
                    }
                })?;

                rows.push(MaterializedUnitRow {
                    sort_key,
                    unit_id: unit.id().get(),
                    level: unit.level().get(),
                    area_km2: f64::from(unit.area().get()),
                    up_area_km2: unit.up_area().map(|area| f64::from(area.get())),
                    outlet_lon: unit.outlet().lon(),
                    outlet_lat: unit.outlet().lat(),
                    geometry,
                    true_bbox,
                    bbox,
                    terminal_unit_id,
                    delineation: delineation.clone(),
                    refinement_status: refinement_status.clone(),
                });
            }
        }

        Ok(rows)
    }

    fn delineation_label(
        &self,
        identity: &FabricIdentity,
        method: &ExportMethod,
    ) -> Result<DelineationLabel, ExportError> {
        self.options
            .delineation_label_override
            .clone()
            .map(Ok)
            .unwrap_or_else(|| DelineationLabel::from_fabric_identity(identity, method))
    }
}

impl Default for UnitBundleGeoParquetWriter {
    fn default() -> Self {
        Self::new(UnitBundleExportOptions::default())
    }
}

struct MaterializedUnitRow {
    sort_key: UnitBundleSpatialSortKey,
    unit_id: i64,
    level: i16,
    area_km2: f64,
    up_area_km2: Option<f64>,
    outlet_lon: f64,
    outlet_lat: f64,
    geometry: Vec<u8>,
    true_bbox: BasinBbox,
    bbox: crate::export::spatial::OutwardF32Bbox,
    terminal_unit_id: i64,
    delineation: DelineationLabel,
    refinement_status: String,
}

fn dataset_bbox(rows: &[MaterializedUnitRow]) -> BasinBbox {
    rows.iter().fold(
        BasinBbox {
            minx: f64::INFINITY,
            miny: f64::INFINITY,
            maxx: f64::NEG_INFINITY,
            maxy: f64::NEG_INFINITY,
        },
        |acc, row| BasinBbox {
            minx: acc.minx.min(row.true_bbox.minx),
            miny: acc.miny.min(row.true_bbox.miny),
            maxx: acc.maxx.max(row.true_bbox.maxx),
            maxy: acc.maxy.max(row.true_bbox.maxy),
        },
    )
}

fn build_record_batch(
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    rows: &[MaterializedUnitRow],
) -> Result<RecordBatch, ExportError> {
    let mut unit_ids = Int64Builder::new();
    let mut levels = Int16Builder::new();
    let mut areas = Float64Builder::new();
    let mut up_areas = Float64Builder::new();
    let mut outlet_lons = Float64Builder::new();
    let mut outlet_lats = Float64Builder::new();
    let mut geometries = BinaryBuilder::new();
    let mut bbox_minx = Float32Builder::new();
    let mut bbox_miny = Float32Builder::new();
    let mut bbox_maxx = Float32Builder::new();
    let mut bbox_maxy = Float32Builder::new();
    let mut terminal_unit_ids = Int64Builder::new();
    let mut delineations = StringBuilder::new();
    let mut refinement_statuses = StringBuilder::new();

    for row in rows {
        unit_ids.append_value(row.unit_id);
        levels.append_value(row.level);
        areas.append_value(row.area_km2);
        if let Some(up_area) = row.up_area_km2 {
            up_areas.append_value(up_area);
        } else {
            up_areas.append_null();
        }
        outlet_lons.append_value(row.outlet_lon);
        outlet_lats.append_value(row.outlet_lat);
        geometries.append_value(&row.geometry);
        bbox_minx.append_value(row.bbox.minx);
        bbox_miny.append_value(row.bbox.miny);
        bbox_maxx.append_value(row.bbox.maxx);
        bbox_maxy.append_value(row.bbox.maxy);
        terminal_unit_ids.append_value(row.terminal_unit_id);
        delineations.append_value(row.delineation.as_str());
        refinement_statuses.append_value(&row.refinement_status);
    }

    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(unit_ids.finish()),
        std::sync::Arc::new(levels.finish()),
        std::sync::Arc::new(areas.finish()),
        std::sync::Arc::new(up_areas.finish()),
        std::sync::Arc::new(outlet_lons.finish()),
        std::sync::Arc::new(outlet_lats.finish()),
        std::sync::Arc::new(geometries.finish()),
        std::sync::Arc::new(bbox_minx.finish()),
        std::sync::Arc::new(bbox_miny.finish()),
        std::sync::Arc::new(bbox_maxx.finish()),
        std::sync::Arc::new(bbox_maxy.finish()),
        std::sync::Arc::new(terminal_unit_ids.finish()),
        std::sync::Arc::new(delineations.finish()),
        std::sync::Arc::new(refinement_statuses.finish()),
    ];

    RecordBatch::try_new(schema, columns)
        .map_err(|source| ExportError::ArrowWriteFailure { source })
}

fn refinement_status(refinement: &TerminalRefinement) -> &'static str {
    match refinement {
        TerminalRefinement::Applied { .. } => "applied",
        TerminalRefinement::BestEffortSkipped { .. } => "best_effort_skipped",
        TerminalRefinement::Disabled => "disabled",
    }
}

#[cfg(test)]
mod unit_bundle_writer_tests {
    use std::fs::File;
    use std::path::Path;

    use arrow::array::{
        Array, BinaryArray, Float32Array, Float64Array, Int16Array, Int64Array, StringArray,
    };
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;
    use geo::{BoundingRect, LineString, MultiPolygon, Polygon};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::statistics::Statistics;
    use serde_json::{Value, json};
    use tempfile::{NamedTempFile, TempDir};

    use crate::algo::GeoCoord;
    use crate::engine::{DelineationOptions, Engine};
    use crate::export::{
        ExportMethod, FabricIdentity, UnitBundleExportInput, UnitBundleGeoParquetWriter,
        UnitBundleSpatialSortKey, unit_bundle_export_schema,
    };
    use crate::refinement::{
        AppliedRefinementReason, ContainedTerminalPolygon, RefinementProvenance,
        RefinementStrategyName,
    };
    use crate::session::DatasetSession;
    use crate::staged::{PreMergeDrainageUnits, RefinementMode, TerminalRefinement};
    use crate::testutil::{DatasetBuilder, TestCatchment};

    #[test]
    fn unit_bundle_writer_round_trip_schema_and_values() {
        let fixture = unit_bundle_fixture();
        let path = write_bundle(&fixture.units, &TerminalRefinement::Disabled);
        let batches = read_batches(path.path());

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(
            batch.schema().fields(),
            unit_bundle_export_schema().fields()
        );
        assert_eq!(batch.num_rows(), 3);

        let unit_ids = i64_column(batch, 0);
        let levels = i16_column(batch, 1);
        let areas = f64_column(batch, 2);
        let up_areas = f64_column(batch, 3);
        let outlet_lons = f64_column(batch, 4);
        let geometries = binary_column(batch, 6);
        let terminal_ids = i64_column(batch, 11);
        let delineations = string_column(batch, 12);
        let refinement_statuses = string_column(batch, 13);

        assert!(unit_ids.iter().all(|value| value.is_some()));
        assert!(levels.iter().all(|value| value == Some(0)));
        assert!(
            areas
                .iter()
                .all(|value| value.is_some_and(|area| area > 0.0))
        );
        assert!(up_areas.iter().any(|value| value.is_none()));
        assert!(
            outlet_lons
                .iter()
                .all(|value| value.is_some_and(f64::is_finite))
        );
        assert!(
            geometries
                .iter()
                .all(|value| value.is_some_and(|wkb| !wkb.is_empty()))
        );
        assert!(terminal_ids.iter().all(|value| value == Some(3)));
        assert!(
            delineations
                .iter()
                .all(|value| value == Some("testfabric/1.0.0/no-refine"))
        );
        assert!(
            refinement_statuses
                .iter()
                .all(|value| value == Some("disabled"))
        );
    }

    #[test]
    fn unit_bundle_writer_footer_geo_metadata_is_file_level() {
        let fixture = unit_bundle_fixture();
        let path = write_bundle(&fixture.units, &TerminalRefinement::Disabled);
        let footer = footer_geo_json(path.path());
        let geometry = &footer["columns"]["geometry"];

        assert_eq!(footer["version"], "1.1.0");
        assert_eq!(footer["primary_column"], "geometry");
        assert_eq!(geometry["encoding"], "WKB");
        assert_eq!(geometry["geometry_types"], json!(["MultiPolygon"]));
        assert_eq!(geometry["crs"]["id"]["authority"], "EPSG");
        assert_eq!(geometry["crs"]["id"]["code"], 4326);
        assert_eq!(geometry["bbox"], json!([-100.0, -1.0, 21.0, 11.0]));
    }

    #[test]
    fn unit_bundle_writer_bbox_columns_are_outward_rounded_with_stats() {
        let fixture = unit_bundle_fixture();
        let path = write_bundle(&fixture.units, &TerminalRefinement::Disabled);
        let batches = read_batches(path.path());
        let batch = &batches[0];
        let metadata = parquet_metadata(path.path());
        let row_group = metadata.row_group(0);

        assert!(f32_stat_min(row_group.column(7).statistics()).is_some());
        assert!(f32_stat_min(row_group.column(8).statistics()).is_some());
        assert!(f32_stat_max(row_group.column(9).statistics()).is_some());
        assert!(f32_stat_max(row_group.column(10).statistics()).is_some());

        let ids = i64_column(batch, 0);
        let minx = f32_column(batch, 7);
        let miny = f32_column(batch, 8);
        let maxx = f32_column(batch, 9);
        let maxy = f32_column(batch, 10);
        for index in 0..ids.len() {
            let expected = expected_bounds(ids.value(index));
            assert!(f64::from(minx.value(index)) <= expected.0);
            assert!(f64::from(miny.value(index)) <= expected.1);
            assert!(f64::from(maxx.value(index)) >= expected.2);
            assert!(f64::from(maxy.value(index)) >= expected.3);
        }
    }

    #[test]
    fn unit_bundle_writer_hilbert_sorted_output_order() {
        let fixture = unit_bundle_fixture();
        let path = write_bundle(&fixture.units, &TerminalRefinement::Disabled);
        let batch = &read_batches(path.path())[0];
        let unit_ids = i64_column(batch, 0);
        let actual = (0..unit_ids.len())
            .map(|row| unit_ids.value(row))
            .collect::<Vec<_>>();

        let identity = identity("1.0.0");
        let delineation = crate::export::DelineationLabel::from_fabric_identity(
            &identity,
            &ExportMethod::no_refine(),
        )
        .expect("label");
        let mut expected = fixture
            .units
            .units()
            .iter()
            .map(|unit| {
                (
                    UnitBundleSpatialSortKey::from_geometry(
                        unit.id(),
                        delineation.clone(),
                        unit.geometry(),
                    )
                    .expect("sort key"),
                    unit.id().get(),
                )
            })
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        let expected = expected.into_iter().map(|(_, id)| id).collect::<Vec<_>>();

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 3);
    }

    #[test]
    fn unit_bundle_writer_empty_input_error() {
        let output = NamedTempFile::new().expect("temp file");
        let err = UnitBundleGeoParquetWriter::default()
            .write(File::create(output.path()).expect("create output"), &[])
            .expect_err("empty input should fail");

        assert!(matches!(err, crate::export::ExportError::EmptyUnitBundle));
    }

    #[test]
    fn unit_bundle_writer_duplicate_unit_and_delineation_rejected() {
        let fixture = unit_bundle_fixture();
        let identity = identity("1.0.0");
        let refinement = TerminalRefinement::Disabled;
        let inputs = [
            UnitBundleExportInput::new(
                &fixture.units,
                &identity,
                ExportMethod::no_refine(),
                &refinement,
            ),
            UnitBundleExportInput::new(
                &fixture.units,
                &identity,
                ExportMethod::no_refine(),
                &refinement,
            ),
        ];
        let output = NamedTempFile::new().expect("temp file");
        let err = UnitBundleGeoParquetWriter::default()
            .write(File::create(output.path()).expect("create output"), &inputs)
            .expect_err("duplicate input rows should fail");

        assert!(matches!(
            err,
            crate::export::ExportError::DuplicateUnitBundleRow {
                delineation, ..
            } if delineation == "testfabric/1.0.0/no-refine"
        ));
    }

    #[test]
    fn unit_bundle_writer_terminal_row_keeps_whole_unit_when_basin_is_carved() {
        let (_dir, root) = DatasetBuilder::new(3)
            .with_custom_catchments(vec![
                TestCatchment {
                    id: 1,
                    area_km2: 10.0,
                    up_area_km2: None,
                    polygon: (-1.0, 0.0, 0.0, 1.0),
                },
                TestCatchment {
                    id: 2,
                    area_km2: 20.0,
                    up_area_km2: Some(30.0),
                    polygon: (0.0, 0.0, 1.0, 1.0),
                },
                TestCatchment {
                    id: 3,
                    area_km2: 30.0,
                    up_area_km2: Some(60.0),
                    polygon: (1.0, 0.0, 3.0, 1.0),
                },
            ])
            .build();
        let session = DatasetSession::open_path(&root).expect("fixture opens");
        let engine = Engine::builder(session).build();
        let options = DelineationOptions::default();
        let resolved = engine
            .resolve_outlet_at_level(
                GeoCoord::new(2.5, 0.5),
                engine
                    .select_level(crate::staged::LevelSelection::Finest)
                    .expect("level"),
                options.resolver_config(),
            )
            .expect("resolved");
        let upstream = engine
            .traverse_upstream_at_level(&resolved)
            .expect("upstream");
        let units = engine
            .produce_pre_merge_units(&upstream)
            .expect("pre-merge units");
        let refinement = TerminalRefinement::Applied {
            refined_outlet: GeoCoord::new(1.5, 0.5),
            geometry: contained(MultiPolygon::new(vec![rect(1.0, 0.0, 2.0, 1.0)])),
            provenance: applied_provenance(),
        };
        let dissolved = engine
            .dissolve_watershed(&units, &refinement, &options)
            .expect("dissolved");

        let path = write_bundle(&units, &refinement);
        let batch = &read_batches(path.path())[0];
        let unit_ids = i64_column(batch, 0);
        let maxx = f32_column(batch, 9);
        let terminal_row = (0..unit_ids.len())
            .find(|row| unit_ids.value(*row) == 3)
            .expect("terminal row");
        let dissolved_bbox = dissolved.geometry().bounding_rect().expect("bbox");

        assert!(f64::from(maxx.value(terminal_row)) >= 3.0);
        assert_eq!(dissolved_bbox.max().x, 2.0);
    }

    struct BundleFixture {
        _dir: TempDir,
        units: PreMergeDrainageUnits,
    }

    fn unit_bundle_fixture() -> BundleFixture {
        let (_dir, root) = DatasetBuilder::new(3)
            .with_custom_catchments(vec![
                TestCatchment {
                    id: 1,
                    area_km2: 10.0,
                    up_area_km2: None,
                    polygon: (-100.0, 10.0, -99.0, 11.0),
                },
                TestCatchment {
                    id: 2,
                    area_km2: 20.0,
                    up_area_km2: Some(30.0),
                    polygon: (-1.0, -1.0, 0.0, 0.0),
                },
                TestCatchment {
                    id: 3,
                    area_km2: 30.0,
                    up_area_km2: Some(60.0),
                    polygon: (20.0, 10.0, 21.0, 11.0),
                },
            ])
            .build();
        let session = DatasetSession::open_path(&root).expect("fixture opens");
        let engine = Engine::builder(session).build();
        let selected = engine
            .select_level(crate::staged::LevelSelection::Finest)
            .expect("level");
        let resolved = engine
            .resolve_outlet_at_level(
                GeoCoord::new(20.5, 10.5),
                selected,
                DelineationOptions::default()
                    .with_refinement_mode(RefinementMode::Disabled)
                    .resolver_config(),
            )
            .expect("resolved");
        let upstream = engine
            .traverse_upstream_at_level(&resolved)
            .expect("upstream");
        let units = engine
            .produce_pre_merge_units(&upstream)
            .expect("pre-merge units");

        BundleFixture { _dir, units }
    }

    fn write_bundle(
        units: &PreMergeDrainageUnits,
        refinement: &TerminalRefinement,
    ) -> NamedTempFile {
        let identity = identity("1.0.0");
        let input =
            UnitBundleExportInput::new(units, &identity, ExportMethod::no_refine(), refinement);
        let output = NamedTempFile::new().expect("temp file");
        UnitBundleGeoParquetWriter::default()
            .write(
                File::create(output.path()).expect("create output"),
                &[input],
            )
            .expect("writer should succeed");
        output
    }

    fn identity(version: &str) -> FabricIdentity {
        FabricIdentity::new("testfabric", Some(version.to_owned()), "adapter-test")
    }

    fn read_batches(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("open parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
        builder
            .build()
            .expect("record batch reader")
            .collect::<Result<Vec<_>, _>>()
            .expect("record batches")
    }

    fn parquet_metadata(path: &Path) -> parquet::file::metadata::ParquetMetaData {
        let file = File::open(path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("serialized parquet reader");
        reader.metadata().clone()
    }

    fn footer_geo_json(path: &Path) -> Value {
        let metadata = parquet_metadata(path);
        let geo = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|entries| entries.iter().find(|entry| entry.key == "geo"))
            .and_then(|entry| entry.value.as_deref())
            .expect("geo footer key_value_metadata should exist");
        serde_json::from_str(geo).expect("geo footer should parse")
    }

    fn i64_column(batch: &RecordBatch, index: usize) -> &Int64Array {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Int64);
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 column")
    }

    fn i16_column(batch: &RecordBatch, index: usize) -> &Int16Array {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Int16);
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("i16 column")
    }

    fn f64_column(batch: &RecordBatch, index: usize) -> &Float64Array {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Float64);
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64 column")
    }

    fn f32_column(batch: &RecordBatch, index: usize) -> &Float32Array {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Float32);
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("f32 column")
    }

    fn string_column(batch: &RecordBatch, index: usize) -> &StringArray {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Utf8);
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column")
    }

    fn binary_column(batch: &RecordBatch, index: usize) -> &BinaryArray {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Binary);
        batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary column")
    }

    fn f32_stat_min(stats: Option<&Statistics>) -> Option<f32> {
        match stats? {
            Statistics::Float(typed) => typed.min_opt().copied(),
            _ => None,
        }
    }

    fn f32_stat_max(stats: Option<&Statistics>) -> Option<f32> {
        match stats? {
            Statistics::Float(typed) => typed.max_opt().copied(),
            _ => None,
        }
    }

    fn expected_bounds(unit_id: i64) -> (f64, f64, f64, f64) {
        match unit_id {
            1 => (-100.0, 10.0, -99.0, 11.0),
            2 => (-1.0, -1.0, 0.0, 0.0),
            3 => (20.0, 10.0, 21.0, 11.0),
            other => panic!("unexpected unit id {other}"),
        }
    }

    fn contained(geometry: MultiPolygon<f64>) -> ContainedTerminalPolygon {
        ContainedTerminalPolygon::new_unchecked_from_d8_carve(geometry)
            .expect("test refined terminal geometry should be non-empty")
    }

    fn applied_provenance() -> RefinementProvenance {
        RefinementProvenance::Applied {
            strategy: RefinementStrategyName::BestEffortD8IfPresent,
            why: AppliedRefinementReason::D8AuxMatchedTerminalBbox {
                declaration_index: 0,
            },
        }
    }

    fn rect(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon<f64> {
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
}
