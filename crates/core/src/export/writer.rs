//! Batch GeoParquet writer for basin export rows.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float32Builder, Float64Builder, Int64Builder, ListBuilder,
    StringBuilder,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tracing::instrument;

use crate::engine::{DelineationResult, RefinementOutcome};
use crate::export::{
    BasinBbox, BasinExportSchemaProfile, BasinId, BasinSpatialSortKey, DelineationLabel,
    ExportError, ExportMethod, ExportOrigin, FabricIdentity, basin_bbox, basin_export_schema,
    geo_footer_json, outward_f32_bbox, plan_row_groups,
};

/// Writer options for basin GeoParquet export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportOptions {
    include_provenance: ProvenanceColumns,
    target_row_group_size: usize,
    delineation_label_override: Option<DelineationLabel>,
}

impl ExportOptions {
    /// Return whether optional provenance columns are written.
    pub fn includes_provenance(&self) -> bool {
        self.include_provenance == ProvenanceColumns::Included
    }

    /// Return the target row-group size passed to Parquet writer properties.
    pub fn target_row_group_size(&self) -> usize {
        self.target_row_group_size
    }

    /// Return a copy with optional provenance columns excluded.
    pub fn without_provenance(mut self) -> Self {
        self.include_provenance = ProvenanceColumns::Excluded;
        self
    }

    /// Return a copy with a caller-provided delineation label for all rows.
    pub fn with_delineation_label_override(mut self, label: DelineationLabel) -> Self {
        self.delineation_label_override = Some(label);
        self
    }
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            include_provenance: ProvenanceColumns::Included,
            target_row_group_size: 8_192,
            delineation_label_override: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProvenanceColumns {
    Included,
    Excluded,
}

/// One input row for the basin GeoParquet batch writer.
#[derive(Debug, Clone)]
pub enum BasinExportInput<'a> {
    /// Caller supplied an explicit basin identifier.
    Explicit {
        /// Parsed basin identity.
        basin_id: &'a BasinId,
        /// Borrowed delineation result to persist.
        result: &'a DelineationResult,
        /// Source fabric identity for default delineation labels and provenance.
        identity: &'a FabricIdentity,
        /// Method label component for default delineation labels.
        method: ExportMethod,
        /// Caller/outlet origin used in diagnostics.
        origin: &'a ExportOrigin,
    },
    /// Writer derives the basin identifier from the terminal unit ID.
    DefaultBasinId {
        /// Borrowed delineation result to persist.
        result: &'a DelineationResult,
        /// Source fabric identity for default delineation labels and provenance.
        identity: &'a FabricIdentity,
        /// Method label component for default delineation labels.
        method: ExportMethod,
        /// Caller/outlet origin used in diagnostics.
        origin: &'a ExportOrigin,
    },
}

impl<'a> BasinExportInput<'a> {
    /// Build an explicit-ID export input.
    pub fn explicit(
        basin_id: &'a BasinId,
        result: &'a DelineationResult,
        identity: &'a FabricIdentity,
        method: ExportMethod,
        origin: &'a ExportOrigin,
    ) -> Self {
        Self::Explicit {
            basin_id,
            result,
            identity,
            method,
            origin,
        }
    }

    /// Build a terminal-unit default-ID export input.
    pub fn default_basin_id(
        result: &'a DelineationResult,
        identity: &'a FabricIdentity,
        method: ExportMethod,
        origin: &'a ExportOrigin,
    ) -> Self {
        Self::DefaultBasinId {
            result,
            identity,
            method,
            origin,
        }
    }
}

/// Writes basin export rows to one GeoParquet file.
#[derive(Debug, Clone)]
pub struct BasinGeoParquetWriter {
    options: ExportOptions,
}

impl BasinGeoParquetWriter {
    /// Create a writer with explicit options.
    pub fn new(options: ExportOptions) -> Self {
        Self { options }
    }

    /// Write a complete batch to a Parquet sink.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |---|---|
    /// | input is empty | [`ExportError::EmptyInput`] |
    /// | rows duplicate `(basin_id, delineation)` | [`ExportError::DuplicateRow`] |
    /// | defaulted IDs collide across origins | [`ExportError::DefaultBasinIdCollision`] |
    /// | geometry, row-group, Arrow, Parquet, or footer metadata fails | [`ExportError`] |
    #[instrument(skip_all, fields(row_count = inputs.len()))]
    pub fn write<W: Write + Send>(
        &self,
        sink: W,
        inputs: &[BasinExportInput<'_>],
    ) -> Result<(), ExportError> {
        if inputs.is_empty() {
            return Err(ExportError::EmptyInput);
        }

        let mut rows = self.materialize_rows(inputs)?;
        rows.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));

        let dataset_bbox = dataset_bbox(&rows);
        let geo_json = geo_footer_json(dataset_bbox);
        let geo_kv = KeyValue {
            key: "geo".to_owned(),
            value: Some(geo_json),
        };

        let schema_profile = if self.options.includes_provenance() {
            BasinExportSchemaProfile::WithProvenance
        } else {
            BasinExportSchemaProfile::Minimal
        };
        let schema = basin_export_schema(schema_profile);
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
            let batch = build_record_batch(
                schema.clone(),
                &rows[start..end],
                self.options.includes_provenance(),
            )?;
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
        inputs: &[BasinExportInput<'_>],
    ) -> Result<Vec<MaterializedRow>, ExportError> {
        let mut default_origins_by_id = BTreeMap::<String, String>::new();
        let mut seen_rows = BTreeSet::<(String, String)>::new();
        let mut rows = Vec::with_capacity(inputs.len());

        for input in inputs {
            let resolved = self.resolve_input_identity(input)?;
            if resolved.is_defaulted
                && let Some(first_origin) = default_origins_by_id
                    .insert(
                        resolved.basin_id.as_str().to_owned(),
                        resolved.origin.to_string(),
                    )
                    .filter(|first| first != &resolved.origin.to_string())
            {
                return Err(ExportError::DefaultBasinIdCollision {
                    basin_id: resolved.basin_id.to_string(),
                    first_origin,
                    second_origin: resolved.origin.to_string(),
                });
            }

            let row_key = (
                resolved.basin_id.as_str().to_owned(),
                resolved.delineation.as_str().to_owned(),
            );
            if !seen_rows.insert(row_key.clone()) {
                return Err(ExportError::DuplicateRow {
                    basin_id: row_key.0,
                    delineation: row_key.1,
                });
            }

            let true_bbox = basin_bbox(resolved.result.geometry())?;
            let bbox = outward_f32_bbox(true_bbox);
            let sort_key = BasinSpatialSortKey::from_geometry(
                resolved.basin_id.clone(),
                resolved.delineation.clone(),
                resolved.result.geometry(),
            )?;
            let geometry = resolved.result.geometry_wkb().map_err(|source| {
                ExportError::GeometryEncodingFailure {
                    basin_id: resolved.basin_id.to_string(),
                    source,
                }
            })?;

            rows.push(MaterializedRow {
                sort_key,
                basin_id: resolved.basin_id,
                delineation: resolved.delineation,
                geometry,
                outlet_lon: resolved.result.resolved_outlet().lon,
                outlet_lat: resolved.result.resolved_outlet().lat,
                area_km2: resolved.result.area_km2().as_f64(),
                true_bbox,
                bbox,
                resolution_method: format!("{:?}", resolved.result.resolution_method()),
                refinement_status: refinement_status(resolved.result.refinement()).to_owned(),
                upstream_unit_ids: resolved
                    .result
                    .upstream_unit_ids()
                    .iter()
                    .map(|unit_id| unit_id.get())
                    .collect(),
                adapter_version: resolved.identity.adapter_version().to_owned(),
            });
        }

        Ok(rows)
    }

    fn resolve_input_identity<'a>(
        &self,
        input: &BasinExportInput<'a>,
    ) -> Result<ResolvedInput<'a>, ExportError> {
        match input {
            BasinExportInput::Explicit {
                basin_id,
                result,
                identity,
                method,
                origin,
            } => Ok(ResolvedInput {
                basin_id: (*basin_id).clone(),
                delineation: self.delineation_label(identity, method)?,
                result,
                identity,
                origin,
                is_defaulted: false,
            }),
            BasinExportInput::DefaultBasinId {
                result,
                identity,
                method,
                origin,
            } => Ok(ResolvedInput {
                basin_id: BasinId::from_terminal_unit_id(result.terminal_unit_id())?,
                delineation: self.delineation_label(identity, method)?,
                result,
                identity,
                origin,
                is_defaulted: true,
            }),
        }
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

impl Default for BasinGeoParquetWriter {
    fn default() -> Self {
        Self::new(ExportOptions::default())
    }
}

struct ResolvedInput<'a> {
    basin_id: BasinId,
    delineation: DelineationLabel,
    result: &'a DelineationResult,
    identity: &'a FabricIdentity,
    origin: &'a ExportOrigin,
    is_defaulted: bool,
}

struct MaterializedRow {
    sort_key: BasinSpatialSortKey,
    basin_id: BasinId,
    delineation: DelineationLabel,
    geometry: Vec<u8>,
    outlet_lon: f64,
    outlet_lat: f64,
    area_km2: f64,
    true_bbox: BasinBbox,
    bbox: crate::export::spatial::OutwardF32Bbox,
    resolution_method: String,
    refinement_status: String,
    upstream_unit_ids: Vec<i64>,
    adapter_version: String,
}

fn dataset_bbox(rows: &[MaterializedRow]) -> BasinBbox {
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
    rows: &[MaterializedRow],
    include_provenance: bool,
) -> Result<RecordBatch, ExportError> {
    let mut basin_ids = StringBuilder::new();
    let mut delineations = StringBuilder::new();
    let mut geometries = BinaryBuilder::new();
    let mut outlet_lons = Float64Builder::new();
    let mut outlet_lats = Float64Builder::new();
    let mut areas = Float64Builder::new();
    let mut bbox_minx = Float32Builder::new();
    let mut bbox_miny = Float32Builder::new();
    let mut bbox_maxx = Float32Builder::new();
    let mut bbox_maxy = Float32Builder::new();
    let mut resolution_methods = StringBuilder::new();
    let mut refinement_statuses = StringBuilder::new();
    let mut upstream_unit_ids = ListBuilder::new(Int64Builder::new());
    let mut adapter_versions = StringBuilder::new();

    for row in rows {
        basin_ids.append_value(row.basin_id.as_str());
        delineations.append_value(row.delineation.as_str());
        geometries.append_value(&row.geometry);
        outlet_lons.append_value(row.outlet_lon);
        outlet_lats.append_value(row.outlet_lat);
        areas.append_value(row.area_km2);
        bbox_minx.append_value(row.bbox.minx);
        bbox_miny.append_value(row.bbox.miny);
        bbox_maxx.append_value(row.bbox.maxx);
        bbox_maxy.append_value(row.bbox.maxy);

        if include_provenance {
            resolution_methods.append_value(&row.resolution_method);
            refinement_statuses.append_value(&row.refinement_status);
            for unit_id in &row.upstream_unit_ids {
                upstream_unit_ids.values().append_value(*unit_id);
            }
            upstream_unit_ids.append(true);
            adapter_versions.append_value(&row.adapter_version);
        }
    }

    let mut columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(basin_ids.finish()),
        std::sync::Arc::new(delineations.finish()),
        std::sync::Arc::new(geometries.finish()),
        std::sync::Arc::new(outlet_lons.finish()),
        std::sync::Arc::new(outlet_lats.finish()),
        std::sync::Arc::new(areas.finish()),
        std::sync::Arc::new(bbox_minx.finish()),
        std::sync::Arc::new(bbox_miny.finish()),
        std::sync::Arc::new(bbox_maxx.finish()),
        std::sync::Arc::new(bbox_maxy.finish()),
    ];

    if include_provenance {
        columns.extend([
            std::sync::Arc::new(resolution_methods.finish()) as ArrayRef,
            std::sync::Arc::new(refinement_statuses.finish()) as ArrayRef,
            std::sync::Arc::new(upstream_unit_ids.finish()) as ArrayRef,
            std::sync::Arc::new(adapter_versions.finish()) as ArrayRef,
        ]);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|source| ExportError::ArrowWriteFailure { source })
}

fn refinement_status(refinement: &RefinementOutcome) -> &'static str {
    match refinement {
        RefinementOutcome::Applied { .. } => "applied",
        RefinementOutcome::BestEffortSkipped { .. } => "best_effort_skipped",
        RefinementOutcome::Disabled => "disabled",
    }
}

#[cfg(test)]
mod export_writer_tests {
    use std::fs::File;
    use std::path::Path;

    use arrow::array::{Array, BinaryArray, Float64Array, StringArray};
    use arrow::datatypes::DataType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::statistics::Statistics;
    use serde_json::{Value, json};
    use tempfile::{NamedTempFile, TempDir};

    use crate::algo::GeoCoord;
    use crate::engine::{DelineationOptions, Engine};
    use crate::export::{
        BasinExportInput, BasinGeoParquetWriter, BasinId, BasinSpatialSortKey, ExportError,
        ExportMethod, ExportOrigin, FabricIdentity, basin_export_schema,
    };
    use crate::staged::RefinementMode;
    use crate::testutil::{DatasetBuilder, TestCatchment};

    use super::*;

    struct TestResult {
        _dir: TempDir,
        result: crate::engine::DelineationResult,
    }

    #[test]
    fn export_writer_round_trip_schema_and_values() {
        let test_result = delineation_for_rect(1, (-1.0, -1.0, 0.0, 0.0), (-0.5, -0.5));
        let path = write_inputs(&[explicit_input(
            "basin-a",
            &test_result.result,
            &identity("1.0.0"),
            ExportMethod::no_refine(),
            &origin("outlet-a"),
        )]);

        let batches = read_batches(path.path());
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(
            batch.schema().fields(),
            basin_export_schema(BasinExportSchemaProfile::WithProvenance).fields()
        );
        assert_eq!(batch.num_rows(), 1);

        let basin_ids = string_column(batch, 0);
        let delineations = string_column(batch, 1);
        let geometries = binary_column(batch, 2);
        let outlet_lons = f64_column(batch, 3);
        let outlet_lats = f64_column(batch, 4);
        let areas = f64_column(batch, 5);

        assert_eq!(basin_ids.value(0), "basin-a");
        assert_eq!(delineations.value(0), "testfabric/1.0.0/no-refine");
        assert!(!geometries.value(0).is_empty());
        assert_eq!(outlet_lons.value(0), -0.5);
        assert_eq!(outlet_lats.value(0), -0.5);
        assert!(areas.value(0) > 0.0);
    }

    #[test]
    fn export_writer_footer_geo_metadata_is_file_level() {
        let test_result = delineation_for_rect(1, (-10.0, 40.0, -9.0, 41.0), (-9.5, 40.5));
        let path = write_inputs(&[explicit_input(
            "basin-a",
            &test_result.result,
            &identity("2.0.0"),
            ExportMethod::d8_best_effort(),
            &origin("zurich"),
        )]);

        let footer = footer_geo_json(path.path());
        let geometry = &footer["columns"]["geometry"];

        assert_eq!(footer["version"], "1.1.0");
        assert_eq!(footer["primary_column"], "geometry");
        assert_eq!(geometry["encoding"], "WKB");
        assert_eq!(geometry["geometry_types"], json!(["MultiPolygon"]));
        assert_eq!(geometry["crs"]["id"]["authority"], "EPSG");
        assert_eq!(geometry["crs"]["id"]["code"], 4326);
        assert_eq!(geometry["bbox"], json!([-10.0, 40.0, -9.0, 41.0]));
    }

    #[test]
    fn export_writer_bbox_column_statistics_are_present() {
        let test_result = delineation_for_rect(1, (1.0, 2.0, 2.0, 3.0), (1.5, 2.5));
        let path = write_inputs(&[explicit_input(
            "basin-a",
            &test_result.result,
            &identity("1.0.0"),
            ExportMethod::no_refine(),
            &origin("outlet-a"),
        )]);
        let metadata = parquet_metadata(path.path());
        let row_group = metadata.row_group(0);

        assert!(f32_stat_min(row_group.column(6).statistics()).is_some());
        assert!(f32_stat_min(row_group.column(7).statistics()).is_some());
        assert!(f32_stat_max(row_group.column(8).statistics()).is_some());
        assert!(f32_stat_max(row_group.column(9).statistics()).is_some());
    }

    #[test]
    fn export_writer_planned_row_group_sizes_are_written() {
        let test_result = delineation_for_rect(1, (1.0, 2.0, 2.0, 3.0), (1.5, 2.5));
        let identity = identity("1.0.0");
        let origin = origin("bulk");
        let basin_ids = (0..9_000)
            .map(|index| BasinId::parse(format!("basin-{index:04}")).expect("valid basin id"))
            .collect::<Vec<_>>();
        let inputs = basin_ids
            .iter()
            .map(|basin_id| {
                BasinExportInput::explicit(
                    basin_id,
                    &test_result.result,
                    &identity,
                    ExportMethod::no_refine(),
                    &origin,
                )
            })
            .collect::<Vec<_>>();
        let path = write_inputs(&inputs);
        let metadata = parquet_metadata(path.path());

        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).num_rows(), 4_500);
        assert_eq!(metadata.row_group(1).num_rows(), 4_500);
    }

    #[test]
    fn export_writer_hilbert_sorted_output_order() {
        let west = delineation_for_rect(1, (-100.0, 10.0, -99.0, 11.0), (-99.5, 10.5));
        let east = delineation_for_rect(2, (20.0, 10.0, 21.0, 11.0), (20.5, 10.5));
        let identity = identity("1.0.0");
        let west_id = BasinId::parse("z-west").expect("valid basin id");
        let east_id = BasinId::parse("a-east").expect("valid basin id");
        let west_origin = origin("west outlet");
        let east_origin = origin("east outlet");
        let path = write_inputs(&[
            BasinExportInput::explicit(
                &east_id,
                &east.result,
                &identity,
                ExportMethod::no_refine(),
                &east_origin,
            ),
            BasinExportInput::explicit(
                &west_id,
                &west.result,
                &identity,
                ExportMethod::no_refine(),
                &west_origin,
            ),
        ]);

        let mut expected = [
            (
                BasinSpatialSortKey::from_geometry(
                    west_id.clone(),
                    DelineationLabel::from_fabric_identity(&identity, &ExportMethod::no_refine())
                        .expect("label"),
                    west.result.geometry(),
                )
                .expect("west sort key"),
                "z-west",
            ),
            (
                BasinSpatialSortKey::from_geometry(
                    east_id.clone(),
                    DelineationLabel::from_fabric_identity(&identity, &ExportMethod::no_refine())
                        .expect("label"),
                    east.result.geometry(),
                )
                .expect("east sort key"),
                "a-east",
            ),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));

        let batches = read_batches(path.path());
        let basin_ids = string_column(&batches[0], 0);
        let actual = (0..basin_ids.len())
            .map(|row| basin_ids.value(row))
            .collect::<Vec<_>>();
        let expected = expected.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn export_writer_empty_input_error() {
        let output = NamedTempFile::new().expect("temp file");
        let err = BasinGeoParquetWriter::default()
            .write(File::create(output.path()).expect("create output"), &[])
            .expect_err("empty input should fail");

        assert!(matches!(err, ExportError::EmptyInput));
    }

    #[test]
    fn export_writer_duplicate_basin_and_delineation_rejected() {
        let test_result = delineation_for_rect(1, (1.0, 2.0, 2.0, 3.0), (1.5, 2.5));
        let identity = identity("1.0.0");
        let basin_id = BasinId::parse("basin-a").expect("valid basin id");
        let origin_a = origin("outlet-a");
        let origin_b = origin("outlet-b");
        let output = NamedTempFile::new().expect("temp file");
        let inputs = [
            BasinExportInput::explicit(
                &basin_id,
                &test_result.result,
                &identity,
                ExportMethod::no_refine(),
                &origin_a,
            ),
            BasinExportInput::explicit(
                &basin_id,
                &test_result.result,
                &identity,
                ExportMethod::no_refine(),
                &origin_b,
            ),
        ];

        let err = BasinGeoParquetWriter::default()
            .write(File::create(output.path()).expect("create output"), &inputs)
            .expect_err("duplicate row should fail");

        assert!(matches!(
            err,
            ExportError::DuplicateRow {
                basin_id,
                delineation
            } if basin_id == "basin-a" && delineation == "testfabric/1.0.0/no-refine"
        ));
    }

    #[test]
    fn export_writer_same_basin_id_with_different_delineation_is_accepted() {
        let test_result = delineation_for_rect(1, (1.0, 2.0, 2.0, 3.0), (1.5, 2.5));
        let basin_id = BasinId::parse("basin-a").expect("valid basin id");
        let origin_a = origin("outlet-a");
        let origin_b = origin("outlet-b");
        let identity_a = identity("1.0.0");
        let identity_b = identity("2.0.0");
        let path = write_inputs(&[
            BasinExportInput::explicit(
                &basin_id,
                &test_result.result,
                &identity_a,
                ExportMethod::no_refine(),
                &origin_a,
            ),
            BasinExportInput::explicit(
                &basin_id,
                &test_result.result,
                &identity_b,
                ExportMethod::no_refine(),
                &origin_b,
            ),
        ]);

        let batches = read_batches(path.path());
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn export_writer_default_id_collision_names_both_origins() {
        let test_result = delineation_for_rect(7, (1.0, 2.0, 2.0, 3.0), (1.5, 2.5));
        let identity = identity("1.0.0");
        let origin_a = origin("outlet alpha");
        let origin_b = origin("outlet beta");
        let output = NamedTempFile::new().expect("temp file");
        let inputs = [
            BasinExportInput::default_basin_id(
                &test_result.result,
                &identity,
                ExportMethod::no_refine(),
                &origin_a,
            ),
            BasinExportInput::default_basin_id(
                &test_result.result,
                &identity,
                ExportMethod::no_refine(),
                &origin_b,
            ),
        ];

        let err = BasinGeoParquetWriter::default()
            .write(File::create(output.path()).expect("create output"), &inputs)
            .expect_err("default collision should fail");
        let message = err.to_string();

        assert!(matches!(err, ExportError::DefaultBasinIdCollision { .. }));
        assert!(message.contains("outlet alpha"));
        assert!(message.contains("outlet beta"));
        assert!(message.contains("supply explicit basin_id values"));
    }

    fn explicit_input<'a>(
        raw_basin_id: &'a str,
        result: &'a crate::engine::DelineationResult,
        identity: &'a FabricIdentity,
        method: ExportMethod,
        origin: &'a ExportOrigin,
    ) -> BasinExportInput<'a> {
        let basin_id = Box::leak(Box::new(
            BasinId::parse(raw_basin_id).expect("test basin id should parse"),
        ));
        BasinExportInput::explicit(basin_id, result, identity, method, origin)
    }

    fn identity(version: &str) -> FabricIdentity {
        FabricIdentity::new("testfabric", Some(version.to_owned()), "adapter-test")
    }

    fn origin(description: &str) -> ExportOrigin {
        ExportOrigin::new(description)
    }

    fn delineation_for_rect(id: i64, rect: (f64, f64, f64, f64), outlet: (f64, f64)) -> TestResult {
        let (_dir, root) = DatasetBuilder::new(1)
            .with_custom_catchments(vec![TestCatchment {
                id,
                area_km2: 10.0,
                up_area_km2: None,
                polygon: rect,
            }])
            .build();
        let session = crate::session::DatasetSession::open_path(&root).expect("fixture opens");
        let engine = Engine::builder(session).build();
        let result = engine
            .delineate(
                GeoCoord::new(outlet.0, outlet.1),
                &DelineationOptions::default().with_refinement_mode(RefinementMode::Disabled),
            )
            .expect("fixture delineates");
        TestResult { _dir, result }
    }

    fn write_inputs(inputs: &[BasinExportInput<'_>]) -> NamedTempFile {
        let output = NamedTempFile::new().expect("temp file");
        BasinGeoParquetWriter::default()
            .write(File::create(output.path()).expect("create output"), inputs)
            .expect("writer should succeed");
        output
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

    fn string_column(batch: &RecordBatch, index: usize) -> &StringArray {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Utf8);
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column")
    }

    fn binary_column(batch: &RecordBatch, index: usize) -> &BinaryArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary column")
    }

    fn f64_column(batch: &RecordBatch, index: usize) -> &Float64Array {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64 column")
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
}

#[cfg(test)]
mod export_golden_tests {
    use std::fs::File;
    use std::path::PathBuf;

    use arrow::array::{Array, BinaryArray, Float32Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use serde_json::{Value, json};

    const FIXTURE_PATH: &str = "tests/fixtures/export/basin-geoparquet-golden.parquet";

    #[test]
    fn export_golden_fixture_reads_with_standard_parquet_arrow_path() {
        let batches = read_batches(fixture_path());

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
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
                "resolution_method",
                "refinement_status",
                "upstream_unit_ids",
                "adapter_version",
            ]
        );
        assert!(
            binary_column(&batches[0], 2)
                .iter()
                .all(|value| value.is_some_and(|wkb| !wkb.is_empty()))
        );
    }

    #[test]
    fn export_golden_footer_geo_metadata_matches_expected_values() {
        let footer = footer_geo_json(fixture_path());
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
    fn export_golden_hilbert_order_is_stable() {
        let batches = read_batches(fixture_path());
        let ids = string_column(&batches[0], 0);
        let actual = (0..ids.len())
            .map(|index| ids.value(index))
            .collect::<Vec<_>>();

        assert_eq!(actual, vec!["basin-center", "basin-west", "basin-east"]);
    }

    #[test]
    fn export_golden_bbox_values_cover_true_geometry_bounds() {
        let batches = read_batches(fixture_path());
        let ids = string_column(&batches[0], 0);
        let minx = f32_column(&batches[0], 6);
        let miny = f32_column(&batches[0], 7);
        let maxx = f32_column(&batches[0], 8);
        let maxy = f32_column(&batches[0], 9);

        for index in 0..ids.len() {
            let expected = expected_bounds(ids.value(index));
            assert!(f64::from(minx.value(index)) <= expected.0);
            assert!(f64::from(miny.value(index)) <= expected.1);
            assert!(f64::from(maxx.value(index)) >= expected.2);
            assert!(f64::from(maxy.value(index)) >= expected.3);
        }
    }

    #[test]
    fn export_golden_fixture_is_isolated_from_existing_fixture_paths() {
        let path = fixture_path();

        assert!(path.exists());
        assert!(path.ends_with("tests/fixtures/export/basin-geoparquet-golden.parquet"));
        assert!(!path.to_string_lossy().contains("tests/fixtures/parity"));
    }

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_PATH)
    }

    fn read_batches(path: PathBuf) -> Vec<arrow::record_batch::RecordBatch> {
        let file = File::open(path).expect("open parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
        builder
            .build()
            .expect("record batch reader")
            .collect::<Result<Vec<_>, _>>()
            .expect("record batches")
    }

    fn footer_geo_json(path: PathBuf) -> Value {
        let file = File::open(path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("serialized parquet reader");
        let geo = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|entries| entries.iter().find(|entry| entry.key == "geo"))
            .and_then(|entry| entry.value.as_deref())
            .expect("geo footer key_value_metadata should exist");
        serde_json::from_str(geo).expect("geo footer should parse")
    }

    fn string_column(batch: &arrow::record_batch::RecordBatch, index: usize) -> &StringArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column")
    }

    fn binary_column(batch: &arrow::record_batch::RecordBatch, index: usize) -> &BinaryArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary column")
    }

    fn f32_column(batch: &arrow::record_batch::RecordBatch, index: usize) -> &Float32Array {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("f32 column")
    }

    fn expected_bounds(basin_id: &str) -> (f64, f64, f64, f64) {
        match basin_id {
            "basin-west" => (-100.0, 10.0, -99.0, 11.0),
            "basin-center" => (-1.0, -1.0, 0.0, 0.0),
            "basin-east" => (20.0, 10.0, 21.0, 11.0),
            other => panic!("unexpected basin id {other}"),
        }
    }
}
