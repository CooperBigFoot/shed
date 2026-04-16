//! Test utilities for building synthetic HFX dataset fixtures.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Int64Array, Int64Builder, ListBuilder,
    RecordBatch,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde_json::json;
use tempfile::TempDir;

/// Custom catchment specification for outlet resolution tests.
pub struct TestCatchment {
    pub id: i64,
    pub area_km2: f32,
    pub up_area_km2: Option<f32>,
    /// Rectangle polygon as (minx, miny, maxx, maxy).
    pub polygon: (f64, f64, f64, f64),
}

/// Custom snap target specification for outlet resolution tests.
pub struct TestSnapTarget {
    pub id: i64,
    pub catchment_id: i64,
    pub weight: f32,
    pub is_mainstem: bool,
    pub geometry: TestSnapGeometry,
}

/// Geometry for a test snap target.
pub enum TestSnapGeometry {
    /// A WKB Point at (lon, lat).
    Point(f64, f64),
    /// A WKB LineString from (x1, y1) to (x2, y2).
    LineString(f64, f64, f64, f64),
}

/// Builder for synthetic HFX dataset fixtures used in integration tests.
pub struct DatasetBuilder {
    dir: TempDir,
    atom_count: usize,
    topology: &'static str,
    include_snap: bool,
    include_rasters: bool,
    row_group_size: usize,
    dag_diamond: bool,
    custom_catchments: Option<Vec<TestCatchment>>,
    custom_snap_targets: Option<Vec<TestSnapTarget>>,
}

impl DatasetBuilder {
    /// Create a new builder with a linear chain of `atom_count` atoms.
    pub fn new(atom_count: usize) -> Self {
        Self {
            dir: TempDir::new().unwrap(),
            atom_count,
            topology: "tree",
            include_snap: false,
            include_rasters: false,
            row_group_size: 8192,
            dag_diamond: false,
            custom_catchments: None,
            custom_snap_targets: None,
        }
    }

    /// Include a `snap.parquet` artifact in the dataset.
    pub fn with_snap(mut self) -> Self {
        self.include_snap = true;
        self
    }

    /// Include stub raster files (`flow_dir.tif`, `flow_acc.tif`) in the dataset.
    pub fn with_rasters(mut self) -> Self {
        self.include_rasters = true;
        self
    }

    /// Set the Parquet row group size for catchments and snap files.
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    /// Add a diamond bifurcation pattern and mark the topology as DAG.
    pub fn with_dag(mut self) -> Self {
        self.topology = "dag";
        self.dag_diamond = true;
        self
    }

    /// Override auto-generated catchments with custom specifications.
    ///
    /// The graph will be built as a linear chain of the provided IDs.
    /// The `atom_count` is automatically set to the number of custom catchments.
    pub fn with_custom_catchments(mut self, catchments: Vec<TestCatchment>) -> Self {
        self.atom_count = catchments.len();
        self.custom_catchments = Some(catchments);
        self
    }

    /// Override auto-generated snap targets with custom specifications.
    ///
    /// Automatically enables the snap artifact.
    pub fn with_custom_snap_targets(mut self, targets: Vec<TestSnapTarget>) -> Self {
        self.include_snap = true;
        self.custom_snap_targets = Some(targets);
        self
    }

    /// Write all artifacts and return `(TempDir, path_to_dataset_root)`.
    ///
    /// The [`TempDir`] must be kept alive by the caller to prevent cleanup.
    pub fn build(self) -> (TempDir, PathBuf) {
        let root = self.dir.path().to_path_buf();
        self.write_manifest(&root);
        self.write_graph(&root);
        self.write_catchments(&root);
        if self.include_snap {
            self.write_snap(&root);
        }
        if self.include_rasters {
            self.write_raster_stubs(&root);
        }
        (self.dir, root)
    }

    // -----------------------------------------------------------------------
    // Artifact writers
    // -----------------------------------------------------------------------

    fn write_manifest(&self, root: &Path) {
        let atom_count = if self.dag_diamond {
            self.atom_count + 4
        } else {
            self.atom_count
        };
        let mut manifest = json!({
            "format_version": "0.1",
            "fabric_name": "testfabric",
            "crs": "EPSG:4326",
            "topology": self.topology,
            "terminal_sink_id": 0,
            "bbox": [-180.0, -90.0, 180.0, 90.0],
            "atom_count": atom_count,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "test-v1"
        });
        if self.include_snap {
            manifest["has_snap"] = json!(true);
        }
        if self.include_rasters {
            manifest["has_rasters"] = json!(true);
            manifest["flow_dir_encoding"] = json!("esri");
        }
        std::fs::write(root.join("manifest.json"), manifest.to_string()).unwrap();
    }

    fn write_graph(&self, root: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "upstream_ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]));

        let (ids, upstream_ids) = self.build_graph_data();

        let id_arr = Int64Array::from(ids);
        let mut list_builder = ListBuilder::new(Int64Builder::new());
        for ups in &upstream_ids {
            for &u in ups {
                list_builder.values().append_value(u);
            }
            list_builder.append(true);
        }
        let upstream_arr = list_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_arr), Arc::new(upstream_arr)],
        )
        .unwrap();

        let file = std::fs::File::create(root.join("graph.arrow")).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    fn write_catchments(&self, root: &Path) {
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
            .set_max_row_group_size(self.row_group_size)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let file = std::fs::File::create(root.join("catchments.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        let mut id_b = Int64Builder::new();
        let mut area_b = Float32Builder::new();
        let mut up_area_b = Float32Builder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();

        if let Some(customs) = &self.custom_catchments {
            for c in customs {
                let (poly_minx, poly_miny, poly_maxx, poly_maxy) = c.polygon;
                id_b.append_value(c.id);
                area_b.append_value(c.area_km2);
                match c.up_area_km2 {
                    Some(v) => up_area_b.append_value(v),
                    None => up_area_b.append_null(),
                }
                minx_b.append_value(poly_minx as f32);
                miny_b.append_value(poly_miny as f32);
                maxx_b.append_value(poly_maxx as f32);
                maxy_b.append_value(poly_maxy as f32);
                let wkb = minimal_wkb_polygon(poly_minx, poly_miny, poly_maxx, poly_maxy);
                geom_b.append_value(&wkb);
            }
        } else {
            let (ids, _) = self.build_graph_data();
            for (idx, &id) in ids.iter().enumerate() {
                let i = idx + 1; // 1-based for bbox spacing
                let minx = (i as f32) * 0.5;
                let miny = 0.0f32;
                let maxx = (i as f32) * 0.5 + 0.4;
                let maxy = 0.4f32;

                id_b.append_value(id);
                area_b.append_value(10.0f32);
                up_area_b.append_null();
                minx_b.append_value(minx);
                miny_b.append_value(miny);
                maxx_b.append_value(maxx);
                maxy_b.append_value(maxy);

                let wkb = minimal_wkb_polygon(minx as f64, miny as f64, maxx as f64, maxy as f64);
                geom_b.append_value(&wkb);
            }
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

    fn write_snap(&self, root: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("catchment_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("is_mainstem", DataType::Boolean, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let props = WriterProperties::builder()
            .set_max_row_group_size(self.row_group_size)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let file = std::fs::File::create(root.join("snap.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        let mut id_b = Int64Builder::new();
        let mut catchment_id_b = Int64Builder::new();
        let mut weight_b = Float32Builder::new();
        let mut is_mainstem_b = BooleanBuilder::new();
        let mut minx_b = Float32Builder::new();
        let mut miny_b = Float32Builder::new();
        let mut maxx_b = Float32Builder::new();
        let mut maxy_b = Float32Builder::new();
        let mut geom_b = BinaryBuilder::new();

        if let Some(customs) = &self.custom_snap_targets {
            for t in customs {
                id_b.append_value(t.id);
                catchment_id_b.append_value(t.catchment_id);
                weight_b.append_value(t.weight);
                is_mainstem_b.append_value(t.is_mainstem);
                match &t.geometry {
                    TestSnapGeometry::Point(x, y) => {
                        // Point bbox needs non-zero extent.
                        let eps: f32 = 1e-6;
                        minx_b.append_value(*x as f32 - eps);
                        miny_b.append_value(*y as f32 - eps);
                        maxx_b.append_value(*x as f32 + eps);
                        maxy_b.append_value(*y as f32 + eps);
                        let wkb = minimal_wkb_point(*x, *y);
                        geom_b.append_value(&wkb);
                    }
                    TestSnapGeometry::LineString(x1, y1, x2, y2) => {
                        minx_b.append_value(x1.min(*x2) as f32);
                        miny_b.append_value(y1.min(*y2) as f32);
                        maxx_b.append_value(x1.max(*x2) as f32);
                        maxy_b.append_value(y1.max(*y2) as f32);
                        let wkb = minimal_wkb_linestring(*x1, *y1, *x2, *y2);
                        geom_b.append_value(&wkb);
                    }
                }
            }
        } else {
            let (ids, _) = self.build_graph_data();
            for (idx, &atom_id) in ids.iter().enumerate() {
                let i = idx + 1;
                let minx = (i as f32) * 0.5;
                let miny = 0.0f32;
                let maxx = (i as f32) * 0.5 + 0.4;
                let maxy = 0.4f32;

                // Center of the bbox for the linestring
                let cx = ((minx + maxx) / 2.0) as f64;
                let cy = ((miny + maxy) / 2.0) as f64;

                id_b.append_value(atom_id);
                catchment_id_b.append_value(atom_id);
                weight_b.append_value(100.0f32);
                is_mainstem_b.append_value(true);
                minx_b.append_value(minx);
                miny_b.append_value(miny);
                maxx_b.append_value(maxx);
                maxy_b.append_value(maxy);

                let wkb = minimal_wkb_linestring(cx - 0.1, cy, cx + 0.1, cy);
                geom_b.append_value(&wkb);
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_b.finish()),
                Arc::new(catchment_id_b.finish()),
                Arc::new(weight_b.finish()),
                Arc::new(is_mainstem_b.finish()),
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

    fn write_raster_stubs(&self, root: &Path) {
        std::fs::write(root.join("flow_dir.tif"), b"stub").unwrap();
        std::fs::write(root.join("flow_acc.tif"), b"stub").unwrap();
    }

    // -----------------------------------------------------------------------
    // Internal data generation
    // -----------------------------------------------------------------------

    /// Build the (ids, upstream_ids) vectors for the graph.
    ///
    /// Linear chain: atom 1 is headwater, atom i has upstream=[i-1].
    /// DAG mode appends four extra atoms forming a diamond on top of the chain.
    fn build_graph_data(&self) -> (Vec<i64>, Vec<Vec<i64>>) {
        let n = self.atom_count;
        let mut ids: Vec<i64>;
        let mut upstream: Vec<Vec<i64>>;

        if let Some(customs) = &self.custom_catchments {
            // Build a linear chain from custom IDs: first is headwater.
            ids = customs.iter().map(|c| c.id).collect();
            upstream = Vec::with_capacity(ids.len());
            for (idx, _) in ids.iter().enumerate() {
                if idx == 0 {
                    upstream.push(vec![]);
                } else {
                    upstream.push(vec![ids[idx - 1]]);
                }
            }
        } else {
            ids = (1..=(n as i64)).collect();
            upstream = Vec::with_capacity(n);

            // Atom 1 is a headwater; atom i has upstream = [i-1].
            for i in 1..=(n as i64) {
                if i == 1 {
                    upstream.push(vec![]);
                } else {
                    upstream.push(vec![i - 1]);
                }
            }
        }

        if self.dag_diamond {
            // Diamond: N+1 and N+2 are headwaters; N+3 merges both;
            // N+4 is downstream of N+3 and N (existing chain outlet).
            let base = n as i64;
            let hw1 = base + 1;
            let hw2 = base + 2;
            let merge = base + 3;
            let outlet = base + 4;

            ids.push(hw1);
            upstream.push(vec![]);

            ids.push(hw2);
            upstream.push(vec![]);

            ids.push(merge);
            upstream.push(vec![hw1, hw2]);

            ids.push(outlet);
            upstream.push(vec![base, merge]);
        }

        (ids, upstream)
    }
}

// ---------------------------------------------------------------------------
// WKB helpers
// ---------------------------------------------------------------------------

fn minimal_wkb_point(x: f64, y: f64) -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1u8); // little-endian
    wkb.extend_from_slice(&1u32.to_le_bytes()); // wkbPoint = 1
    wkb.extend_from_slice(&x.to_le_bytes());
    wkb.extend_from_slice(&y.to_le_bytes());
    wkb
}

fn minimal_wkb_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1u8); // little-endian
    wkb.extend_from_slice(&3u32.to_le_bytes()); // polygon type
    wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
    wkb.extend_from_slice(&5u32.to_le_bytes()); // 5 points (closed)
    for (x, y) in [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ] {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}

fn minimal_wkb_linestring(x1: f64, y1: f64, x2: f64, y2: f64) -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1u8); // little-endian
    wkb.extend_from_slice(&2u32.to_le_bytes()); // linestring type
    wkb.extend_from_slice(&2u32.to_le_bytes()); // 2 points
    for (x, y) in [(x1, y1), (x2, y2)] {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::DatasetSession;

    #[test]
    fn test_minimal_dataset_opens() {
        let (_dir, root) = DatasetBuilder::new(3).build();
        let session = DatasetSession::open(&root).expect("minimal dataset should open");
        assert_eq!(session.manifest().atom_count().get(), 3);
        assert!(session.snap().is_none());
        assert!(session.raster_paths().is_none());
    }

    #[test]
    fn test_dataset_with_snap_opens() {
        let (_dir, root) = DatasetBuilder::new(5).with_snap().build();
        let session = DatasetSession::open(&root).expect("dataset with snap should open");
        assert!(session.snap().is_some());
    }

    #[test]
    fn test_dataset_with_rasters_opens() {
        let (_dir, root) = DatasetBuilder::new(4).with_rasters().build();
        let session = DatasetSession::open(&root).expect("dataset with rasters should open");
        assert!(session.raster_paths().is_some());
        let rp = session.raster_paths().unwrap();
        assert!(rp.flow_dir().exists());
        assert!(rp.flow_acc().exists());
    }

    #[test]
    fn test_dataset_with_small_row_groups() {
        let (_dir, root) = DatasetBuilder::new(10).with_row_group_size(3).build();
        let session = DatasetSession::open(&root).expect("small row group dataset should open");
        assert_eq!(session.manifest().atom_count().get(), 10);
    }

    #[test]
    fn test_dag_dataset_opens() {
        let (_dir, root) = DatasetBuilder::new(3).with_dag().build();
        let session = DatasetSession::open(&root).expect("dag dataset should open");
        // DAG mode adds 4 extra atoms
        assert_eq!(session.manifest().atom_count().get(), 7);
        assert_eq!(session.topology(), hfx_core::Topology::Dag);
    }

    #[test]
    fn test_graph_has_correct_row_count() {
        let (_dir, root) = DatasetBuilder::new(5).build();
        let session = DatasetSession::open(&root).unwrap();
        assert_eq!(session.graph().len(), 5);
    }

    #[test]
    fn test_catchments_have_correct_count() {
        let (_dir, root) = DatasetBuilder::new(4).build();
        let session = DatasetSession::open(&root).unwrap();
        assert_eq!(session.catchments().total_rows(), 4);
    }

    #[test]
    fn test_full_dataset_opens() {
        let (_dir, root) = DatasetBuilder::new(6)
            .with_snap()
            .with_rasters()
            .with_row_group_size(2)
            .build();
        let session = DatasetSession::open(&root).expect("full dataset should open");
        assert_eq!(session.manifest().atom_count().get(), 6);
        assert!(session.snap().is_some());
        assert!(session.raster_paths().is_some());
    }

    #[test]
    fn test_custom_catchments_dataset_opens() {
        let catchments = vec![
            TestCatchment {
                id: 10,
                area_km2: 5.0,
                up_area_km2: Some(100.0),
                polygon: (1.0, 0.0, 1.4, 0.4),
            },
            TestCatchment {
                id: 20,
                area_km2: 8.0,
                up_area_km2: None,
                polygon: (1.5, 0.0, 1.9, 0.4),
            },
        ];
        let (_dir, root) = DatasetBuilder::new(2)
            .with_custom_catchments(catchments)
            .build();
        let session = DatasetSession::open(&root).expect("custom catchments dataset should open");
        assert_eq!(session.manifest().atom_count().get(), 2);
    }

    #[test]
    fn test_custom_snap_targets_dataset_opens() {
        let catchments = vec![
            TestCatchment {
                id: 1,
                area_km2: 10.0,
                up_area_km2: None,
                polygon: (0.5, 0.0, 0.9, 0.4),
            },
            TestCatchment {
                id: 2,
                area_km2: 10.0,
                up_area_km2: None,
                polygon: (1.0, 0.0, 1.4, 0.4),
            },
        ];
        let targets = vec![
            TestSnapTarget {
                id: 1,
                catchment_id: 1,
                weight: 50.0,
                is_mainstem: true,
                geometry: TestSnapGeometry::Point(0.7, 0.2),
            },
            TestSnapTarget {
                id: 2,
                catchment_id: 2,
                weight: 100.0,
                is_mainstem: false,
                geometry: TestSnapGeometry::LineString(1.1, 0.2, 1.3, 0.2),
            },
        ];
        let (_dir, root) = DatasetBuilder::new(2)
            .with_custom_catchments(catchments)
            .with_custom_snap_targets(targets)
            .build();
        let session = DatasetSession::open(&root).expect("custom snap targets dataset should open");
        assert!(session.snap().is_some());
    }
}
