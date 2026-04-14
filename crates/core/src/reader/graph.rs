//! Graph reader — loads graph.arrow into a DrainageGraph.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, LargeListArray, ListArray};
use arrow::datatypes::{DataType, Field};
use arrow::ipc::reader::FileReader;
use hfx_core::{AdjacencyRow, AtomId, DrainageGraph};
use tracing::{debug, info, instrument};

use crate::error::SessionError;

/// Load `graph.arrow` from `path` and return a [`DrainageGraph`].
///
/// # Errors
///
/// | Condition | Error variant |
/// |-----------|---------------|
/// | File cannot be opened | [`SessionError::Io`] |
/// | File is not valid Arrow IPC | [`SessionError::GraphArrowParse`] |
/// | Schema missing or wrong column type | [`SessionError::GraphSchema`] |
/// | A row contains an invalid atom ID (zero or negative) | [`SessionError::InvalidRow`] |
/// | Graph domain validation fails (empty, duplicate IDs) | [`SessionError::GraphDomain`] |
#[instrument(skip_all, fields(path = %path.display()))]
pub fn load_graph(path: &Path) -> Result<DrainageGraph, SessionError> {
    let file = std::fs::File::open(path).map_err(|e| SessionError::io("graph.arrow", e))?;

    let reader =
        FileReader::try_new(file, None).map_err(|e| SessionError::GraphArrowParse { source: e })?;

    validate_schema(&reader)?;

    debug!("graph.arrow schema validated, reading record batches");

    let rows = read_rows(reader)?;

    let row_count = rows.len();
    let graph = DrainageGraph::new(rows).map_err(|e| SessionError::GraphDomain { source: e })?;

    info!(row_count, "graph.arrow loaded");
    Ok(graph)
}

/// Validate that the Arrow IPC schema contains the expected columns.
fn validate_schema(reader: &FileReader<std::fs::File>) -> Result<(), SessionError> {
    let schema = reader.schema();

    // Check "id" column: must be Int64.
    match schema.field_with_name("id") {
        Err(_) => {
            return Err(SessionError::graph_schema(
                "missing required column \"id\" (expected Int64)",
            ))
        }
        Ok(field) => {
            if field.data_type() != &DataType::Int64 {
                return Err(SessionError::graph_schema(format!(
                    "column \"id\" has type {:?}, expected Int64",
                    field.data_type()
                )));
            }
        }
    }

    // Check "upstream_ids" column: must be List<Int64> or LargeList<Int64>.
    match schema.field_with_name("upstream_ids") {
        Err(_) => {
            return Err(SessionError::graph_schema(
                "missing required column \"upstream_ids\" (expected List(Int64))",
            ))
        }
        Ok(field) => {
            if !is_list_int64(field.data_type()) {
                return Err(SessionError::graph_schema(format!(
                    "column \"upstream_ids\" has type {:?}, expected List(Int64) or LargeList(Int64)",
                    field.data_type()
                )));
            }
        }
    }

    Ok(())
}

/// Return `true` if `dt` is `List<Int64>` or `LargeList<Int64>`.
fn is_list_int64(dt: &DataType) -> bool {
    let expected = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    if dt == &expected {
        return true;
    }
    // Also accept LargeList variant.
    matches!(dt, DataType::LargeList(f) if f.data_type() == &DataType::Int64)
}

/// Read all record batches from the reader and convert each row into an [`AdjacencyRow`].
fn read_rows(
    reader: FileReader<std::fs::File>,
) -> Result<Vec<AdjacencyRow>, SessionError> {
    let mut rows: Vec<AdjacencyRow> = Vec::new();
    let mut global_row: usize = 0;

    for batch_result in reader {
        let batch =
            batch_result.map_err(|e| SessionError::GraphArrowParse { source: e })?;

        let num_rows = batch.num_rows();

        // SAFETY: schema was validated above; columns are guaranteed to exist with correct types.
        let id_col = batch
            .column_by_name("id")
            .expect("id column must exist after schema validation");
        let id_arr = id_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column must be Int64 after schema validation");

        let upstream_col = batch
            .column_by_name("upstream_ids")
            .expect("upstream_ids column must exist after schema validation");

        for i in 0..num_rows {
            let raw_id = id_arr.value(i);
            let atom_id = AtomId::new(raw_id).map_err(|e| {
                SessionError::invalid_row(
                    "graph.arrow",
                    global_row + i,
                    format!("invalid atom id {raw_id}: {e}"),
                )
            })?;

            let upstream = extract_upstream(upstream_col, i, global_row + i)?;

            rows.push(AdjacencyRow::new(atom_id, upstream));
        }

        global_row += num_rows;
    }

    Ok(rows)
}

/// Extract the upstream ID list for row `i` from the upstream_ids column.
///
/// Handles both `ListArray` and `LargeListArray`.
fn extract_upstream(
    col: &dyn arrow::array::Array,
    i: usize,
    row_idx: usize,
) -> Result<Vec<AtomId>, SessionError> {
    if let Some(list_arr) = col.as_any().downcast_ref::<ListArray>() {
        let values = list_arr.value(i);
        let int_arr = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("inner values must be Int64 after schema validation");
        convert_upstream_values(int_arr.values(), row_idx)
    } else if let Some(list_arr) = col.as_any().downcast_ref::<LargeListArray>() {
        let values = list_arr.value(i);
        let int_arr = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("inner values must be Int64 after schema validation");
        convert_upstream_values(int_arr.values(), row_idx)
    } else {
        // Schema validation guarantees this branch is unreachable.
        Err(SessionError::graph_schema(
            "upstream_ids column has unexpected type after schema validation",
        ))
    }
}

/// Convert a slice of raw `i64` upstream IDs into a `Vec<AtomId>`.
fn convert_upstream_values(
    values: &arrow::buffer::ScalarBuffer<i64>,
    row_idx: usize,
) -> Result<Vec<AtomId>, SessionError> {
    values
        .iter()
        .map(|&raw| {
            AtomId::new(raw).map_err(|e| {
                SessionError::invalid_row(
                    "graph.arrow",
                    row_idx,
                    format!("invalid upstream atom id {raw}: {e}"),
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{Int64Array, Int64Builder, ListBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use crate::error::SessionError;

    use super::load_graph;

    // --- Fixture helpers ---

    fn write_graph_fixture(path: &Path, ids: &[i64], upstream_ids: &[Vec<i64>]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "upstream_ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]));

        let id_arr = Int64Array::from(ids.to_vec());
        let mut list_builder = ListBuilder::new(Int64Builder::new());
        for ups in upstream_ids {
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

        let file = std::fs::File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    fn write_schema_only_fixture(path: &Path, schema: Arc<Schema>) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.finish().unwrap();
    }

    // --- Tests ---

    #[test]
    fn test_valid_tree_graph() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        // Atom 1: headwater (no upstream)
        // Atom 2: upstream=[1]
        // Atom 3: upstream=[1, 2]
        write_graph_fixture(&path, &[1, 2, 3], &[vec![], vec![1], vec![1, 2]]);

        let graph = load_graph(&path).expect("valid graph should load");
        assert_eq!(graph.len(), 3);

        let id1 = hfx_core::AtomId::new(1).unwrap();
        let id3 = hfx_core::AtomId::new(3).unwrap();

        let row1 = graph.get(id1).expect("atom 1 should be present");
        assert!(row1.is_headwater());

        let row3 = graph.get(id3).expect("atom 3 should be present");
        assert_eq!(row3.upstream_ids().len(), 2);
    }

    #[test]
    fn test_headwater_detection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        write_graph_fixture(&path, &[1, 2], &[vec![], vec![1]]);

        let graph = load_graph(&path).unwrap();

        let headwater = graph.get(hfx_core::AtomId::new(1).unwrap()).unwrap();
        assert!(headwater.is_headwater());

        let interior = graph.get(hfx_core::AtomId::new(2).unwrap()).unwrap();
        assert!(!interior.is_headwater());
    }

    #[test]
    fn test_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.arrow");

        let err = load_graph(&path).unwrap_err();
        assert!(matches!(err, SessionError::Io { .. }));
    }

    #[test]
    fn test_not_arrow_ipc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.arrow");
        std::fs::write(&path, b"this is not arrow ipc data at all 0xdeadbeef").unwrap();

        let err = load_graph(&path).unwrap_err();
        assert!(matches!(err, SessionError::GraphArrowParse { .. }));
    }

    #[test]
    fn test_missing_id_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        // Only "upstream_ids", no "id".
        let schema = Arc::new(Schema::new(vec![Field::new(
            "upstream_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        )]));
        write_schema_only_fixture(&path, schema);

        let err = load_graph(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::GraphSchema { .. }),
            "expected GraphSchema, got {err:?}"
        );
    }

    #[test]
    fn test_missing_upstream_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        // Only "id", no "upstream_ids".
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        write_schema_only_fixture(&path, schema);

        let err = load_graph(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::GraphSchema { .. }),
            "expected GraphSchema, got {err:?}"
        );
    }

    #[test]
    fn test_zero_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        // id=0 is invalid (terminal sink sentinel).
        write_graph_fixture(&path, &[0], &[vec![]]);

        let err = load_graph(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::InvalidRow { row: 0, .. }),
            "expected InvalidRow at row 0, got {err:?}"
        );
    }

    #[test]
    fn test_negative_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        write_graph_fixture(&path, &[-1], &[vec![]]);

        let err = load_graph(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::InvalidRow { row: 0, .. }),
            "expected InvalidRow at row 0, got {err:?}"
        );
    }

    #[test]
    fn test_empty_graph() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        // Zero rows.
        write_graph_fixture(&path, &[], &[]);

        let err = load_graph(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::GraphDomain { .. }),
            "expected GraphDomain, got {err:?}"
        );
    }

    #[test]
    fn test_duplicate_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        // Two rows with the same id.
        write_graph_fixture(&path, &[1, 1], &[vec![], vec![]]);

        let err = load_graph(&path).unwrap_err();
        assert!(
            matches!(err, SessionError::GraphDomain { .. }),
            "expected GraphDomain, got {err:?}"
        );
    }
}
