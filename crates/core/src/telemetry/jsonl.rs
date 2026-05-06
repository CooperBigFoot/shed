//! Emit closed stage spans as JSONL records.

use std::collections::BTreeMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber, debug};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Environment variable holding the JSONL trace output path.
pub const TRACE_ENV_VAR: &str = "PYSHED_BENCH_TRACE";

/// Keeps the JSONL writer alive and flushes it on drop.
#[derive(Debug)]
pub struct JsonlGuard {
    writer: SharedWriter,
}

impl Drop for JsonlGuard {
    fn drop(&mut self) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writer.flush();
        }
    }
}

/// Emits one JSONL record for each closed `stage` span.
#[derive(Debug, Clone)]
pub struct JsonlLayer {
    writer: SharedWriter,
}

type SharedWriter = Arc<Mutex<BufWriter<File>>>;

#[derive(Debug)]
struct StageSpan {
    started_at: Instant,
    fields: StageFields,
}

#[derive(Debug, Default)]
struct StageFields {
    values: BTreeMap<String, Value>,
}

impl StageFields {
    fn record(&mut self, values: &impl RecordValues) {
        values.record(&mut FieldVisitor {
            fields: &mut self.values,
        });
    }
}

trait RecordValues {
    fn record(&self, visitor: &mut dyn Visit);
}

impl RecordValues for Attributes<'_> {
    fn record(&self, visitor: &mut dyn Visit) {
        Attributes::record(self, visitor);
    }
}

impl RecordValues for Record<'_> {
    fn record(&self, visitor: &mut dyn Visit) {
        Record::record(self, visitor);
    }
}

impl RecordValues for Event<'_> {
    fn record(&self, visitor: &mut dyn Visit) {
        Event::record(self, visitor);
    }
}

struct FieldVisitor<'a> {
    fields: &'a mut BTreeMap<String, Value>,
}

impl FieldVisitor<'_> {
    fn insert(&mut self, field: &Field, value: Value) {
        self.fields.insert(field.name().to_owned(), value);
    }
}

impl Visit for FieldVisitor<'_> {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, json!(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, json!(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, json!(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, json!(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.insert(field, json!(format!("{value:?}")));
    }
}

impl JsonlLayer {
    /// Create a JSONL layer writing to `path`.
    ///
    /// # Errors
    ///
    /// | Condition | Error |
    /// |---|---|
    /// | The file cannot be opened for append/create | Underlying [`io::Error`] |
    pub fn from_path(path: impl AsRef<Path>) -> io::Result<(Self, JsonlGuard)> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let writer = Arc::new(Mutex::new(BufWriter::new(file)));
        Ok((
            Self {
                writer: Arc::clone(&writer),
            },
            JsonlGuard { writer },
        ))
    }

    /// Create a JSONL layer from [`TRACE_ENV_VAR`] when it is set.
    ///
    /// # Errors
    ///
    /// | Condition | Error |
    /// |---|---|
    /// | The environment variable is set but the target file cannot be opened | Underlying [`io::Error`] |
    pub fn from_env() -> io::Result<Option<(Self, JsonlGuard)>> {
        match env::var_os(TRACE_ENV_VAR) {
            Some(path) => Self::from_path(PathBuf::from(path)).map(Some),
            None => Ok(None),
        }
    }

    fn emit(&self, span: StageSpan) {
        let mut record = Map::new();
        record.insert("kind".to_owned(), json!("stage"));
        record.insert("timestamp".to_owned(), json!(unix_timestamp_ms()));
        record.insert(
            "duration_ms".to_owned(),
            json!(span.started_at.elapsed().as_secs_f64() * 1000.0),
        );
        record.insert(
            "thread".to_owned(),
            json!(format!("{:?}", thread::current().id())),
        );

        for key in [
            "stage",
            "bytes",
            "requests",
            "cache_status",
            "path",
            "row_groups",
            "rows",
            "matches",
        ] {
            if let Some(value) = span.fields.values.get(key) {
                record.insert(key.to_owned(), value.clone());
            }
        }

        let line = Value::Object(record);
        let Ok(mut writer) = self.writer.lock() else {
            debug!("bench trace writer lock poisoned");
            return;
        };
        if let Err(error) = serde_json::to_writer(&mut *writer, &line)
            .and_then(|()| writer.write_all(b"\n").map_err(serde_json::Error::io))
        {
            debug!(%error, "failed to write bench trace JSONL record");
        }
    }
}

fn unix_timestamp_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

impl<S> Layer<S> for JsonlLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().name() != "stage" {
            return;
        }

        let Some(span) = ctx.span(id) else {
            return;
        };

        let mut fields = StageFields::default();
        fields.record(attrs);
        span.extensions_mut().insert(StageSpan {
            started_at: Instant::now(),
            fields,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        let Some(stage) = extensions.get_mut::<StageSpan>() else {
            return;
        };
        stage.fields.record(values);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(scope) = ctx.event_scope(event) else {
            return;
        };
        let mut nearest_stage_id = None;
        for span in scope.from_root() {
            if span.extensions().get::<StageSpan>().is_some() {
                nearest_stage_id = Some(span.id());
            }
        }
        let Some(stage_id) = nearest_stage_id else {
            return;
        };
        let Some(span) = ctx.span(&stage_id) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        let Some(stage) = extensions.get_mut::<StageSpan>() else {
            return;
        };
        stage.fields.record(event);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(stage) = span.extensions_mut().remove::<StageSpan>() else {
            return;
        };
        self.emit(stage);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::Value;
    use tempfile::NamedTempFile;
    use tracing_subscriber::prelude::*;

    use crate::telemetry::{
        Stage, StageGuard, record_bytes, record_cache_status, record_matches, record_path,
        record_requests, record_row_groups, record_rows,
    };

    use super::JsonlLayer;

    fn collect_records(path: &std::path::Path) -> Vec<Value> {
        fs::read_to_string(path)
            .expect("trace file should be readable")
            .lines()
            .map(|line| serde_json::from_str(line).expect("trace line should be JSON"))
            .collect()
    }

    #[test]
    fn emits_one_line_on_closed_stage_span() {
        let file = NamedTempFile::new().expect("temp file should be created");
        let (layer, guard) = JsonlLayer::from_path(file.path()).expect("layer should be created");
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let _stage = StageGuard::enter(Stage::OutletResolve);
        });
        drop(guard);

        let records = collect_records(file.path());
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["kind"], "stage");
        assert_eq!(records[0]["stage"], "outlet_resolve");
        assert!(records[0]["timestamp"].as_u64().is_some());
        assert!(records[0]["duration_ms"].as_f64().is_some());
    }

    #[test]
    fn ignores_non_stage_spans() {
        let file = NamedTempFile::new().expect("temp file should be created");
        let (layer, guard) = JsonlLayer::from_path(file.path()).expect("layer should be created");
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("not_stage", stage = "ignored");
            let _entered = span.enter();
        });
        drop(guard);

        let records = collect_records(file.path());
        assert!(records.is_empty());
    }

    #[test]
    fn includes_helper_recorded_fields() {
        let file = NamedTempFile::new().expect("temp file should be created");
        let (layer, guard) = JsonlLayer::from_path(file.path()).expect("layer should be created");
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let _stage = StageGuard::enter(Stage::RasterCacheLookup);
            record_bytes(4096);
            record_requests(3);
            record_cache_status("hit");
            record_path("/tmp/shed/cache.bin");
            record_row_groups(8);
            record_rows(144);
            record_matches(13);
        });
        drop(guard);

        let records = collect_records(file.path());
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["stage"], "raster_cache_lookup");
        assert_eq!(records[0]["bytes"], 4096);
        assert_eq!(records[0]["requests"], 3);
        assert_eq!(records[0]["cache_status"], "hit");
        assert_eq!(records[0]["path"], "/tmp/shed/cache.bin");
        assert_eq!(records[0]["row_groups"], 8);
        assert_eq!(records[0]["rows"], 144);
        assert_eq!(records[0]["matches"], 13);
    }
}
