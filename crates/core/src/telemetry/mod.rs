//! Emit stable stage telemetry for shed delineation work.
//!
//! JSONL schema target for later layers:
//! - Each record is an event object with a stable record kind such as
//!   `stage_duration` or `stage_field`.
//! - `stage` is always one of [`Stage`]'s lower_snake_case names.
//! - `duration_ms` is emitted when a stage guard drops.
//! - `bytes`, `requests`, `cache_status`, `path`, `row_groups`, `rows`, and
//!   `matches` are optional span fields.
//! - Timestamp and thread fields may be added by the tracing layer.
//! - These field names and stage names are stable for downstream parsers.

pub mod jsonl;

use std::fmt;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::field;
use tracing::span::EnteredSpan;
use tracing::{Level, Span};

/// Identifies a stable telemetry stage in the delineation pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// Opening a remote dataset or object-store root.
    RemoteOpen,
    /// Fetching or reading the HFX manifest.
    ManifestFetch,
    /// Fetching or reading the catchment graph.
    GraphFetch,
    /// Opening the catchment store.
    CatchmentStoreOpen,
    /// Building or loading the catchment identifier index.
    CatchmentIdIndex,
    /// Opening the snap store.
    SnapStoreOpen,
    /// Building or loading the snap identifier index.
    SnapIdIndex,
    /// Validating graph nodes against catchment records.
    ValidateGraphCatchments,
    /// Validating snap references against catchment records.
    ValidateSnapRefs,
    /// Resolving the requested outlet.
    OutletResolve,
    /// Traversing upstream catchments.
    UpstreamTraversal,
    /// Refining the terminal outlet.
    TerminalRefine,
    /// Fetching the terminal catchment geometry.
    TerminalCatchmentFetch,
    /// Localizing the flow-direction raster.
    RasterLocalizeFlowDir,
    /// Localizing the flow-accumulation raster.
    RasterLocalizeFlowAcc,
    /// Preparing a COG read window.
    CogPrepareWindow,
    /// Fetching COG tiles.
    CogFetchTiles,
    /// Looking up raster data in cache.
    RasterCacheLookup,
    /// Assembling watershed geometry.
    WatershedAssembly,
    /// Composing the final result.
    ResultCompose,
}

impl Stage {
    /// Return the stable lower_snake_case stage name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RemoteOpen => "remote_open",
            Self::ManifestFetch => "manifest_fetch",
            Self::GraphFetch => "graph_fetch",
            Self::CatchmentStoreOpen => "catchment_store_open",
            Self::CatchmentIdIndex => "catchment_id_index",
            Self::SnapStoreOpen => "snap_store_open",
            Self::SnapIdIndex => "snap_id_index",
            Self::ValidateGraphCatchments => "validate_graph_catchments",
            Self::ValidateSnapRefs => "validate_snap_refs",
            Self::OutletResolve => "outlet_resolve",
            Self::UpstreamTraversal => "upstream_traversal",
            Self::TerminalRefine => "terminal_refine",
            Self::TerminalCatchmentFetch => "terminal_catchment_fetch",
            Self::RasterLocalizeFlowDir => "raster_localize_flow_dir",
            Self::RasterLocalizeFlowAcc => "raster_localize_flow_acc",
            Self::CogPrepareWindow => "cog_prepare_window",
            Self::CogFetchTiles => "cog_fetch_tiles",
            Self::RasterCacheLookup => "raster_cache_lookup",
            Self::WatershedAssembly => "watershed_assembly",
            Self::ResultCompose => "result_compose",
        }
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Create a tracing span for a telemetry stage.
pub fn stage_span(stage: Stage) -> Span {
    tracing::span!(
        Level::INFO,
        "stage",
        stage = stage.as_str(),
        duration_ms = field::Empty,
        bytes = field::Empty,
        requests = field::Empty,
        cache_status = field::Empty,
        path = field::Empty,
        row_groups = field::Empty,
        rows = field::Empty,
        matches = field::Empty,
    )
}

/// Enters a stage span and records `duration_ms` when dropped.
#[derive(Debug)]
pub struct StageGuard {
    span: Span,
    _entered: EnteredSpan,
    started_at: Instant,
}

impl StageGuard {
    /// Enter a telemetry stage span until the guard is dropped.
    pub fn enter(stage: Stage) -> Self {
        let span = stage_span(stage);
        let entered = span.clone().entered();
        Self {
            span,
            _entered: entered,
            started_at: Instant::now(),
        }
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        self.span
            .record("duration_ms", duration_ms(self.started_at.elapsed()));
    }
}

/// Record byte count on the current span.
pub fn record_bytes(bytes: u64) {
    Span::current().record("bytes", bytes);
}

/// Record request count on the current span.
pub fn record_requests(requests: u64) {
    Span::current().record("requests", requests);
}

/// Record cache status on the current span.
pub fn record_cache_status(status: impl fmt::Display) {
    Span::current().record("cache_status", field::display(status));
}

/// Record path on the current span.
pub fn record_path(path: impl AsRef<Path>) {
    Span::current().record("path", field::display(path.as_ref().display()));
}

/// Record selected row-group count on the current span.
pub fn record_row_groups(row_groups: u64) {
    Span::current().record("row_groups", row_groups);
    tracing::event!(Level::INFO, row_groups);
}

/// Record row count on the current span.
pub fn record_rows(rows: u64) {
    Span::current().record("rows", rows);
    tracing::event!(Level::INFO, rows);
}

/// Record matched row count on the current span.
pub fn record_matches(matches: u64) {
    Span::current().record("matches", matches);
    tracing::event!(Level::INFO, matches);
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};
    use tracing_core::span::Current;

    use crate::telemetry::{
        Stage, StageGuard, record_bytes, record_cache_status, record_matches, record_path,
        record_requests, record_row_groups, record_rows, stage_span,
    };

    #[derive(Debug)]
    struct RecordedSpan {
        name: String,
        metadata: &'static Metadata<'static>,
        fields: HashMap<String, String>,
    }

    #[derive(Debug, Default)]
    struct RecordingState {
        next_id: u64,
        spans: HashMap<u64, RecordedSpan>,
        stack: Vec<u64>,
    }

    #[derive(Debug, Clone, Default)]
    struct RecordingSubscriber {
        state: Arc<Mutex<RecordingState>>,
    }

    struct FieldVisitor<'a> {
        fields: &'a mut HashMap<String, String>,
    }

    impl Visit for FieldVisitor<'_> {
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields
                .insert(field.name().to_owned(), value.to_string());
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields
                .insert(field.name().to_owned(), value.to_owned());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.fields
                .insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    impl Subscriber for RecordingSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, span: &Attributes<'_>) -> Id {
            let mut state = self.state.lock().expect("recording state should lock");
            state.next_id += 1;
            let id = state.next_id;
            let mut recorded = RecordedSpan {
                name: span.metadata().name().to_owned(),
                metadata: span.metadata(),
                fields: HashMap::new(),
            };
            span.record(&mut FieldVisitor {
                fields: &mut recorded.fields,
            });
            state.spans.insert(id, recorded);
            Id::from_u64(id)
        }

        fn record(&self, span: &Id, values: &Record<'_>) {
            let mut state = self.state.lock().expect("recording state should lock");
            let recorded = state
                .spans
                .get_mut(&span.into_u64())
                .expect("span should have been created before recording");
            values.record(&mut FieldVisitor {
                fields: &mut recorded.fields,
            });
        }

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {}

        fn enter(&self, span: &Id) {
            let mut state = self.state.lock().expect("recording state should lock");
            state.stack.push(span.into_u64());
        }

        fn exit(&self, span: &Id) {
            let mut state = self.state.lock().expect("recording state should lock");
            let popped = state.stack.pop();
            assert_eq!(popped, Some(span.into_u64()));
        }

        fn current_span(&self) -> Current {
            let state = self.state.lock().expect("recording state should lock");
            state
                .stack
                .last()
                .and_then(|id| {
                    state
                        .spans
                        .get(id)
                        .map(|span| Current::new(Id::from_u64(*id), span.metadata))
                })
                .unwrap_or_else(Current::none)
        }
    }

    fn recorded_stage_span(state: &Arc<Mutex<RecordingState>>) -> RecordedSpan {
        let mut state = state.lock().expect("recording state should lock");
        state
            .spans
            .drain()
            .map(|(_, span)| span)
            .find(|span| span.name == "stage")
            .expect("stage span should be recorded")
    }

    #[test]
    fn stage_string_taxonomy_is_stable() {
        let taxonomy = [
            (Stage::RemoteOpen, "remote_open"),
            (Stage::ManifestFetch, "manifest_fetch"),
            (Stage::GraphFetch, "graph_fetch"),
            (Stage::CatchmentStoreOpen, "catchment_store_open"),
            (Stage::CatchmentIdIndex, "catchment_id_index"),
            (Stage::SnapStoreOpen, "snap_store_open"),
            (Stage::SnapIdIndex, "snap_id_index"),
            (Stage::ValidateGraphCatchments, "validate_graph_catchments"),
            (Stage::ValidateSnapRefs, "validate_snap_refs"),
            (Stage::OutletResolve, "outlet_resolve"),
            (Stage::UpstreamTraversal, "upstream_traversal"),
            (Stage::TerminalRefine, "terminal_refine"),
            (Stage::TerminalCatchmentFetch, "terminal_catchment_fetch"),
            (Stage::RasterLocalizeFlowDir, "raster_localize_flow_dir"),
            (Stage::RasterLocalizeFlowAcc, "raster_localize_flow_acc"),
            (Stage::CogPrepareWindow, "cog_prepare_window"),
            (Stage::CogFetchTiles, "cog_fetch_tiles"),
            (Stage::RasterCacheLookup, "raster_cache_lookup"),
            (Stage::WatershedAssembly, "watershed_assembly"),
            (Stage::ResultCompose, "result_compose"),
        ];

        assert_eq!(taxonomy.len(), 20);
        for (stage, expected) in taxonomy {
            assert_eq!(stage.as_str(), expected);
            assert_eq!(stage.to_string(), expected);
        }
    }

    #[test]
    fn stage_serde_uses_stable_taxonomy() {
        let taxonomy = [
            (Stage::RemoteOpen, "remote_open"),
            (Stage::ManifestFetch, "manifest_fetch"),
            (Stage::GraphFetch, "graph_fetch"),
            (Stage::CatchmentStoreOpen, "catchment_store_open"),
            (Stage::CatchmentIdIndex, "catchment_id_index"),
            (Stage::SnapStoreOpen, "snap_store_open"),
            (Stage::SnapIdIndex, "snap_id_index"),
            (Stage::ValidateGraphCatchments, "validate_graph_catchments"),
            (Stage::ValidateSnapRefs, "validate_snap_refs"),
            (Stage::OutletResolve, "outlet_resolve"),
            (Stage::UpstreamTraversal, "upstream_traversal"),
            (Stage::TerminalRefine, "terminal_refine"),
            (Stage::TerminalCatchmentFetch, "terminal_catchment_fetch"),
            (Stage::RasterLocalizeFlowDir, "raster_localize_flow_dir"),
            (Stage::RasterLocalizeFlowAcc, "raster_localize_flow_acc"),
            (Stage::CogPrepareWindow, "cog_prepare_window"),
            (Stage::CogFetchTiles, "cog_fetch_tiles"),
            (Stage::RasterCacheLookup, "raster_cache_lookup"),
            (Stage::WatershedAssembly, "watershed_assembly"),
            (Stage::ResultCompose, "result_compose"),
        ];

        for (stage, expected) in taxonomy {
            let serialized = serde_json::to_string(&stage).expect("stage should serialize to JSON");
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: Stage = serde_json::from_str(&serialized)
                .expect("stable stage name should deserialize from JSON");
            assert_eq!(deserialized, stage);
        }

        assert!(
            serde_json::from_str::<Stage>("\"RemoteOpen\"").is_err(),
            "default serde variant name must not deserialize"
        );
    }

    #[test]
    fn stage_guard_records_duration_ms_on_drop() {
        let subscriber = RecordingSubscriber::default();
        let state = subscriber.state.clone();
        let dispatch = tracing::Dispatch::new(subscriber);

        tracing::dispatcher::with_default(&dispatch, || {
            let _guard = StageGuard::enter(Stage::OutletResolve);
            thread::sleep(Duration::from_millis(2));
        });

        let span = recorded_stage_span(&state);
        assert_eq!(span.fields.get("stage"), Some(&"outlet_resolve".to_owned()));
        let duration = span
            .fields
            .get("duration_ms")
            .expect("duration_ms should be recorded")
            .parse::<u64>()
            .expect("duration_ms should parse as u64");
        assert!(duration >= 1, "duration_ms should be at least 1");
    }

    #[test]
    fn helper_field_recording_updates_current_span() {
        let subscriber = RecordingSubscriber::default();
        let state = subscriber.state.clone();
        let dispatch = tracing::Dispatch::new(subscriber);

        tracing::dispatcher::with_default(&dispatch, || {
            let span = stage_span(Stage::RasterCacheLookup);
            let _entered = span.enter();
            record_bytes(4096);
            record_requests(3);
            record_cache_status("hit");
            record_path("/tmp/shed/cache.bin");
            record_row_groups(8);
            record_rows(144);
            record_matches(13);
        });

        let span = recorded_stage_span(&state);
        assert_eq!(
            span.fields.get("stage"),
            Some(&"raster_cache_lookup".to_owned())
        );
        assert_eq!(span.fields.get("bytes"), Some(&"4096".to_owned()));
        assert_eq!(span.fields.get("requests"), Some(&"3".to_owned()));
        assert_eq!(span.fields.get("cache_status"), Some(&"hit".to_owned()));
        assert_eq!(
            span.fields.get("path"),
            Some(&"/tmp/shed/cache.bin".to_owned())
        );
        assert_eq!(span.fields.get("row_groups"), Some(&"8".to_owned()));
        assert_eq!(span.fields.get("rows"), Some(&"144".to_owned()));
        assert_eq!(span.fields.get("matches"), Some(&"13".to_owned()));
    }
}
