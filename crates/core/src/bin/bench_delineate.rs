//! Diagnostic delineation benchmark harness.

use std::env;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use shed_core::algo::GeoCoord;
use shed_core::session::DatasetSession;
use shed_core::source_telemetry::HttpStatsSnapshot;
use shed_core::{DelineationOptions, Engine, ResolverConfig, SearchRadiusMetres};
use tracing_subscriber::prelude::*;

const R2_DATASET: &str = "https://basin-delineations-public.upstream.tech/grit/1.0.0/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Cold,
    Warm,
    Hot,
}

#[derive(Debug)]
struct Config {
    mode: Mode,
    dataset: String,
    outlet: GeoCoord,
    outlet_label: String,
    search_radius: SearchRadiusMetres,
    iterations: usize,
    out: PathBuf,
    cache_root: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct IterationSummary {
    iteration: usize,
    wall_time_ms: f64,
    http_stats: Option<Value>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args()?;
    run(config)
}

fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let dataset = resolve_dataset(&config.dataset)?;
    let cache_parent = config
        .cache_root
        .clone()
        .unwrap_or_else(|| env::temp_dir().join("shed-bench-cache"));
    fs::create_dir_all(&cache_parent)?;
    let cache_dir = unique_child_dir(&cache_parent, "run")?;

    let out_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&config.out)?;
    let mut out = BufWriter::new(out_file);
    write_jsonl(&mut out, header_record(&config, &dataset, &cache_dir))?;

    let mut summaries = match config.mode {
        Mode::Cold => run_cold(&config, &dataset, &cache_dir, &mut out)?,
        Mode::Warm => run_warm(&config, &dataset, &cache_dir, &mut out)?,
        Mode::Hot => run_hot(&config, &dataset, &cache_dir, &mut out)?,
    };
    summaries.sort_by_key(|summary| summary.iteration);

    for summary in &summaries {
        write_jsonl(&mut out, iteration_record(summary))?;
    }
    write_jsonl(&mut out, final_record(&summaries))?;
    out.flush()?;

    Ok(())
}

fn run_cold(
    config: &Config,
    dataset: &str,
    cache_dir: &Path,
    out: &mut impl Write,
) -> Result<Vec<IterationSummary>, Box<dyn Error>> {
    let mut summaries = Vec::with_capacity(config.iterations);
    for iteration in 0..config.iterations {
        let iter_cache = unique_child_dir(cache_dir, &format!("cold-{iteration}"))?;
        let summary = measure_fresh_engine(config, dataset, &iter_cache, iteration, out)?;
        summaries.push(summary);
    }
    Ok(summaries)
}

fn run_warm(
    config: &Config,
    dataset: &str,
    cache_dir: &Path,
    out: &mut impl Write,
) -> Result<Vec<IterationSummary>, Box<dyn Error>> {
    let warm_cache = unique_child_dir(cache_dir, "warm")?;
    let _ = run_once(
        dataset,
        config.outlet,
        config.search_radius,
        &warm_cache,
        None,
    )?;

    let mut summaries = Vec::with_capacity(config.iterations);
    for iteration in 0..config.iterations {
        let summary = measure_fresh_engine(config, dataset, &warm_cache, iteration, out)?;
        summaries.push(summary);
    }
    Ok(summaries)
}

fn run_hot(
    config: &Config,
    dataset: &str,
    cache_dir: &Path,
    out: &mut impl Write,
) -> Result<Vec<IterationSummary>, Box<dyn Error>> {
    let hot_cache = unique_child_dir(cache_dir, "hot")?;
    let _env = BenchEnv::set(&hot_cache, None);
    let session = DatasetSession::open(dataset)?;
    let engine = Engine::builder(session).build();
    let options = delineation_options(config);

    let mut summaries = Vec::with_capacity(config.iterations);
    for iteration in 0..config.iterations {
        let trace = temp_trace_path(iteration)?;
        let _env = BenchEnv::set(&hot_cache, Some(&trace));
        let start = Instant::now();
        let (layer, guard) = shed_core::telemetry::jsonl::JsonlLayer::from_path(&trace)?;
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            engine.delineate(config.outlet, &options)
        })?;
        drop(guard);
        let wall_time_ms = start.elapsed().as_secs_f64() * 1000.0;
        let summary = IterationSummary {
            iteration,
            wall_time_ms,
            // Hot mode reuses one Engine, whose object-store counters are
            // cumulative. Keep per-iteration records scoped to fresh-engine
            // cold/warm runs until the engine exposes resettable deltas.
            http_stats: None,
        };
        copy_stage_records(out, &trace, iteration, wall_time_ms)?;
        summaries.push(summary);
    }

    Ok(summaries)
}

fn measure_fresh_engine(
    config: &Config,
    dataset: &str,
    cache_dir: &Path,
    iteration: usize,
    out: &mut impl Write,
) -> Result<IterationSummary, Box<dyn Error>> {
    let trace = temp_trace_path(iteration)?;
    let summary = run_once(
        dataset,
        config.outlet,
        config.search_radius,
        cache_dir,
        Some(&trace),
    )?;
    copy_stage_records(out, &trace, iteration, summary.wall_time_ms)?;
    Ok(IterationSummary {
        iteration,
        ..summary
    })
}

fn run_once(
    dataset: &str,
    outlet: GeoCoord,
    search_radius: SearchRadiusMetres,
    cache_dir: &Path,
    trace: Option<&Path>,
) -> Result<IterationSummary, Box<dyn Error>> {
    let _env = BenchEnv::set(cache_dir, trace);
    let start = Instant::now();
    let http_stats = if let Some(trace_path) = trace {
        let (layer, guard) = shed_core::telemetry::jsonl::JsonlLayer::from_path(trace_path)?;
        let subscriber = tracing_subscriber::registry().with(layer);
        let http_stats = tracing::subscriber::with_default(subscriber, || {
            run_engine_once(dataset, outlet, search_radius)
        })?;
        drop(guard);
        http_stats
    } else {
        run_engine_once(dataset, outlet, search_radius)?
    };
    let wall_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(IterationSummary {
        iteration: 0,
        wall_time_ms,
        http_stats: http_stats.map(http_stats_value),
    })
}

fn run_engine_once(
    dataset: &str,
    outlet: GeoCoord,
    search_radius: SearchRadiusMetres,
) -> Result<Option<HttpStatsSnapshot>, Box<dyn Error>> {
    let session = DatasetSession::open(dataset)?;
    let engine = Engine::builder(session).build();
    let options = options_for_search_radius(search_radius);
    engine.delineate(outlet, &options)?;
    Ok(engine.http_stats())
}

fn http_stats_value(snapshot: HttpStatsSnapshot) -> Value {
    let per_path = snapshot
        .per_path
        .into_iter()
        .map(|(path, counters)| {
            (
                path,
                json!({
                    "requests": counters.requests,
                    "bytes_in": counters.bytes_in,
                    "bytes_out": counters.bytes_out,
                }),
            )
        })
        .collect();

    Value::Object(Map::from_iter([
        ("total_requests".to_owned(), json!(snapshot.total_requests)),
        ("total_bytes_in".to_owned(), json!(snapshot.total_bytes_in)),
        (
            "total_bytes_out".to_owned(),
            json!(snapshot.total_bytes_out),
        ),
        ("per_path".to_owned(), Value::Object(per_path)),
    ]))
}

fn delineation_options(config: &Config) -> DelineationOptions {
    options_for_search_radius(config.search_radius)
}

fn options_for_search_radius(search_radius: SearchRadiusMetres) -> DelineationOptions {
    DelineationOptions::default()
        .with_resolver_config(ResolverConfig::new().with_search_radius(search_radius))
}

fn copy_stage_records(
    out: &mut impl Write,
    trace: &Path,
    iteration: usize,
    wall_time_ms: f64,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(trace)?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let mut value: Value = serde_json::from_str(&line)?;
        let Some(object) = value.as_object_mut() else {
            continue;
        };
        object.insert("iteration".to_owned(), json!(iteration));
        object.insert("iteration_wall_time_ms".to_owned(), json!(wall_time_ms));
        write_jsonl(out, value)?;
    }
    Ok(())
}

fn header_record(config: &Config, dataset: &str, cache_dir: &Path) -> Value {
    json!({
        "kind": "header",
        "dataset": dataset,
        "dataset_arg": config.dataset,
        "mode": mode_name(config.mode),
        "outlet": {
            "label": config.outlet_label,
            "lat": config.outlet.lat,
            "lon": config.outlet.lon,
        },
        "search_radius_m": config.search_radius.as_f64(),
        "iterations": config.iterations,
        "cache_dir": cache_dir.display().to_string(),
        "tool": {
            "name": "bench_delineate",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "notes": warm_note(config.mode),
    })
}

fn iteration_record(summary: &IterationSummary) -> Value {
    let mut record = Map::new();
    record.insert("kind".to_owned(), json!("iteration"));
    record.insert("iteration".to_owned(), json!(summary.iteration));
    record.insert("wall_time_ms".to_owned(), json!(summary.wall_time_ms));
    if let Some(stats) = &summary.http_stats {
        record.insert("http".to_owned(), stats.clone());
    }
    Value::Object(record)
}

fn final_record(summaries: &[IterationSummary]) -> Value {
    let walls: Vec<f64> = summaries
        .iter()
        .map(|summary| summary.wall_time_ms)
        .collect();
    json!({
        "kind": "summary",
        "iterations": summaries.len(),
        "wall_time_ms": {
            "min": percentile(&walls, 0.0),
            "median": percentile(&walls, 0.5),
            "p95": percentile(&walls, 0.95),
            "max": percentile(&walls, 1.0),
        },
    })
}

fn percentile(values: &[f64], q: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = ((sorted.len() - 1) as f64 * q).ceil() as usize;
    sorted.get(rank).copied()
}

fn write_jsonl(out: &mut impl Write, value: Value) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *out, &value)?;
    out.write_all(b"\n")?;
    Ok(())
}

fn resolve_dataset(dataset: &str) -> Result<String, Box<dyn Error>> {
    match dataset {
        "r2" => Ok(R2_DATASET.to_owned()),
        "local" => local_fixture_dataset(),
        other => Ok(other.to_owned()),
    }
}

#[cfg(feature = "test-fixtures")]
fn local_fixture_dataset() -> Result<String, Box<dyn Error>> {
    let builder = shed_core::testutil::DatasetBuilder::new(8).with_longitude_span(-1.0, 1.0);
    let (_dir, root) = builder.build();
    let persistent_root = unique_child_dir(&env::temp_dir(), "shed-bench-local-fixture")?;
    copy_dir(&root, &persistent_root)?;
    Ok(persistent_root.display().to_string())
}

#[cfg(not(feature = "test-fixtures"))]
fn local_fixture_dataset() -> Result<String, Box<dyn Error>> {
    Err("--dataset local requires building shed-core with --features test-fixtures; pass an explicit dataset path otherwise".into())
}

#[cfg(feature = "test-fixtures")]
fn copy_dir(from: &Path, to: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn temp_trace_path(iteration: usize) -> Result<PathBuf, Box<dyn Error>> {
    let dir = env::temp_dir().join("shed-bench-traces");
    fs::create_dir_all(&dir)?;
    Ok(dir.join(format!("trace-{}-{iteration}.jsonl", unique_suffix()?)))
}

fn unique_child_dir(parent: &Path, prefix: &str) -> Result<PathBuf, Box<dyn Error>> {
    let dir = parent.join(format!("{prefix}-{}", unique_suffix()?));
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn unique_suffix() -> Result<String, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{}-{nanos}", std::process::id()))
}

impl Config {
    fn from_args() -> Result<Self, Box<dyn Error>> {
        parse_config_args(env::args().skip(1))
    }
}

fn parse_config_args(args: impl IntoIterator<Item = String>) -> Result<Config, Box<dyn Error>> {
    let mut args = args.into_iter();
    let mut mode = None;
    let mut dataset = None;
    let mut outlet = None;
    let mut search_radius = SearchRadiusMetres::DEFAULT;
    let mut iterations = None;
    let mut out = None;
    let mut cache_root = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => mode = Some(parse_mode(&next_value(&mut args, "--mode")?)?),
            "--dataset" => dataset = Some(next_value(&mut args, "--dataset")?),
            "--outlet" => outlet = Some(parse_outlet(&next_value(&mut args, "--outlet")?)?),
            "--search-radius-m" => {
                search_radius = parse_search_radius_m(&next_value(&mut args, "--search-radius-m")?)?
            }
            "--iterations" => {
                iterations = Some(parse_iterations(&next_value(&mut args, "--iterations")?)?)
            }
            "--out" => out = Some(PathBuf::from(next_value(&mut args, "--out")?)),
            "--cache-dir" => {
                cache_root = Some(PathBuf::from(next_value(&mut args, "--cache-dir")?))
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    let (outlet, outlet_label) = outlet.unwrap_or((zurich(), "zurich".to_owned()));
    Ok(Config {
        mode: mode.ok_or("--mode is required")?,
        dataset: dataset.ok_or("--dataset is required")?,
        outlet,
        outlet_label,
        search_radius,
        iterations: iterations.unwrap_or(1),
        out: out.ok_or("--out is required")?,
        cache_root,
    })
}

fn next_value(
    args: &mut impl Iterator<Item = String>,
    name: &'static str,
) -> Result<String, Box<dyn Error>> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value").into())
}

fn parse_mode(value: &str) -> Result<Mode, Box<dyn Error>> {
    match value {
        "cold" => Ok(Mode::Cold),
        "warm" => Ok(Mode::Warm),
        "hot" => Ok(Mode::Hot),
        _ => Err("--mode must be cold, warm, or hot".into()),
    }
}

fn parse_outlet(value: &str) -> Result<(GeoCoord, String), Box<dyn Error>> {
    match value {
        "zurich" => Ok((zurich(), value.to_owned())),
        "repparfjord" => Ok((repparfjord(), value.to_owned())),
        "hammerfest" => Ok((hammerfest(), value.to_owned())),
        _ => {
            let Some((lat, lon)) = value.split_once(',') else {
                return Err(
                    "--outlet must be zurich, repparfjord, hammerfest, or <lat>,<lon>".into(),
                );
            };
            let lat = lat.parse::<f64>()?;
            let lon = lon.parse::<f64>()?;
            Ok((GeoCoord::new(lon, lat), value.to_owned()))
        }
    }
}

fn parse_search_radius_m(value: &str) -> Result<SearchRadiusMetres, Box<dyn Error>> {
    let metres = value.parse::<f64>()?;
    Ok(SearchRadiusMetres::new(metres)?)
}

fn parse_iterations(value: &str) -> Result<usize, Box<dyn Error>> {
    let iterations = value.parse::<usize>()?;
    if iterations == 0 {
        return Err("--iterations must be greater than zero".into());
    }
    Ok(iterations)
}

fn zurich() -> GeoCoord {
    GeoCoord::new(8.5417, 47.3769)
}

fn repparfjord() -> GeoCoord {
    GeoCoord::new(23.04, 69.97)
}

fn hammerfest() -> GeoCoord {
    GeoCoord::new(23.6821, 70.6634)
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Cold => "cold",
        Mode::Warm => "warm",
        Mode::Hot => "hot",
    }
}

fn warm_note(mode: Mode) -> Option<&'static str> {
    if mode == Mode::Warm {
        Some("warm mode uses same-process cache population followed by fresh Engine instances")
    } else {
        None
    }
}

fn print_usage() {
    eprintln!(
        "usage: bench_delineate --mode cold|warm|hot --dataset r2|local|<url-or-path> \\\n         --outlet zurich|repparfjord|hammerfest|<lat>,<lon> --iterations N --out <jsonl> \\\n         [--search-radius-m <metres>] [--cache-dir <path>]\n\ncanonical:\n  bench_delineate --mode cold --dataset r2 --outlet zurich --iterations 3 --out scratchpad/benchmarks/cold-r2-zurich.jsonl\n  bench_delineate --mode cold --dataset r2 --outlet repparfjord --iterations 3 --out scratchpad/benchmarks/cold-r2-repparfjord.jsonl\n  bench_delineate --mode cold --dataset r2 --outlet hammerfest --search-radius-m 5000 --iterations 3 --out scratchpad/benchmarks/cold-r2-hammerfest.jsonl\n\nnote: hammerfest may fail at the default 1000 m resolver radius; pass --search-radius-m when benchmarking it."
    );
}

struct BenchEnv {
    previous_trace: Option<std::ffi::OsString>,
    previous_net: Option<std::ffi::OsString>,
    previous_cache: Option<std::ffi::OsString>,
}

impl BenchEnv {
    fn set(cache_dir: &Path, trace: Option<&Path>) -> Self {
        let env = Self {
            previous_trace: env::var_os("PYSHED_BENCH_TRACE"),
            previous_net: env::var_os("PYSHED_BENCH_NET"),
            previous_cache: env::var_os("HFX_CACHE_DIR"),
        };
        // SAFETY: this single-threaded benchmark harness mutates process env only
        // around synchronous Engine construction and delineation calls.
        unsafe {
            match trace {
                Some(path) => env::set_var("PYSHED_BENCH_TRACE", path),
                None => env::remove_var("PYSHED_BENCH_TRACE"),
            }
            env::set_var("PYSHED_BENCH_NET", "1");
            env::set_var("HFX_CACHE_DIR", cache_dir);
        }
        env
    }
}

impl Drop for BenchEnv {
    fn drop(&mut self) {
        // SAFETY: see [`BenchEnv::set`].
        unsafe {
            restore_env("PYSHED_BENCH_TRACE", self.previous_trace.take());
            restore_env("PYSHED_BENCH_NET", self.previous_net.take());
            restore_env("HFX_CACHE_DIR", self.previous_cache.take());
        }
    }
}

unsafe fn restore_env(name: &str, value: Option<std::ffi::OsString>) {
    match value {
        Some(value) => unsafe { env::set_var(name, value) },
        None => unsafe { env::remove_var(name) },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use shed_core::source_telemetry::{HttpStatsSnapshot, PathCounters};

    fn args(extra: &[&str]) -> Vec<String> {
        let mut args = vec![
            "--mode",
            "hot",
            "--dataset",
            "r2",
            "--iterations",
            "1",
            "--out",
            "bench.jsonl",
        ];
        args.extend_from_slice(extra);
        args.into_iter().map(str::to_owned).collect()
    }

    #[test]
    fn parse_config_accepts_search_radius_m() {
        let config =
            crate::parse_config_args(args(&["--search-radius-m", "5000"])).expect("valid config");

        assert_eq!(config.search_radius.as_f64(), 5000.0);
    }

    #[test]
    fn parse_config_accepts_repparfjord_outlet_shorthand() {
        let config =
            crate::parse_config_args(args(&["--outlet", "repparfjord"])).expect("valid config");

        assert_eq!(config.outlet, crate::repparfjord());
        assert_eq!(config.outlet_label, "repparfjord");
    }

    #[test]
    fn iteration_record_includes_http_stats_when_present() {
        let mut per_path = BTreeMap::new();
        per_path.insert(
            "catchments.parquet".to_owned(),
            PathCounters {
                requests: 3,
                bytes_in: 42,
                bytes_out: 0,
            },
        );
        let summary = crate::IterationSummary {
            iteration: 2,
            wall_time_ms: 12.5,
            http_stats: Some(crate::http_stats_value(HttpStatsSnapshot {
                total_requests: 3,
                total_bytes_in: 42,
                total_bytes_out: 0,
                per_path,
            })),
        };

        assert_eq!(
            crate::iteration_record(&summary),
            json!({
                "kind": "iteration",
                "iteration": 2,
                "wall_time_ms": 12.5,
                "http": {
                    "total_requests": 3,
                    "total_bytes_in": 42,
                    "total_bytes_out": 0,
                    "per_path": {
                        "catchments.parquet": {
                            "requests": 3,
                            "bytes_in": 42,
                            "bytes_out": 0,
                        },
                    },
                },
            })
        );
    }
}
