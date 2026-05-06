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
use shed_core::{DelineationOptions, Engine};
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
    let _ = run_once(dataset, config.outlet, &warm_cache, None)?;

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
    let options = DelineationOptions::default();

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
    let summary = run_once(dataset, config.outlet, cache_dir, Some(&trace))?;
    copy_stage_records(out, &trace, iteration, summary.wall_time_ms)?;
    Ok(IterationSummary {
        iteration,
        ..summary
    })
}

fn run_once(
    dataset: &str,
    outlet: GeoCoord,
    cache_dir: &Path,
    trace: Option<&Path>,
) -> Result<IterationSummary, Box<dyn Error>> {
    let _env = BenchEnv::set(cache_dir, trace);
    let start = Instant::now();
    if let Some(trace_path) = trace {
        let (layer, guard) = shed_core::telemetry::jsonl::JsonlLayer::from_path(trace_path)?;
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || run_engine_once(dataset, outlet))?;
        drop(guard);
    } else {
        run_engine_once(dataset, outlet)?;
    }

    Ok(IterationSummary {
        iteration: 0,
        wall_time_ms: start.elapsed().as_secs_f64() * 1000.0,
        http_stats: None,
    })
}

fn run_engine_once(dataset: &str, outlet: GeoCoord) -> Result<(), Box<dyn Error>> {
    let session = DatasetSession::open(dataset)?;
    let engine = Engine::builder(session).build();
    let options = DelineationOptions::default();
    engine.delineate(outlet, &options)?;
    Ok(())
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
        let mut args = env::args().skip(1);
        let mut mode = None;
        let mut dataset = None;
        let mut outlet = None;
        let mut iterations = None;
        let mut out = None;
        let mut cache_root = None;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--mode" => mode = Some(parse_mode(&next_value(&mut args, "--mode")?)?),
                "--dataset" => dataset = Some(next_value(&mut args, "--dataset")?),
                "--outlet" => outlet = Some(parse_outlet(&next_value(&mut args, "--outlet")?)?),
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
        Ok(Self {
            mode: mode.ok_or("--mode is required")?,
            dataset: dataset.ok_or("--dataset is required")?,
            outlet,
            outlet_label,
            iterations: iterations.unwrap_or(1),
            out: out.ok_or("--out is required")?,
            cache_root,
        })
    }
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
        "hammerfest" => Ok((hammerfest(), value.to_owned())),
        _ => {
            let Some((lat, lon)) = value.split_once(',') else {
                return Err("--outlet must be zurich, hammerfest, or <lat>,<lon>".into());
            };
            let lat = lat.parse::<f64>()?;
            let lon = lon.parse::<f64>()?;
            Ok((GeoCoord::new(lon, lat), value.to_owned()))
        }
    }
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
        "usage: bench_delineate --mode cold|warm|hot --dataset r2|local|<url-or-path> \\\n+         --outlet zurich|hammerfest|<lat>,<lon> --iterations N --out <jsonl> \\\n+         [--cache-dir <path>]"
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
