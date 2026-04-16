use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{ArgAction, Args, Parser, Subcommand};
use serde_json::json;
use tracing::error;

use shed_core::algo::{CleanEpsilon, GeoCoord, SnapThreshold};
use shed_core::resolver::{ResolverConfig, SearchRadiusMetres};
use shed_core::session::DatasetSession;
use shed_core::{
    DelineationOptions, DelineationResult, Engine, EngineError, RefinementOutcome, ResolutionMethod,
};
use shed_gdal::{GdalGeometryRepair, GdalRasterSource};

// ── CLI structure ─────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(name = "shed", version, about = "Watershed delineation engine for HFX datasets")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short = 'v', long = "verbose", action = ArgAction::Count, global = true)]
    verbose: u8,

    #[arg(short = 'q', long = "quiet", global = true, conflicts_with = "verbose")]
    quiet: bool,

    #[arg(long = "json", global = true)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    Delineate(DelineateArgs),
}

#[derive(Debug, Args)]
struct DelineateArgs {
    #[arg(long, short = 'd')]
    dataset: PathBuf,

    #[arg(long, requires = "lon", conflicts_with = "outlets")]
    lat: Option<f64>,

    #[arg(long, requires = "lat", conflicts_with = "outlets")]
    lon: Option<f64>,

    #[arg(long, conflicts_with_all = ["lat", "lon"])]
    outlets: Option<PathBuf>,

    #[arg(long, short = 'o')]
    output: Option<PathBuf>,

    #[arg(long, short = 'f', default_value = "geojson", value_enum)]
    format: OutputFormat,

    #[arg(long)]
    snap_radius: Option<f64>,

    #[arg(long)]
    snap_threshold: Option<u32>,

    #[arg(long)]
    clean_epsilon: Option<f64>,

    #[arg(long)]
    no_refine: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum OutputFormat {
    Geojson,
}

// ── Outlet ────────────────────────────────────────────────────────────────────

struct Outlet {
    id: Option<String>,
    name: Option<String>,
    coord: GeoCoord,
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.verbose, cli.quiet, cli.json);

    match cli.command {
        Command::Delineate(args) => match run_delineate(&args, cli.json) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                if cli.json {
                    let envelope = json!({"error": format!("{e:#}")});
                    let _ = writeln!(io::stdout(), "{envelope}");
                } else {
                    error!("{e:#}");
                }
                ExitCode::FAILURE
            }
        },
    }
}

// ── Tracing initialisation ────────────────────────────────────────────────────

fn init_tracing(verbose: u8, quiet: bool, json: bool) {
    // RUST_LOG wins over everything.
    if std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_writer(io::stderr)
            .with_env_filter(tracing_subscriber::EnvFilter::from_env("RUST_LOG"))
            .init();
        return;
    }

    // --quiet or (--json without explicit verbosity): suppress tracing.
    if quiet || (json && verbose == 0) {
        return;
    }

    let level = match verbose {
        0 => tracing::Level::WARN,
        1 => tracing::Level::INFO,
        2 => tracing::Level::DEBUG,
        _ => tracing::Level::TRACE,
    };

    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_max_level(level)
        .init();
}

// ── run_delineate ─────────────────────────────────────────────────────────────

fn run_delineate(args: &DelineateArgs, json_mode: bool) -> Result<()> {
    // 1. Parse outlets.
    let outlets = parse_outlets(args)?;

    // 2. Open session.
    let session =
        DatasetSession::open(&args.dataset).context("failed to open HFX dataset session")?;

    // 3. Build engine.
    let engine = Engine::builder(session)
        .with_raster_source(GdalRasterSource::new())
        .with_geometry_repair(GdalGeometryRepair::new())
        .build();

    // 4. Build options.
    let options = build_options(args)?;

    // 5. Delineate.
    let coords: Vec<GeoCoord> = outlets.iter().map(|o| o.coord).collect();
    let results = if coords.len() == 1 {
        vec![engine.delineate(coords[0], &options)]
    } else {
        engine.delineate_batch_uniform(&coords, &options)
    };

    // 6. Write output.
    write_output(args, &outlets, &results, json_mode)
}

// ── Option building ───────────────────────────────────────────────────────────

fn build_options(args: &DelineateArgs) -> Result<DelineationOptions> {
    let mut options = DelineationOptions::default();

    if let Some(radius) = args.snap_radius {
        let sr = SearchRadiusMetres::new(radius)
            .map_err(|e| anyhow::anyhow!("invalid --snap-radius: {e}"))?;
        options = options.with_resolver_config(ResolverConfig::new().with_search_radius(sr));
    }

    if let Some(threshold) = args.snap_threshold {
        options = options.with_snap_threshold(SnapThreshold::new(threshold));
    }

    if let Some(epsilon) = args.clean_epsilon {
        options = options.with_clean_epsilon(CleanEpsilon::new(epsilon));
    }

    if args.no_refine {
        options = options.with_refine(false);
    }

    Ok(options)
}

// ── Outlet parsing ────────────────────────────────────────────────────────────

fn parse_outlets(args: &DelineateArgs) -> Result<Vec<Outlet>> {
    if let (Some(lat), Some(lon)) = (args.lat, args.lon) {
        validate_coord(lat, lon)?;
        return Ok(vec![Outlet {
            id: None,
            name: None,
            coord: GeoCoord::new(lon, lat),
        }]);
    }

    if let Some(path) = &args.outlets {
        return parse_csv_outlets(path);
    }

    anyhow::bail!("provide either --lat + --lon or --outlets <csv>")
}

fn validate_coord(lat: f64, lon: f64) -> Result<()> {
    if !(-90.0..=90.0).contains(&lat) {
        anyhow::bail!("latitude {lat} is outside [-90, 90]");
    }
    if !(-180.0..=180.0).contains(&lon) {
        anyhow::bail!("longitude {lon} is outside [-180, 180]");
    }
    Ok(())
}

fn parse_csv_outlets(path: &PathBuf) -> Result<Vec<Outlet>> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("failed to open outlets CSV: {}", path.display()))?;

    let headers = reader.headers().context("failed to read CSV headers")?.clone();
    let has_id = headers.iter().any(|h| h == "id");
    let has_name = headers.iter().any(|h| h == "name");
    let has_lat = headers.iter().any(|h| h == "lat");
    let has_lon = headers.iter().any(|h| h == "lon");

    if !has_lat || !has_lon {
        anyhow::bail!("CSV is missing required columns: lat and lon");
    }

    let mut outlets = Vec::new();
    for (row_idx, result) in reader.records().enumerate() {
        let record = result.with_context(|| format!("failed to read CSV row {}", row_idx + 2))?;

        let lat: f64 = record
            .get(headers.iter().position(|h| h == "lat").unwrap())
            .ok_or_else(|| anyhow::anyhow!("row {}: missing lat", row_idx + 2))?
            .parse()
            .with_context(|| format!("row {}: invalid lat", row_idx + 2))?;

        let lon: f64 = record
            .get(headers.iter().position(|h| h == "lon").unwrap())
            .ok_or_else(|| anyhow::anyhow!("row {}: missing lon", row_idx + 2))?
            .parse()
            .with_context(|| format!("row {}: invalid lon", row_idx + 2))?;

        validate_coord(lat, lon)
            .with_context(|| format!("row {}: coordinate out of range", row_idx + 2))?;

        let id = if has_id {
            headers
                .iter()
                .position(|h| h == "id")
                .and_then(|i| record.get(i))
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        } else {
            None
        };

        let name = if has_name {
            headers
                .iter()
                .position(|h| h == "name")
                .and_then(|i| record.get(i))
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        } else {
            None
        };

        outlets.push(Outlet { id, name, coord: GeoCoord::new(lon, lat) });
    }

    if outlets.is_empty() {
        anyhow::bail!("CSV contains no data rows");
    }

    Ok(outlets)
}

// ── Output ────────────────────────────────────────────────────────────────────

fn write_output(
    args: &DelineateArgs,
    outlets: &[Outlet],
    results: &[Result<DelineationResult, EngineError>],
    json_mode: bool,
) -> Result<()> {
    let any_failed = results.iter().any(|r| r.is_err());

    if json_mode {
        // JSON envelope to stdout; FeatureCollection goes to file if requested.
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for (outlet, result) in outlets.iter().zip(results.iter()) {
            match result {
                Ok(dr) => {
                    successes.push(json!({
                        "id": outlet.id,
                        "lat": outlet.coord.lat,
                        "lon": outlet.coord.lon,
                        "area_km2": dr.area_km2().as_f64(),
                        "terminal_atom_id": dr.terminal_atom_id().get(),
                    }));
                }
                Err(e) => {
                    failures.push(json!({
                        "id": outlet.id,
                        "lat": outlet.coord.lat,
                        "lon": outlet.coord.lon,
                        "error": format!("{e:#}"),
                    }));
                }
            }
        }

        let total = outlets.len();
        let succeeded = successes.len();
        let failed = failures.len();
        let envelope = json!({
            "successes": successes,
            "failures": failures,
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
        });

        writeln!(io::stdout(), "{}", serde_json::to_string_pretty(&envelope)?)
            .context("failed to write JSON envelope to stdout")?;

        // Optionally also write the FeatureCollection to a file.
        if let Some(output_path) = &args.output {
            let fc = build_feature_collection(outlets, results);
            let json_str =
                serde_json::to_string_pretty(&fc).context("failed to serialize GeoJSON")?;
            std::fs::write(output_path, json_str)
                .with_context(|| format!("failed to write output to {}", output_path.display()))?;
        }
    } else {
        // Normal mode: GeoJSON FeatureCollection.
        let fc = build_feature_collection(outlets, results);
        let json_str = serde_json::to_string_pretty(&fc).context("failed to serialize GeoJSON")?;

        if let Some(output_path) = &args.output {
            std::fs::write(output_path, json_str)
                .with_context(|| format!("failed to write output to {}", output_path.display()))?;
        } else {
            writeln!(io::stdout(), "{json_str}").context("failed to write GeoJSON to stdout")?;
        }
    }

    if any_failed {
        anyhow::bail!("one or more outlets failed to delineate");
    }

    Ok(())
}

fn build_feature_collection(
    outlets: &[Outlet],
    results: &[Result<DelineationResult, EngineError>],
) -> serde_json::Value {
    let features: Vec<serde_json::Value> = outlets
        .iter()
        .zip(results.iter())
        .filter_map(|(outlet, result)| result.as_ref().ok().map(|dr| (outlet, dr)))
        .map(|(outlet, dr)| result_to_geojson_feature(dr, outlet))
        .collect();

    json!({
        "type": "FeatureCollection",
        "features": features,
    })
}

// ── GeoJSON serialisation ─────────────────────────────────────────────────────

fn result_to_geojson_feature(result: &DelineationResult, outlet: &Outlet) -> serde_json::Value {
    let geometry = multi_polygon_to_geojson(result.geometry());
    let mut properties = serde_json::Map::new();

    if let Some(id) = &outlet.id {
        properties.insert("id".into(), json!(id));
    }
    if let Some(name) = &outlet.name {
        properties.insert("name".into(), json!(name));
    }
    properties.insert("area_km2".into(), json!(result.area_km2().as_f64()));
    properties.insert("terminal_atom_id".into(), json!(result.terminal_atom_id().get()));
    properties.insert("input_lat".into(), json!(result.input_outlet().lat));
    properties.insert("input_lon".into(), json!(result.input_outlet().lon));
    properties.insert("resolved_lat".into(), json!(result.resolved_outlet().lat));
    properties.insert("resolved_lon".into(), json!(result.resolved_outlet().lon));
    properties.insert("upstream_atom_count".into(), json!(result.upstream_atom_ids().len()));
    properties.insert(
        "resolution_method".into(),
        json!(format_resolution_method(result.resolution_method())),
    );
    properties.insert("refinement".into(), json!(format_refinement(result.refinement())));

    json!({
        "type": "Feature",
        "geometry": geometry,
        "properties": properties,
    })
}

fn multi_polygon_to_geojson(mp: &geo::MultiPolygon<f64>) -> serde_json::Value {
    let polygons: Vec<serde_json::Value> = mp
        .0
        .iter()
        .map(|poly| {
            let mut rings = Vec::new();
            rings.push(ring_to_coords(poly.exterior()));
            for hole in poly.interiors() {
                rings.push(ring_to_coords(hole));
            }
            json!(rings)
        })
        .collect();

    json!({"type": "MultiPolygon", "coordinates": polygons})
}

fn ring_to_coords(ls: &geo::LineString<f64>) -> Vec<[f64; 2]> {
    ls.coords().map(|c| [c.x, c.y]).collect()
}

fn format_resolution_method(method: &ResolutionMethod) -> String {
    match method {
        ResolutionMethod::Snap {
            snap_id,
            distance_m,
            weight,
            mainstem_status,
            candidates_considered,
        } => format!(
            "snap(id={snap_id:?}, dist={distance_m:.1}m, weight={weight:?}, \
             mainstem={mainstem_status:?}, candidates={candidates_considered})"
        ),
        ResolutionMethod::PointInPolygon { candidates_considered, tie_break } => match tie_break {
            Some(tb) => format!("pip(candidates={candidates_considered}, tie_break={tb:?})"),
            None => format!("pip(candidates={candidates_considered})"),
        },
    }
}

fn format_refinement(r: &RefinementOutcome) -> String {
    match r {
        RefinementOutcome::Applied { refined_outlet } => {
            format!("applied(lon={:.6}, lat={:.6})", refined_outlet.lon, refined_outlet.lat)
        }
        RefinementOutcome::NoRastersAvailable => "no_rasters_available".into(),
        RefinementOutcome::NoRasterSourceProvided => "no_raster_source_provided".into(),
        RefinementOutcome::Disabled => "disabled".into(),
    }
}
