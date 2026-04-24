//! Run a single synthetic large-fixture delineation for memory and timing checks.

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::env;
use std::error::Error;
use std::time::Instant;

use shed_core::algo::GeoCoord;
use shed_core::session::DatasetSession;
use shed_core::testutil::DatasetBuilder;
use shed_core::{DelineationOptions, Engine};

const DEFAULT_ATOMS: usize = 2_500;
const DEFAULT_COORDS_PER_RING: usize = 1_500;

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    atoms: usize,
    coords_per_ring: usize,
}

impl BenchConfig {
    fn from_env_and_args() -> Result<Self, Box<dyn Error>> {
        let mut config = Self {
            atoms: read_env_usize("SHED_BENCH_ATOMS")?.unwrap_or(DEFAULT_ATOMS),
            coords_per_ring: read_env_usize("SHED_BENCH_COORDS_PER_RING")?
                .unwrap_or(DEFAULT_COORDS_PER_RING),
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--atoms" => {
                    let value = args.next().ok_or("--atoms requires a value")?;
                    config.atoms = parse_positive_usize("--atoms", &value)?;
                }
                "--coords-per-ring" => {
                    let value = args.next().ok_or("--coords-per-ring requires a value")?;
                    config.coords_per_ring = parse_positive_usize("--coords-per-ring", &value)?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => return Err(format!("unknown argument: {arg}").into()),
            }
        }

        Ok(config)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    run()
}

fn run() -> Result<(), Box<dyn Error>> {
    let config = BenchConfig::from_env_and_args()?;
    let (_dir, root) = DatasetBuilder::new(config.atoms)
        .with_polygon_complexity(config.coords_per_ring)
        .build();
    let session = DatasetSession::open(&root)?;
    let engine = Engine::builder(session).build();
    let options = DelineationOptions::default().with_refine(false);
    let outlet = terminal_atom_center(config.atoms);

    let start = Instant::now();
    let result = engine.delineate(outlet, &options)?;
    let elapsed_ms = start.elapsed().as_millis();
    let wkb = result.geometry_wkb()?;

    println!(
        "{{\"atoms\":{},\"coords_per_ring\":{},\"elapsed_ms\":{},\"area_km2\":{},\"polygon_count\":{},\"wkb_bytes\":{}}}",
        config.atoms,
        config.coords_per_ring,
        elapsed_ms,
        result.area_km2().as_f64(),
        result.geometry().0.len(),
        wkb.len()
    );

    Ok(())
}

fn terminal_atom_center(atoms: usize) -> GeoCoord {
    let i = atoms as f64;
    GeoCoord::new(i * 0.5 + 0.2, 0.2)
}

fn read_env_usize(name: &str) -> Result<Option<usize>, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => parse_positive_usize(name, &value).map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(source) => Err(Box::new(source)),
    }
}

fn parse_positive_usize(name: &str, value: &str) -> Result<usize, Box<dyn Error>> {
    let parsed = value
        .parse::<usize>()
        .map_err(|source| format!("{name} must be a positive integer: {source}"))?;
    if parsed == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }
    Ok(parsed)
}

fn print_usage() {
    eprintln!(
        "usage: bench_delineation [--atoms N] [--coords-per-ring N]\n\
         env: SHED_BENCH_ATOMS, SHED_BENCH_COORDS_PER_RING"
    );
}
