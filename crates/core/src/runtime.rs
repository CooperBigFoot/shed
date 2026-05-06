//! Shared async runtime for object-store backed readers.

use std::num::NonZeroUsize;
use std::sync::LazyLock;

use tokio::runtime::{Builder, Runtime};

const MIN_WORKER_THREADS: usize = 2;
const MAX_WORKER_THREADS: usize = 16;
const FALLBACK_WORKER_THREADS: usize = 8;

pub(crate) static RT: LazyLock<Runtime> =
    LazyLock::new(|| build_runtime(std::env::var("PYSHED_TOKIO_WORKERS").ok().as_deref()));

pub(crate) fn build_runtime(env_value: Option<&str>) -> Runtime {
    match Builder::new_multi_thread()
        .worker_threads(parse_worker_threads(env_value))
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("failed to build shared Tokio runtime: {error}"),
    }
}

fn parse_worker_threads(env_value: Option<&str>) -> usize {
    env_value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map_or_else(default_worker_threads, clamp_worker_threads)
}

fn default_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(FALLBACK_WORKER_THREADS)
        .clamp(MIN_WORKER_THREADS, MAX_WORKER_THREADS)
}

fn clamp_worker_threads(value: usize) -> usize {
    value.clamp(MIN_WORKER_THREADS, MAX_WORKER_THREADS)
}

#[cfg(test)]
mod tests {
    use super::{clamp_worker_threads, default_worker_threads, parse_worker_threads};

    #[test]
    fn env_override_sets_worker_threads() {
        assert_eq!(parse_worker_threads(Some("6")), 6);
    }

    #[test]
    fn env_override_clamps_to_minimum() {
        assert_eq!(parse_worker_threads(Some("1")), 2);
    }

    #[test]
    fn env_override_clamps_to_maximum() {
        assert_eq!(parse_worker_threads(Some("64")), 16);
    }

    #[test]
    fn invalid_env_override_falls_back_to_default() {
        assert_eq!(
            parse_worker_threads(Some("invalid")),
            default_worker_threads()
        );
    }

    #[test]
    fn nonpositive_env_override_falls_back_to_default() {
        assert_eq!(parse_worker_threads(Some("0")), default_worker_threads());
    }

    #[test]
    fn missing_env_override_falls_back_to_default() {
        assert_eq!(parse_worker_threads(None), default_worker_threads());
    }

    #[test]
    fn default_worker_threads_is_clamped() {
        let default_threads = default_worker_threads();
        assert_eq!(default_threads, clamp_worker_threads(default_threads));
    }
}
