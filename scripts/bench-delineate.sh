#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/bench-delineate.sh --mode cold|warm|hot --dataset r2|local|<url-or-path> \
    --outlet zurich|repparfjord|hammerfest|<lat>,<lon> --iterations N --out <jsonl> \
    [--search-radius-m <metres>] [--cache-dir <path>] [--release] [--measure-rss]

canonical:
  scripts/bench-delineate.sh --release --measure-rss --mode cold --dataset r2 --outlet zurich --iterations 3 --out scratchpad/benchmarks/cold-r2-zurich.jsonl
  scripts/bench-delineate.sh --release --measure-rss --mode cold --dataset r2 --outlet repparfjord --iterations 3 --out scratchpad/benchmarks/cold-r2-repparfjord.jsonl
  scripts/bench-delineate.sh --release --measure-rss --mode cold --dataset r2 --outlet hammerfest --search-radius-m 5000 --iterations 3 --out scratchpad/benchmarks/cold-r2-hammerfest.jsonl
  scripts/bench-delineate.sh --release --measure-rss --mode warm --dataset r2 --outlet zurich --iterations 5 --out scratchpad/benchmarks/warm-r2-zurich.jsonl
  scripts/bench-delineate.sh --release --measure-rss --mode hot --dataset r2 --outlet zurich --iterations 10 --out scratchpad/benchmarks/hot-r2-zurich.jsonl

note: hammerfest may fail at the default 1000 m resolver radius; pass --search-radius-m when benchmarking it.
EOF
}

release=0
measure_rss=0
bench_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      release=1
      shift
      ;;
    --measure-rss)
      measure_rss=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      bench_args+=("$1")
      shift
      ;;
  esac
done

if [[ ${#bench_args[@]} -eq 0 ]]; then
  usage
  exit 2
fi

cargo_args=(run -p shed-core --bin bench_delineate)
if [[ $release -eq 1 ]]; then
  cargo_args+=(--release)
fi
cargo_args+=(-- "${bench_args[@]}")

if [[ $measure_rss -eq 1 ]]; then
  scripts/measure-rss.sh cargo "${cargo_args[@]}"
else
  cargo "${cargo_args[@]}"
fi
