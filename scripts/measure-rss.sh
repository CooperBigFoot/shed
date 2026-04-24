#!/usr/bin/env bash
set -u

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/measure-rss.sh --bin target/release/examples/bench_delineation -- --atoms 2500 --coords-per-ring 1500
  scripts/measure-rss.sh <command> [args...]
EOF
}

json_escape() {
  local value=$1
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  value=${value//$'\r'/\\r}
  value=${value//$'\t'/\\t}
  printf '%s' "$value"
}

if [[ $# -eq 0 ]]; then
  usage
  exit 2
fi

if [[ ${1:-} == "--bin" ]]; then
  if [[ $# -lt 2 ]]; then
    usage
    exit 2
  fi
  bin=$2
  shift 2
  if [[ ${1:-} == "--" ]]; then
    shift
  fi
  cmd=("$bin" "$@")
else
  if [[ ${1:-} == "--" ]]; then
    shift
  fi
  cmd=("$@")
fi

if [[ ${#cmd[@]} -eq 0 ]]; then
  usage
  exit 2
fi

stdout_file=$(mktemp "${TMPDIR:-/tmp}/shed-rss-stdout.XXXXXX")
stderr_file=$(mktemp "${TMPDIR:-/tmp}/shed-rss-stderr.XXXXXX")
trap 'rm -f "$stdout_file" "$stderr_file"' EXIT

platform=$(uname -s)
case "$platform" in
  Darwin)
    time_args=(-l)
    ;;
  Linux)
    time_args=(-v)
    ;;
  *)
    echo "unsupported platform for /usr/bin/time RSS parsing: $platform" >&2
    exit 2
    ;;
esac

if /usr/bin/time "${time_args[@]}" bash -c '"$@"; status=$?; printf "\n__SHED_RSS_EXIT_STATUS=%d\n" "$status" >&2; exit "$status"' shed-rss "${cmd[@]}" >"$stdout_file" 2>"$stderr_file"; then
  time_status=0
else
  time_status=$?
fi

exit_status=$(awk -F= '/^__SHED_RSS_EXIT_STATUS=/ { value = $2 } END { if (value != "") print value }' "$stderr_file")
if [[ -z ${exit_status:-} ]]; then
  exit_status=$time_status
fi

if [[ -s "$stdout_file" ]]; then
  printf '%s\n' "--- command stdout ---" >&2
  cat "$stdout_file" >&2
fi

if [[ -s "$stderr_file" ]]; then
  printf '%s\n' "--- command stderr and time output ---" >&2
  awk '!/^__SHED_RSS_EXIT_STATUS=/' "$stderr_file" >&2
fi

case "$platform" in
  Darwin)
    max_rss_bytes=$(awk '/maximum resident set size/ { value = $1 } END { if (value != "") print value }' "$stderr_file")
    ;;
  Linux)
    max_rss_bytes=$(awk -F: '/Maximum resident set size/ { gsub(/^[ \t]+/, "", $2); value = $2 * 1024 } END { if (value != "") printf "%.0f\n", value }' "$stderr_file")
    ;;
esac

if [[ -z ${max_rss_bytes:-} ]]; then
  max_rss_json=null
else
  max_rss_json=$max_rss_bytes
fi

command_string=$(printf '%q ' "${cmd[@]}")
command_string=${command_string% }
escaped_command=$(json_escape "$command_string")
printf '{"command":"%s","exit_status":%d,"max_rss_bytes":%s}\n' \
  "$escaped_command" \
  "$exit_status" \
  "$max_rss_json"

exit "$exit_status"
