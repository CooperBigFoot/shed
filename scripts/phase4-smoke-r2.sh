#!/usr/bin/env bash
set -uo pipefail

PUBLIC_ROOT="https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/"
MANIFEST_URL="${PUBLIC_ROOT}manifest.json"
LAT="70.4521297483898"
LON="28.4906601273434"
REMOTE_OUTPUT="/tmp/phase4-smoke.geojson"
LOCAL_OUTPUT="${TMPDIR:-/tmp}/phase4-smoke-local.geojson"
SCRIPT_VERSION="r1.1"

started_at_iso8601=$(date -u "+%Y-%m-%dT%H:%M:%SZ")

emit_missing_jq_error() {
  printf '{"record_type":"script_error","message":"missing required command: jq"}\n'
}

if ! command -v jq >/dev/null 2>&1; then
  emit_missing_jq_error
  exit 2
fi

emit_script_error() {
  local message=$1
  local detail=${2:-}

  jq -cn \
    --arg message "$message" \
    --arg detail "$detail" \
    '{
      record_type: "script_error",
      message: $message,
      detail: (if $detail == "" then null else $detail end)
    }'
}

emit_header() {
  local fabric_name=$1
  local adapter_version=$2
  local cache_dir_purged=$3
  local trace_path=$4

  jq -cn \
    --arg fabric_name "$fabric_name" \
    --arg adapter_version "$adapter_version" \
    --argjson cache_dir_purged "$cache_dir_purged" \
    --arg trace_path "$trace_path" \
    --arg started_at_iso8601 "$started_at_iso8601" \
    --arg script_version "$SCRIPT_VERSION" \
    '{
      record_type: "header",
      fabric_name: $fabric_name,
      adapter_version: $adapter_version,
      cache_dir_purged: $cache_dir_purged,
      trace_path: $trace_path,
      started_at_iso8601: $started_at_iso8601,
      script_version: $script_version
    }'
}

emit_dataset_run() {
  local dataset=$1
  local status=$2
  local failure_kind=$3
  local exit_code=$4
  local signal_number=$5
  local bytes_on_wire=$6
  local peak_rss_bytes=$7
  local wall_seconds=$8
  local stderr_log_path=$9
  local object_store_trace_path=${10}
  local fabric_name=${11}
  local adapter_version=${12}

  jq -cn \
    --arg dataset "$dataset" \
    --arg status "$status" \
    --arg failure_kind "$failure_kind" \
    --argjson exit_code "$exit_code" \
    --argjson signal_number "$signal_number" \
    --argjson bytes_on_wire "$bytes_on_wire" \
    --argjson peak_rss_bytes "$peak_rss_bytes" \
    --argjson wall_seconds "$wall_seconds" \
    --argjson lat "$LAT" \
    --argjson lon "$LON" \
    --arg stderr_log_path "$stderr_log_path" \
    --arg object_store_trace_path "$object_store_trace_path" \
    --arg fabric_name "$fabric_name" \
    --arg adapter_version "$adapter_version" \
    '{
      record_type: "dataset_run",
      dataset: $dataset,
      status: $status,
      failure_kind: (if $failure_kind == "" then null else $failure_kind end),
      exit_code: $exit_code,
      signal_number: $signal_number,
      bytes_on_wire: $bytes_on_wire,
      peak_rss_bytes: $peak_rss_bytes,
      wall_seconds: $wall_seconds,
      outlet: [$lat, $lon],
      stderr_log_path: $stderr_log_path,
      object_store_trace_path: (if $object_store_trace_path == "" then null else $object_store_trace_path end),
      fabric_name: $fabric_name,
      adapter_version: $adapter_version
    }'
}

wall_seconds() {
  local start=$1
  local end=$2
  awk -v start="$start" -v end="$end" 'BEGIN { printf "%.3f", end - start }'
}

json_number_field() {
  local json=$1
  local key=$2

  printf '%s\n' "$json" |
    jq -er --arg key "$key" '.[$key] | select(type == "number")' 2>/dev/null
}

peak_rss_from_measurement() {
  local rss_json=$1
  local peak_rss

  peak_rss=$(json_number_field "$rss_json" "max_rss_bytes")
  if [[ -z ${peak_rss:-} ]]; then
    peak_rss=null
  fi

  printf '%s\n' "$peak_rss"
}

exit_code_from_measurement() {
  local rss_json=$1
  local fallback_exit_code=$2
  local measured_exit_code

  measured_exit_code=$(json_number_field "$rss_json" "exit_status")
  if [[ -z ${measured_exit_code:-} ]]; then
    measured_exit_code=$fallback_exit_code
  fi

  printf '%s\n' "$measured_exit_code"
}

bytes_on_wire_from_trace() {
  local trace_file=$1
  local bytes_on_wire

  if [[ ! -f "$trace_file" ]]; then
    printf '0\n'
    return 0
  fi

  bytes_on_wire=$(awk '
    {
      line = tolower($0)
      while (match(line, /range[^[:alnum:]]+bytes[[:space:]]*=[^0-9]*[0-9]+[[:space:]]*-[[:space:]]*[0-9]+/)) {
        token = substr(line, RSTART, RLENGTH)
        gsub(/[^0-9-]/, "", token)
        split(token, parts, "-")
        if (parts[1] != "" && parts[2] != "") {
          total += parts[2] - parts[1] + 1
        }
        line = substr(line, RSTART + RLENGTH)
      }
    }
    END {
      printf "%.0f\n", total + 0
    }
  ' "$trace_file" 2>/dev/null)

  if [[ -z ${bytes_on_wire:-} ]]; then
    bytes_on_wire=0
  fi

  printf '%s\n' "$bytes_on_wire"
  return 0
}

classify_exit_code() {
  local exit_code=$1
  local status
  local failure_kind
  local signal_number

  if [[ "$exit_code" -eq 0 ]]; then
    status=ok
    failure_kind=""
    signal_number=null
  elif [[ "$exit_code" -gt 128 ]]; then
    status=failed
    failure_kind=signal
    signal_number=$((exit_code - 128))
  else
    status=failed
    failure_kind=non_zero_exit
    signal_number=null
  fi

  printf '%s\t%s\t%s\n' "$status" "$failure_kind" "$signal_number"
}

run_remote() {
  local trace_file=$1
  local fabric_name=$2
  local adapter_version=$3
  local start
  local end
  local rss_json
  local wrapper_exit_code
  local exit_code
  local peak_rss
  local seconds
  local bytes_on_wire
  local classification
  local status
  local failure_kind
  local signal_number

  if [[ ${HFX_SMOKE_FAULT_INJECT:-} == "remote_segfault" ]]; then
    printf 'fault injection: simulating remote segfault without running remote command\n' >&2
    printf 'HFX_SMOKE_FAULT_INJECT=remote_segfault simulated exit 139\n' >"$trace_file"
    exit_code=139
    peak_rss=67108864
    seconds=0.125
    bytes_on_wire=0
  else
    start=$(date +%s)
    rss_json=$(RUST_LOG=object_store=trace scripts/measure-rss.sh --bin ./target/release/shed -- delineate --dataset "$PUBLIC_ROOT" --lat "$LAT" --lon "$LON" --output "$REMOTE_OUTPUT" 2>"$trace_file")
    wrapper_exit_code=$?
    end=$(date +%s)

    exit_code=$(exit_code_from_measurement "$rss_json" "$wrapper_exit_code")
    peak_rss=$(peak_rss_from_measurement "$rss_json")
    seconds=$(wall_seconds "$start" "$end")
    bytes_on_wire=$(bytes_on_wire_from_trace "$trace_file")
  fi

  classification=$(classify_exit_code "$exit_code")
  status=$(printf '%s\n' "$classification" | awk -F '\t' '{ print $1 }')
  failure_kind=$(printf '%s\n' "$classification" | awk -F '\t' '{ print $2 }')
  signal_number=$(printf '%s\n' "$classification" | awk -F '\t' '{ print $3 }')

  emit_dataset_run \
    "remote-r2" \
    "$status" \
    "$failure_kind" \
    "$exit_code" \
    "$signal_number" \
    "$bytes_on_wire" \
    "$peak_rss" \
    "$seconds" \
    "$trace_file" \
    "$trace_file" \
    "$fabric_name" \
    "$adapter_version"

  return 0
}

run_local() {
  local dataset_root=$1
  local stderr_log_file=$2
  local fabric_name=$3
  local adapter_version=$4
  local start
  local end
  local rss_json
  local wrapper_exit_code
  local exit_code
  local peak_rss
  local seconds
  local classification
  local status
  local failure_kind
  local signal_number

  start=$(date +%s)
  rss_json=$(scripts/measure-rss.sh --bin ./target/release/shed -- delineate --dataset "$dataset_root" --lat "$LAT" --lon "$LON" --output "$LOCAL_OUTPUT" 2>"$stderr_log_file")
  wrapper_exit_code=$?
  end=$(date +%s)

  exit_code=$(exit_code_from_measurement "$rss_json" "$wrapper_exit_code")
  peak_rss=$(peak_rss_from_measurement "$rss_json")
  seconds=$(wall_seconds "$start" "$end")

  classification=$(classify_exit_code "$exit_code")
  status=$(printf '%s\n' "$classification" | awk -F '\t' '{ print $1 }')
  failure_kind=$(printf '%s\n' "$classification" | awk -F '\t' '{ print $2 }')
  signal_number=$(printf '%s\n' "$classification" | awk -F '\t' '{ print $3 }')

  emit_dataset_run \
    "local-disk" \
    "$status" \
    "$failure_kind" \
    "$exit_code" \
    "$signal_number" \
    0 \
    "$peak_rss" \
    "$seconds" \
    "$stderr_log_file" \
    "" \
    "$fabric_name" \
    "$adapter_version"

  return 0
}

manifest=$(curl -fsSL "$MANIFEST_URL")
curl_status=$?
if [[ "$curl_status" -ne 0 ]]; then
  emit_script_error "failed to fetch published manifest" "url=$MANIFEST_URL status=$curl_status"
  exit 2
fi

fabric_name=$(printf '%s\n' "$manifest" | jq -er '.fabric_name | select(type == "string" and length > 0)' 2>/dev/null)
fabric_status=$?
adapter_version=$(printf '%s\n' "$manifest" | jq -er '.adapter_version | select(type == "string" and length > 0)' 2>/dev/null)
adapter_status=$?

if [[ "$fabric_status" -ne 0 || "$adapter_status" -ne 0 ]]; then
  emit_script_error "failed to parse fabric_name and adapter_version from published manifest" "url=$MANIFEST_URL"
  exit 2
fi

cache_root=${HFX_CACHE_DIR:-$HOME/.cache/hfx}
cache_dir="${cache_root%/}/$fabric_name/$adapter_version"
trace_file=$(mktemp "${TMPDIR:-/tmp}/phase4-smoke-r2-trace.XXXXXX")
local_stderr_file=$(mktemp "${TMPDIR:-/tmp}/phase4-smoke-r2-local-stderr.XXXXXX")
local_root=${LOCAL_HFX_ROOT:-$HOME/Desktop/merit-hfx/global/hfx}

printf 'published manifest: fabric_name=%s adapter_version=%s\n' "$fabric_name" "$adapter_version" >&2
printf 'purging remote cache subdirectory: %s\n' "$cache_dir" >&2
rm -rf "$cache_dir"
rm_status=$?
if [[ "$rm_status" -eq 0 ]]; then
  cache_dir_purged=true
else
  cache_dir_purged=false
  printf 'warning: failed to purge remote cache subdirectory: %s\n' "$cache_dir" >&2
fi

printf 'remote object_store trace and stderr: %s\n' "$trace_file" >&2
printf 'local stderr: %s\n' "$local_stderr_file" >&2
printf 'summary: GDAL /vsis3/ raster reads bypass object_store and are not counted in bytes_on_wire.\n' >&2

emit_header "$fabric_name" "$adapter_version" "$cache_dir_purged" "$trace_file"
run_remote "$trace_file" "$fabric_name" "$adapter_version"
run_local "$local_root" "$local_stderr_file" "$fabric_name" "$adapter_version"
