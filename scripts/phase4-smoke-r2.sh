#!/usr/bin/env bash
set -uo pipefail

PUBLIC_ROOT="https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/"
MANIFEST_URL="${PUBLIC_ROOT}manifest.json"
LAT="70.4521297483898"
LON="28.4906601273434"
REMOTE_OUTPUT="/tmp/phase4-smoke.geojson"
LOCAL_OUTPUT="${TMPDIR:-/tmp}/phase4-smoke-local.geojson"
SCRIPT_VERSION="r3.0"
ONE_GB_BYTES=1000000000
WALL_SECONDS_LIMIT=300
RSS_RATIO_LIMIT=5

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
  local cache_mode=$5

  jq -cn \
    --arg fabric_name "$fabric_name" \
    --arg adapter_version "$adapter_version" \
    --argjson cache_dir_purged "$cache_dir_purged" \
    --arg trace_path "$trace_path" \
    --arg cache_mode "$cache_mode" \
    --arg started_at_iso8601 "$started_at_iso8601" \
    --arg script_version "$SCRIPT_VERSION" \
    '{
      record_type: "header",
      fabric_name: $fabric_name,
      adapter_version: $adapter_version,
      cache_dir_purged: $cache_dir_purged,
      trace_path: $trace_path,
      cache_mode: $cache_mode,
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
  local cog_header_bytes=${13}
  local cog_tile_bytes=${14}
  local cog_tile_count=${15}
  local cog_window_pixels=${16}
  local non_cog_bytes_on_wire=${17}

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
    --argjson cog_header_bytes "$cog_header_bytes" \
    --argjson cog_tile_bytes "$cog_tile_bytes" \
    --argjson cog_tile_count "$cog_tile_count" \
    --argjson cog_window_pixels "$cog_window_pixels" \
    --argjson non_cog_bytes_on_wire "$non_cog_bytes_on_wire" \
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
      cog_header_bytes: $cog_header_bytes,
      cog_tile_bytes: $cog_tile_bytes,
      cog_tile_count: $cog_tile_count,
      cog_window_pixels: $cog_window_pixels,
      non_cog_bytes_on_wire: $non_cog_bytes_on_wire,
      fabric_name: $fabric_name,
      adapter_version: $adapter_version
    }'
}

emit_summary() {
  local remote_record=$1
  local local_record=$2

  jq -nc \
    --argjson remote "$remote_record" \
    --argjson local "$local_record" \
    --argjson one_gb_bytes "$ONE_GB_BYTES" \
    --argjson wall_seconds_limit "$WALL_SECONDS_LIMIT" \
    --argjson rss_ratio_limit "$RSS_RATIO_LIMIT" \
    '
      def number_or_null:
        if type == "number" then . else null end;

      ($remote.record_type == "dataset_run" and $local.record_type == "dataset_run") as $comparison_ok
      | ($remote.bytes_on_wire | number_or_null) as $remote_bytes
      | ($local.bytes_on_wire | number_or_null) as $local_bytes
      | ($remote.wall_seconds | number_or_null) as $remote_wall
      | ($local.wall_seconds | number_or_null) as $local_wall
      | ($remote.peak_rss_bytes | number_or_null) as $remote_rss
      | ($local.peak_rss_bytes | number_or_null) as $local_rss
      | (if $remote_wall != null and $local_wall != null and $local_wall > 0
         then (($remote_wall / $local_wall) * 100 | round / 100)
         else null
         end) as $wall_ratio
      | (if $remote_rss != null and $local_rss != null and $local_rss > 0
         then (($remote_rss / $local_rss) * 100 | round / 100)
         else null
         end) as $rss_ratio
      | {
          record_type: "summary",
          comparison_ok: $comparison_ok,
          remote_status: $remote.status,
          local_status: $local.status,
          bytes_on_wire_delta: (($remote_bytes // 0) - ($local_bytes // 0)),
          wall_seconds_ratio: $wall_ratio,
          peak_rss_bytes_ratio: $rss_ratio,
          gates: {
            bytes_on_wire_under_1gb: ($remote_bytes != null and $remote_bytes < $one_gb_bytes),
            wall_seconds_under_300: ($remote_wall != null and $remote_wall < $wall_seconds_limit),
            peak_rss_ratio_under_5x_local: ($rss_ratio != null and $rss_ratio < $rss_ratio_limit),
            local_run_succeeded: ($local.status == "ok")
          }
        }
      | .all_gates_passed = (
          .comparison_ok
          and .gates.bytes_on_wire_under_1gb
          and .gates.wall_seconds_under_300
          and .gates.peak_rss_ratio_under_5x_local
          and .gates.local_run_succeeded
        )
    '
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

trace_number_sum() {
  local trace_file=$1
  local field=$2
  local total

  if [[ ! -f "$trace_file" ]]; then
    printf '0\n'
    return 0
  fi

  total=$(awk -v field="$field" '
    {
      line = $0
      pattern = "(^|[^[:alnum:]_])" field "[[:space:]]*=[[:space:]]*[0-9]+"
      while (match(line, pattern)) {
        token = substr(line, RSTART, RLENGTH)
        sub(".*" field "[[:space:]]*=[[:space:]]*", "", token)
        total += token + 0
        line = substr(line, RSTART + RLENGTH)
      }
    }
    END {
      printf "%.0f\n", total + 0
    }
  ' "$trace_file" 2>/dev/null)

  if [[ -z ${total:-} ]]; then
    total=0
  fi

  printf '%s\n' "$total"
  return 0
}

regular_file_bytes_under_dir() {
  local dir=$1
  local bytes

  if [[ ! -d "$dir" ]]; then
    printf '0\n'
    return 0
  fi

  bytes=$(find "$dir" -type f -exec wc -c {} \; 2>/dev/null |
    awk '{ total += $1 } END { printf "%.0f\n", total + 0 }')

  if [[ -z ${bytes:-} ]]; then
    bytes=0
  fi

  printf '%s\n' "$bytes"
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
  local raster_cache_dir=$4
  local start
  local end
  local rss_json
  local wrapper_exit_code
  local exit_code
  local peak_rss
  local seconds
  local bytes_on_wire
  local trace_bytes
  local cog_header_bytes
  local cog_tile_bytes
  local cog_tile_count
  local cog_window_pixels
  local non_cog_bytes_on_wire
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
    cog_header_bytes=0
    cog_tile_bytes=0
    cog_tile_count=0
    cog_window_pixels=0
    non_cog_bytes_on_wire=0
  elif [[ ${HFX_SMOKE_FAULT_INJECT:-} == "remote_ok_local_fail" ]]; then
    printf 'fault injection: simulating remote ok without running remote command\n' >&2
    printf 'HFX_SMOKE_FAULT_INJECT=remote_ok_local_fail simulated remote ok\n' >"$trace_file"
    exit_code=0
    peak_rss=134217728
    seconds=1.250
    bytes_on_wire=1048576
    cog_header_bytes=65536
    cog_tile_bytes=983040
    cog_tile_count=8
    cog_window_pixels=262144
    non_cog_bytes_on_wire=0
  elif [[ ${HFX_SMOKE_FAULT_INJECT:-} == "both_ok_remote_huge" ]]; then
    printf 'fault injection: simulating oversized remote ok without running remote command\n' >&2
    printf 'HFX_SMOKE_FAULT_INJECT=both_ok_remote_huge simulated remote ok over gates\n' >"$trace_file"
    exit_code=0
    peak_rss=600000000
    seconds=301.000
    bytes_on_wire=1000000001
    cog_header_bytes=33554432
    cog_tile_bytes=966445569
    cog_tile_count=2048
    cog_window_pixels=268435456
    non_cog_bytes_on_wire=0
  else
    start=$(date +%s)
    rss_json=$(RUST_LOG=shed_core=debug,object_store=trace scripts/measure-rss.sh --bin ./target/release/shed -- delineate --dataset "$PUBLIC_ROOT" --lat "$LAT" --lon "$LON" --output "$REMOTE_OUTPUT" 2>"$trace_file")
    wrapper_exit_code=$?
    end=$(date +%s)

    exit_code=$(exit_code_from_measurement "$rss_json" "$wrapper_exit_code")
    peak_rss=$(peak_rss_from_measurement "$rss_json")
    seconds=$(wall_seconds "$start" "$end")
    trace_bytes=$(bytes_on_wire_from_trace "$trace_file")
    cog_header_bytes=$(trace_number_sum "$trace_file" "cog_header_bytes")
    cog_tile_bytes=$(trace_number_sum "$trace_file" "cog_tile_bytes")
    cog_tile_count=$(trace_number_sum "$trace_file" "cog_tile_count")
    cog_window_pixels=$(trace_number_sum "$trace_file" "window_pixels")
    bytes_on_wire=$trace_bytes
    non_cog_bytes_on_wire=$((bytes_on_wire - cog_header_bytes - cog_tile_bytes))
    if [[ "$non_cog_bytes_on_wire" -lt 0 ]]; then
      non_cog_bytes_on_wire=0
    fi
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
    "$adapter_version" \
    "$cog_header_bytes" \
    "$cog_tile_bytes" \
    "$cog_tile_count" \
    "$cog_window_pixels" \
    "$non_cog_bytes_on_wire"

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

  if [[ ${HFX_SMOKE_FAULT_INJECT:-} == "remote_ok_local_fail" ]]; then
    printf 'fault injection: simulating local failure without running local command\n' >&2
    printf 'HFX_SMOKE_FAULT_INJECT=remote_ok_local_fail simulated local failure\n' >"$stderr_log_file"
    exit_code=1
    peak_rss=67108864
    seconds=0.500
  elif [[ ${HFX_SMOKE_FAULT_INJECT:-} == "both_ok_remote_huge" ]]; then
    printf 'fault injection: simulating local ok without running local command\n' >&2
    printf 'HFX_SMOKE_FAULT_INJECT=both_ok_remote_huge simulated local ok\n' >"$stderr_log_file"
    exit_code=0
    peak_rss=100000000
    seconds=1.000
  else
    start=$(date +%s)
    rss_json=$(scripts/measure-rss.sh --bin ./target/release/shed -- delineate --dataset "$dataset_root" --lat "$LAT" --lon "$LON" --output "$LOCAL_OUTPUT" 2>"$stderr_log_file")
    wrapper_exit_code=$?
    end=$(date +%s)

    exit_code=$(exit_code_from_measurement "$rss_json" "$wrapper_exit_code")
    peak_rss=$(peak_rss_from_measurement "$rss_json")
    seconds=$(wall_seconds "$start" "$end")
  fi

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
    "$adapter_version" \
    0 \
    0 \
    0 \
    0 \
    0

  return 0
}

uses_fake_local_record() {
  case ${HFX_SMOKE_FAULT_INJECT:-} in
    remote_ok_local_fail|both_ok_remote_huge)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

cache_mode=${HFX_SMOKE_CACHE_MODE:-cold}
case "$cache_mode" in
  cold|warm)
    ;;
  *)
    emit_script_error "invalid HFX_SMOKE_CACHE_MODE" "value=$cache_mode expected=cold|warm"
    exit 2
    ;;
esac

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
raster_cache_dir="$cache_dir/raster-windows"
trace_file=$(mktemp "${TMPDIR:-/tmp}/phase4-smoke-r2-trace.XXXXXX")
local_stderr_file=$(mktemp "${TMPDIR:-/tmp}/phase4-smoke-r2-local-stderr.XXXXXX")
local_root=${LOCAL_HFX_ROOT:-$HOME/Desktop/merit-hfx/global/hfx}

printf 'published manifest: fabric_name=%s adapter_version=%s\n' "$fabric_name" "$adapter_version" >&2
if [[ "$cache_mode" == "cold" ]]; then
  printf 'purging remote cache subdirectory: %s\n' "$cache_dir" >&2
  rm -rf "$cache_dir"
  rm_status=$?
  if [[ "$rm_status" -eq 0 ]]; then
    cache_dir_purged=true
  else
    cache_dir_purged=false
    printf 'warning: failed to purge remote cache subdirectory: %s\n' "$cache_dir" >&2
  fi
else
  cache_dir_purged=false
  printf 'warm cache mode: preserving remote cache subdirectory: %s\n' "$cache_dir" >&2
fi

printf 'remote object_store trace and stderr: %s\n' "$trace_file" >&2
printf 'local stderr: %s\n' "$local_stderr_file" >&2
printf 'summary: bytes_on_wire comes from object_store Range traces; cog_* fields separate remote COG header/tile reads from manifest/graph/parquet reads.\n' >&2

emit_header "$fabric_name" "$adapter_version" "$cache_dir_purged" "$trace_file" "$cache_mode"

remote_record=$(run_remote "$trace_file" "$fabric_name" "$adapter_version" "$raster_cache_dir")
remote_status=$?
if [[ "$remote_status" -ne 0 || -z ${remote_record:-} ]]; then
  emit_script_error "failed to emit remote dataset_run record" "status=$remote_status"
  exit 2
fi
printf '%s\n' "$remote_record"

if ! uses_fake_local_record; then
  if [[ ! -d "$local_root" ]]; then
    emit_script_error "local HFX root is missing" "path=$local_root"
    exit 2
  fi
fi

local_record=$(run_local "$local_root" "$local_stderr_file" "$fabric_name" "$adapter_version")
local_status=$?
if [[ "$local_status" -ne 0 || -z ${local_record:-} ]]; then
  emit_script_error "failed to emit local dataset_run record" "status=$local_status"
  exit 2
fi
printf '%s\n' "$local_record"

summary_record=$(emit_summary "$remote_record" "$local_record" 2>/dev/null)
summary_status=$?
if [[ "$summary_status" -ne 0 || -z ${summary_record:-} ]]; then
  emit_script_error "summary JSON build failed" "status=$summary_status"
  exit 2
fi
printf '%s\n' "$summary_record"

printf '%s\n' "$summary_record" | jq -e '.comparison_ok == true' >/dev/null 2>&1
comparison_status=$?
if [[ "$comparison_status" -ne 0 ]]; then
  exit 2
fi

exit 0
