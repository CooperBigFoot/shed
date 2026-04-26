#!/usr/bin/env bash
set -euo pipefail

PUBLIC_ROOT="https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/"
MANIFEST_URL="${PUBLIC_ROOT}manifest.json"
LAT="70.4521297483898"
LON="28.4906601273434"
REMOTE_OUTPUT="/tmp/phase4-smoke.geojson"
LOCAL_OUTPUT="${TMPDIR:-/tmp}/phase4-smoke-local.geojson"

json_string_field() {
  local key=$1
  awk -v key="$key" '
    {
      text = text $0 "\n"
    }
    END {
      pattern = "\"" key "\"[[:space:]]*:[[:space:]]*\"[^\"]*\""
      if (match(text, pattern)) {
        value = substr(text, RSTART, RLENGTH)
        sub("^\"" key "\"[[:space:]]*:[[:space:]]*\"", "", value)
        sub("\"$", "", value)
        print value
      }
    }
  '
}

json_number_or_null_field() {
  local key=$1
  sed -n "s/.*\"$key\":\\([^,}]*\\).*/\\1/p"
}

wall_seconds() {
  local start=$1
  local end=$2
  awk -v start="$start" -v end="$end" 'BEGIN { printf "%.3f", end - start }'
}

bytes_on_wire_from_trace() {
  local trace_file=$1
  awk '
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
  ' "$trace_file"
}

run_remote() {
  local trace_file=$1
  local start
  local end
  local rss_json
  local peak_rss
  local seconds
  local bytes_on_wire

  start=$(date +%s)
  rss_json=$(RUST_LOG=object_store=trace scripts/measure-rss.sh --bin ./target/release/shed -- delineate --dataset "$PUBLIC_ROOT" --lat "$LAT" --lon "$LON" --output "$REMOTE_OUTPUT" 2>"$trace_file")
  end=$(date +%s)

  peak_rss=$(printf '%s\n' "$rss_json" | json_number_or_null_field "max_rss_bytes")
  if [[ -z "$peak_rss" ]]; then
    peak_rss=null
  fi

  seconds=$(wall_seconds "$start" "$end")
  bytes_on_wire=$(bytes_on_wire_from_trace "$trace_file")

  printf '{"dataset":"remote-r2","bytes_on_wire":%s,"peak_rss_bytes":%s,"wall_seconds":%s,"outlet":[%s,%s]}\n' \
    "$bytes_on_wire" \
    "$peak_rss" \
    "$seconds" \
    "$LAT" \
    "$LON"
}

run_local() {
  local dataset_root=$1
  local start
  local end
  local rss_json
  local peak_rss
  local seconds

  start=$(date +%s)
  rss_json=$(scripts/measure-rss.sh --bin ./target/release/shed -- delineate --dataset "$dataset_root" --lat "$LAT" --lon "$LON" --output "$LOCAL_OUTPUT")
  end=$(date +%s)

  peak_rss=$(printf '%s\n' "$rss_json" | json_number_or_null_field "max_rss_bytes")
  if [[ -z "$peak_rss" ]]; then
    peak_rss=null
  fi

  seconds=$(wall_seconds "$start" "$end")

  printf '{"dataset":"local-disk","bytes_on_wire":0,"peak_rss_bytes":%s,"wall_seconds":%s,"outlet":[%s,%s]}\n' \
    "$peak_rss" \
    "$seconds" \
    "$LAT" \
    "$LON"
}

manifest=$(curl -fsSL "$MANIFEST_URL")
fabric_name=$(printf '%s\n' "$manifest" | json_string_field "fabric_name")
adapter_version=$(printf '%s\n' "$manifest" | json_string_field "adapter_version")

if [[ -z "$fabric_name" || -z "$adapter_version" ]]; then
  printf 'failed to parse fabric_name and adapter_version from %s\n' "$MANIFEST_URL" >&2
  exit 2
fi

cache_root=${HFX_CACHE_DIR:-$HOME/.cache/hfx}
cache_dir="${cache_root%/}/$fabric_name/$adapter_version"
trace_file=$(mktemp "${TMPDIR:-/tmp}/phase4-smoke-r2-trace.XXXXXX")
local_root=${LOCAL_HFX_ROOT:-$HOME/Desktop/merit-hfx/global/hfx}

printf 'published manifest: fabric_name=%s adapter_version=%s\n' "$fabric_name" "$adapter_version"
printf 'purging remote cache subdirectory: %s\n' "$cache_dir"
rm -rf "$cache_dir"

printf 'remote object_store trace: %s\n' "$trace_file"
printf 'summary: GDAL /vsis3/ raster reads bypass object_store and are not counted in bytes_on_wire.\n'

run_remote "$trace_file"
run_local "$local_root"
