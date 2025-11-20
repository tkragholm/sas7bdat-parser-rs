#!/usr/bin/env bash
set -euo pipefail

if ! command -v hyperfine >/dev/null 2>&1; then
    echo "error: hyperfine is required (brew install hyperfine)" >&2
    exit 1
fi

DATASET=${1:-ahs2013n.sas7bdat}
if [ ! -f "$DATASET" ]; then
    echo "error: dataset '$DATASET' not found" >&2
    exit 1
fi

RUNS=${RUNS:-3}
WARMUP=${WARMUP:-1}
ROW_GROUP_SIZE=${ROW_GROUP_SIZE:-65536}
BENCH_OUT_DIR=${BENCH_OUT_DIR:-/tmp/sas7bd-columnar-bench}
KEEP_OUTPUTS=${KEEP_OUTPUTS:-0}
JOBS=${JOBS:-}
mkdir -p "$BENCH_OUT_DIR"

cmd_to_string() {
    local quoted=()
    for arg in "$@"; do
        quoted+=("$(printf '%q' "$arg")")
    done
    local IFS=' '
    printf "%s" "${quoted[*]}"
}

CURRENT_FEATURE_FLAG=""
build_command() {
    local output=$1
    shift
    local extra_flags=("$@")

    local remove_cmd
    remove_cmd=$(printf "rm -f %q" "$output")

    local cargo_cmd=(cargo run --release)
    if [ -n "$CURRENT_FEATURE_FLAG" ]; then
        cargo_cmd+=(--features "$CURRENT_FEATURE_FLAG")
    fi
    cargo_cmd+=(--bin sas7bd -- convert --out "$output" "$DATASET" --parquet-row-group-size "$ROW_GROUP_SIZE" --columnar)
    if [ -n "$JOBS" ]; then
        cargo_cmd+=(--jobs "$JOBS")
    fi
    if [ "${#extra_flags[@]}" -gt 0 ]; then
        cargo_cmd+=("${extra_flags[@]}")
    fi

    local cargo_str
    cargo_str=$(cmd_to_string "${cargo_cmd[@]}")
    printf "%s && %s" "$remove_cmd" "$cargo_str"
}

cleanup_outputs() {
    if [ "$KEEP_OUTPUTS" != "1" ]; then
        rm -f "$@"
    fi
}

bench_suite() {
    local suite_name=$1
    local feature_flag=$2
    local suite_desc=$3

    CURRENT_FEATURE_FLAG="$feature_flag"
    local out_columnar="$BENCH_OUT_DIR/${suite_name}_columnar.parquet"

    local cmd_columnar
    cmd_columnar=$(build_command "$out_columnar")

    echo "==> $suite_desc (dataset: $DATASET)"
    hyperfine --warmup "$WARMUP" --runs "$RUNS" "$cmd_columnar"
    cleanup_outputs "$out_columnar"
}

bench_suite "baseline" "" "Columnar contiguous"
bench_suite "hotpath" "hotpath" "Columnar contiguous (hotpath instrumented)"
