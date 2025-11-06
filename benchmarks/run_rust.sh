#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] <path-to-sas7bdat>" >&2
  exit 1
fi

BUILD_ONLY=false
if [[ "$1" == "--build-only" ]]; then
  BUILD_ONLY=true
  shift
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
BIN="${ROOT}/target/release/examples/benchmark"
FEATURE_FLAGS=()
needs_build=false
STAMP_FILE="${ROOT}/target/benchmark_features.txt"

if [[ -n "${BENCH_PARALLEL_ROWS:-}" || -n "${BENCH_COLUMNAR:-}" ]]; then
  FEATURE_FLAGS+=(--features parallel-rows)
  needs_build=true
fi

if [[ -n "${BENCH_HOTPATH:-}" ]]; then
  FEATURE_FLAGS+=(--features hotpath)
  needs_build=true
fi

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if [[ ! -x "${BIN}" ]]; then
  needs_build=true
elif [[ "${ROOT}/examples/benchmark.rs" -nt "${BIN}" ]]; then
  needs_build=true
elif find "${ROOT}/src" -name '*.rs' -newer "${BIN}" -print -quit | grep -q .; then
  needs_build=true
fi

current_features="none"
if (( ${#FEATURE_FLAGS[@]} > 0 )); then
  current_features="${FEATURE_FLAGS[*]}"
fi

if [[ -f "${STAMP_FILE}" ]]; then
  stamp_contents="$(<"${STAMP_FILE}")"
else
  stamp_contents=""
fi

if [[ "${stamp_contents}" != "${current_features}" ]]; then
  needs_build=true
fi

if [[ "${needs_build}" == true ]]; then
  if (( ${#FEATURE_FLAGS[@]} > 0 )); then
    cargo build --quiet --release --example benchmark "${FEATURE_FLAGS[@]}" >/dev/null
  else
    cargo build --quiet --release --example benchmark >/dev/null
  fi
  printf '%s' "${current_features}" > "${STAMP_FILE}"
fi

if [[ "${BUILD_ONLY}" == false ]]; then
  if [[ -n "${BENCH_HOTPATH:-}" ]]; then
    mkdir -p "${ROOT}/target/hotpath"
    HOTPATH_OUT="${ROOT}/target/hotpath" "${BIN}" "${FILE}"
  else
    "${BIN}" "${FILE}"
  fi
fi
