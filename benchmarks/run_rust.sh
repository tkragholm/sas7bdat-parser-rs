#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
echo "Usage: $0 [--build-only] <path-to-sas7bdat> [benchmark-args...]" >&2
  exit 1
fi

BUILD_ONLY=false
if [[ "$1" == "--build-only" ]]; then
  BUILD_ONLY=true
  shift
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] <path-to-sas7bdat> [benchmark-args...]" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
shift
CLI_ARGS=("$@")
BIN="${ROOT}/target/release/sas7bd"
FEATURE_FLAGS=()
needs_build=false
STAMP_FILE="${ROOT}/target/benchmark_features.txt"

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
elif [[ "${ROOT}/src/bin/sas7bd.rs" -nt "${BIN}" ]]; then
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
    cargo build --quiet --release --bin sas7bd "${FEATURE_FLAGS[@]}" >/dev/null
  else
    cargo build --quiet --release --bin sas7bd >/dev/null
  fi
  printf '%s' "${current_features}" > "${STAMP_FILE}"
fi

if [[ "${BUILD_ONLY}" == false ]]; then
  if [[ -n "${BENCH_HOTPATH:-}" ]]; then
    mkdir -p "${ROOT}/target/hotpath"
  fi
  if [[ -n "${BENCH_OUTPUT:-}" ]]; then
    OUT_FILE="${BENCH_OUTPUT}"
    CLEANUP=false
  else
    OUT_FILE="$(mktemp "${ROOT}/target/bench-rust-XXXXXX.parquet")"
    CLEANUP=true
  fi
  cmd=("${BIN}" "convert" "--out" "${OUT_FILE}" "${FILE}")
  if (( ${#CLI_ARGS[@]} > 0 )); then
    cmd+=("${CLI_ARGS[@]}")
  fi
  if [[ -n "${BENCH_HOTPATH:-}" ]]; then
    HOTPATH_OUT="${ROOT}/target/hotpath" "${cmd[@]}"
  else
    "${cmd[@]}"
  fi
  if [[ "${CLEANUP}" == true ]]; then
    rm -f "${OUT_FILE}"
  fi
fi
