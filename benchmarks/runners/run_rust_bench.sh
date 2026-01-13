#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] <path-to-sas7bdat>" >&2
  exit 1
fi

BUILD_ONLY=false
if [[ "${1:-}" == "--build-only" ]]; then
  BUILD_ONLY=true
  shift
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
MANIFEST="${ROOT}/benchmarks/rust/rustbench/Cargo.toml"
TARGET_DIR="${ROOT}/benchmarks/.build/rustbench"
BIN="${TARGET_DIR}/release/sas7bdat-rustbench"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if [[ ! -x "${BIN}" ]]; then
  cargo build --quiet --release --manifest-path "${MANIFEST}" --target-dir "${TARGET_DIR}"
else
  cargo build --quiet --release --manifest-path "${MANIFEST}" --target-dir "${TARGET_DIR}"
fi

if [[ "${BUILD_ONLY}" == false ]]; then
  "${BIN}" "${FILE}"
fi
