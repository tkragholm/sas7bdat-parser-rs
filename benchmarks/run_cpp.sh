#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
SOURCE_DIR="${ROOT}/benchmarks/cppbench"
BUILD_DIR="${ROOT}/benchmarks/.build/cppbench"
BIN="${BUILD_DIR}/cpp_bench"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if ! command -v cmake >/dev/null 2>&1; then
  echo "CMake not found. Install CMake to build the C++ benchmark." >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"

cmake -S "${SOURCE_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "${BUILD_DIR}" --config Release >/dev/null

"${BIN}" "${FILE}"
