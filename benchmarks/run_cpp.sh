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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
SOURCE_DIR="${ROOT}/benchmarks/cppbench"
BUILD_DIR="${ROOT}/benchmarks/.build/cppbench"
BIN="${BUILD_DIR}/cpp_bench"
CMAKE_CACHE="${BUILD_DIR}/CMakeCache.txt"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if ! command -v cmake >/dev/null 2>&1; then
  echo "CMake not found. Install CMake to build the C++ benchmark." >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"

configure_needed=false
if [[ ! -f "${CMAKE_CACHE}" ]]; then
  configure_needed=true
elif [[ "${SOURCE_DIR}/CMakeLists.txt" -nt "${CMAKE_CACHE}" ]]; then
  configure_needed=true
elif find "${SOURCE_DIR}" "${ROOT}/benchmarks/lib/cpp" \( -name 'CMakeLists.txt' -o -name '*.cmake' \) -newer "${CMAKE_CACHE}" -print -quit | grep -q .; then
  configure_needed=true
fi

if [[ "${configure_needed}" == true ]]; then
  cmake -S "${SOURCE_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE=Release >/dev/null
fi

cmake --build "${BUILD_DIR}" --config Release >/dev/null

if [[ "${BUILD_ONLY}" == true ]]; then
  exit 0
fi

"${BIN}" "${FILE}"
