#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] [--csv-output <path>] <path-to-sas7bdat>" >&2
  exit 1
fi

BUILD_ONLY=false
CSV_OUTPUT=""
if [[ "${1:-}" == "--build-only" ]]; then
  BUILD_ONLY=true
  shift
fi

if [[ "${1:-}" == "--csv-output" ]]; then
  if [[ $# -lt 3 ]]; then
    echo "Usage: $0 [--build-only] [--csv-output <path>] <path-to-sas7bdat>" >&2
    exit 1
  fi
  CSV_OUTPUT="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$2")"
  shift 2
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] [--csv-output <path>] <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
SOURCE_DIR="${ROOT}/benchmarks/cpp/cppbench"
BUILD_DIR="${ROOT}/benchmarks/.build/cppbench"
BIN="${BUILD_DIR}/cpp_bench"
CMAKE_CACHE="${BUILD_DIR}/CMakeCache.txt"
CMAKE_LOG="${BUILD_DIR}/cmake-configure.log"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if ! command -v cmake >/dev/null 2>&1; then
  echo "CMake not found. Install CMake to build the C++ benchmark." >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"

cmake_args=("-DCMAKE_BUILD_TYPE=Release")
if [[ -n "${Boost_DIR:-}" ]]; then
  cmake_args+=("-DBoost_DIR=${Boost_DIR}")
fi
if [[ -n "${BOOST_ROOT:-}" ]]; then
  cmake_args+=("-DBoost_ROOT=${BOOST_ROOT}")
fi
if [[ -n "${Boost_ROOT:-}" ]]; then
  cmake_args+=("-DBoost_ROOT=${Boost_ROOT}")
fi
if [[ -n "${CMAKE_PREFIX_PATH:-}" ]]; then
  cmake_args+=("-DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")
fi
if [[ -n "${CMAKE_ARGS:-}" ]]; then
  # Allow custom overrides like -DBoost_NO_SYSTEM_PATHS=ON.
  read -r -a extra_cmake_args <<< "${CMAKE_ARGS}"
  cmake_args+=("${extra_cmake_args[@]}")
fi

configure_needed=false
if [[ ! -f "${CMAKE_CACHE}" ]]; then
  configure_needed=true
elif [[ "${SOURCE_DIR}/CMakeLists.txt" -nt "${CMAKE_CACHE}" ]]; then
  configure_needed=true
elif find "${SOURCE_DIR}" "${ROOT}/benchmarks/lib/cpp" \( -name 'CMakeLists.txt' -o -name '*.cmake' \) -newer "${CMAKE_CACHE}" -print -quit | grep -q .; then
  configure_needed=true
fi

if [[ "${configure_needed}" == true ]]; then
  if ! cmake -S "${SOURCE_DIR}" -B "${BUILD_DIR}" "${cmake_args[@]}" >"${CMAKE_LOG}" 2>&1; then
    cat "${CMAKE_LOG}" >&2
    exit 1
  fi
fi

cmake --build "${BUILD_DIR}" --config Release >/dev/null

if [[ "${BUILD_ONLY}" == true ]]; then
  exit 0
fi

if [[ -n "${CSV_OUTPUT}" ]]; then
  "${BIN}" --csv "${CSV_OUTPUT}" "${FILE}"
else
  "${BIN}" "${FILE}"
fi
