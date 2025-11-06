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
BIN="${ROOT}/benchmarks/.build/readstat_bench"
SRC="${ROOT}/benchmarks/readstat_bench.c"
READSTAT_ROOT="${ROOT}/benchmarks/lib/c"
READSTAT_SRC="${READSTAT_ROOT}/src"
BUILD_DIR="$(dirname "${BIN}")"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if [[ ! -d "${READSTAT_ROOT}" ]]; then
  echo "Vendored ReadStat sources not found at ${READSTAT_ROOT}" >&2
  exit 1
fi

if ! command -v cc >/dev/null 2>&1; then
  echo "C compiler 'cc' not found in PATH." >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"

# Determine whether a rebuild is required
needs_build=false
if [[ ! -x "${BIN}" ]]; then
  needs_build=true
elif [[ "${SRC}" -nt "${BIN}" ]]; then
  needs_build=true
elif find "${READSTAT_SRC}" -type f -name '*.c' -newer "${BIN}" -print -quit | grep -q .; then
  needs_build=true
elif find "${READSTAT_SRC}" -type f -name '*.h' -newer "${BIN}" -print -quit | grep -q .; then
  needs_build=true
fi

if [[ "${needs_build}" == true ]]; then
  CFLAGS=(-O3 -std=c11 -Wall -Wextra -pedantic -I"${READSTAT_SRC}" -I"${READSTAT_SRC}/sas")
  LIBS=(-lm)
  case "$(uname -s)" in
    Darwin*) LIBS+=(-liconv) ;;
  esac
  SOURCES=(
    "${SRC}"
    "${READSTAT_SRC}/CKHashTable.c"
    "${READSTAT_SRC}/readstat_bits.c"
    "${READSTAT_SRC}/readstat_convert.c"
    "${READSTAT_SRC}/readstat_error.c"
    "${READSTAT_SRC}/readstat_io_unistd.c"
    "${READSTAT_SRC}/readstat_malloc.c"
    "${READSTAT_SRC}/readstat_metadata.c"
    "${READSTAT_SRC}/readstat_parser.c"
    "${READSTAT_SRC}/readstat_value.c"
    "${READSTAT_SRC}/readstat_variable.c"
    "${READSTAT_SRC}/readstat_writer.c"
    "${READSTAT_SRC}/sas/ieee.c"
    "${READSTAT_SRC}/sas/readstat_sas.c"
    "${READSTAT_SRC}/sas/readstat_sas_rle.c"
    "${READSTAT_SRC}/sas/readstat_sas7bdat_read.c"
  )

  cc "${CFLAGS[@]}" "${SOURCES[@]}" -o "${BIN}" "${LIBS[@]}"
fi

if [[ "${BUILD_ONLY}" == false ]]; then
  "${BIN}" "${FILE}"
fi
