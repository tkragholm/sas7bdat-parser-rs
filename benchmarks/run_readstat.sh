#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
BIN="${ROOT}/benchmarks/.build/readstat_bench"
SRC="${ROOT}/benchmarks/readstat_bench.c"
READSTAT_SRC="${ROOT}/read-stat-src"
BUILD_DIR="$(dirname "${BIN}")"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if [[ ! -d "${READSTAT_SRC}" ]]; then
  echo "Vendored ReadStat sources not found at ${READSTAT_SRC}" >&2
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

"${BIN}" "${FILE}"
