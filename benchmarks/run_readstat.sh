#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
BIN="${ROOT}/benchmarks/readstat_bench"
SRC="${ROOT}/benchmarks/readstat_bench.c"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if [[ ! -x "${BIN}" || "${SRC}" -nt "${BIN}" ]]; then
  cc -O3 -Wall -Wextra "${SRC}" -I/usr/local/include -L/usr/local/lib -lreadstat -liconv -o "${BIN}"
fi

"${BIN}" "${FILE}"
