#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-sas7bdat> [--additional hyperfine args...]" >&2
  exit 1
fi

FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
shift

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

PREP_CMD="benchmarks/run_rust.sh --build-only \"${FILE}\" && benchmarks/run_cpp.sh --build-only \"${FILE}\" && benchmarks/run_readstat.sh --build-only \"${FILE}\""

hyperfine \
  --prepare "${PREP_CMD}" \
  "$@" \
  "benchmarks/run_rust.sh \"${FILE}\"" \
  "benchmarks/run_readstat.sh \"${FILE}\"" \
  "benchmarks/run_cpp.sh \"${FILE}\""
