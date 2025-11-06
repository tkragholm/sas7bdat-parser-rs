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

# Ensure binaries are built
benchmarks/run_rust.sh "${FILE}" >/dev/null
benchmarks/run_csharp.sh "${FILE}" >/dev/null

hyperfine "$@" \
  "benchmarks/run_rust.sh ${FILE}" \
  "benchmarks/run_csharp.sh ${FILE}" \
  "benchmarks/run_readstat.sh ${FILE}"
