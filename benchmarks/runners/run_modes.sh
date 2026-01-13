#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-sas7bdat> [-- additional hyperfine args]" >&2
  exit 1
fi

INPUT="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
if [[ ! -f "${INPUT}" ]]; then
  echo "Input file not found: ${INPUT}" >&2
  exit 1
fi
shift || true

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

HYPERFINE_ARGS=("$@")

prepare_cmd=$(
  cat <<EOF
${ROOT}/benchmarks/runners/run_rust.sh --build-only "${INPUT}"
EOF
)

hyperfine \
  --prepare "${prepare_cmd}" \
  "${HYPERFINE_ARGS[@]}" \
  "${ROOT}/benchmarks/runners/run_rust.sh \"${INPUT}\""
