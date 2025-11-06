#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] <path-to-sas7bdat>" >&2
  exit 1
fi

BUILD_ONLY=false
if [[ "$1" == "--build-only" ]]; then
  BUILD_ONLY=true
  shift
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--build-only] <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"
BIN="${ROOT}/target/release/examples/benchmark"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

needs_build=false
if [[ ! -x "${BIN}" ]]; then
  needs_build=true
elif [[ "${ROOT}/examples/benchmark.rs" -nt "${BIN}" ]]; then
  needs_build=true
elif find "${ROOT}/src" -name '*.rs' -newer "${BIN}" -print -quit | grep -q .; then
  needs_build=true
fi

if [[ "${needs_build}" == true ]]; then
  cargo build --quiet --release --example benchmark >/dev/null
fi

if [[ "${BUILD_ONLY}" == false ]]; then
  "${BIN}" "${FILE}"
fi
