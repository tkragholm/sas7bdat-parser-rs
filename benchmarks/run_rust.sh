#!/usr/bin/env bash
set -euo pipefail

if [[ $# < 1 ]]; then
  echo "Usage: $0 <path-to-sas7bdat>" >&2
  exit 1
fi

FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

cargo run --quiet --release --example benchmark -- "${FILE}"
