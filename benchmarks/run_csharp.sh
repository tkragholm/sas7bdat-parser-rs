#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-sas7bdat>" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="${ROOT}/benchmarks/SasBenchmarks"
FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

mkdir -p "${PROJECT_DIR}/../.dotnet-cli-cache"

run_dotnet() {
  DOTNET_CLI_HOME="${PROJECT_DIR}/../.dotnet-cli-cache" \
  DOTNET_ROOT="${PROJECT_DIR}/../.dotnet-cli-cache" \
  dotnet "$@"
}

if [[ ! -f "${PROJECT_DIR}/bin/Debug/net9.0/SasBenchmarks.dll" ]]; then
  run_dotnet build --nologo --verbosity quiet "${PROJECT_DIR}/SasBenchmarks.csproj" >/dev/null
fi

run_dotnet run --no-build --no-restore --project "${PROJECT_DIR}/SasBenchmarks.csproj" -- "${FILE}"
