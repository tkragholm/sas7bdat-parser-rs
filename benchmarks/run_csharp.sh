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
PROJECT_DIR="${ROOT}/benchmarks/SasBenchmarks"
PROJECT_FILE="${PROJECT_DIR}/SasBenchmarks.csproj"
LIB_PROJECT_DIR="${ROOT}/benchmarks/lib/csharp/Sas7Bdat.Core"
LIB_PROJECT_FILE="${LIB_PROJECT_DIR}/Sas7Bdat.Core.csproj"
CONFIG_FILE="${ROOT}/benchmarks/NuGet.Config"
realpath_py() {
  python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1"
}
CACHE_DIR="$(realpath_py "${PROJECT_DIR}/../.dotnet-cli-cache")"
NUGET_DIR="$(realpath_py "${PROJECT_DIR}/../.nuget")"
ASSETS_FILE="${PROJECT_DIR}/obj/project.assets.json"
FRAMEWORK="net9.0"
CONFIGURATION="Release"
OUTPUT_DLL="${PROJECT_DIR}/bin/${CONFIGURATION}/${FRAMEWORK}/SasBenchmarks.dll"

if [[ ! -f "${FILE}" ]]; then
  echo "Input file not found: ${FILE}" >&2
  exit 1
fi

if ! command -v dotnet >/dev/null 2>&1; then
  echo "dotnet CLI not found in PATH. Install the .NET SDK 9.0 or newer." >&2
  exit 1
fi

mkdir -p "${CACHE_DIR}"
mkdir -p "${NUGET_DIR}/packages"

run_dotnet() {
  DOTNET_CLI_HOME="${CACHE_DIR}" \
  NUGET_PACKAGES="${NUGET_DIR}/packages" \
  dotnet "$@"
}

needs_restore=false
if [[ ! -f "${ASSETS_FILE}" ]]; then
  needs_restore=true
elif [[ "${PROJECT_FILE}" -nt "${ASSETS_FILE}" ]]; then
  needs_restore=true
elif [[ -f "${LIB_PROJECT_FILE}" && "${LIB_PROJECT_FILE}" -nt "${ASSETS_FILE}" ]]; then
  needs_restore=true
elif [[ -f "${CONFIG_FILE}" && "${CONFIG_FILE}" -nt "${ASSETS_FILE}" ]]; then
  needs_restore=true
elif find "${PROJECT_DIR}" -maxdepth 1 -name '*.csproj' -newer "${ASSETS_FILE}" -print -quit | grep -q .; then
  needs_restore=true
elif [[ -d "${LIB_PROJECT_DIR}" ]] && find "${LIB_PROJECT_DIR}" -maxdepth 1 -name '*.csproj' -newer "${ASSETS_FILE}" -print -quit | grep -q .; then
  needs_restore=true
fi

needs_build=false
if [[ ! -f "${OUTPUT_DLL}" ]]; then
  needs_build=true
elif find "${PROJECT_DIR}" -maxdepth 1 -name '*.cs' -newer "${OUTPUT_DLL}" -print -quit | grep -q .; then
  needs_build=true
elif [[ -d "${LIB_PROJECT_DIR}" ]] && find "${LIB_PROJECT_DIR}" -name '*.cs' -newer "${OUTPUT_DLL}" -print -quit | grep -q .; then
  needs_build=true
elif [[ "${PROJECT_FILE}" -nt "${OUTPUT_DLL}" ]]; then
  needs_build=true
fi

if [[ "${needs_build}" == true ]]; then
  needs_restore=true
fi

if [[ "${needs_restore}" == true ]]; then
  restore_args=(restore --nologo "${PROJECT_FILE}")
  if [[ -f "${CONFIG_FILE}" ]]; then
    restore_args+=(--configfile "${CONFIG_FILE}")
  fi
  run_dotnet "${restore_args[@]}" >/dev/null
fi

if [[ "${needs_build}" == true ]]; then
  run_dotnet build --nologo --verbosity quiet --no-restore "${PROJECT_FILE}" \
    -c "${CONFIGURATION}" >/dev/null
fi

if [[ "${BUILD_ONLY}" == false ]]; then
  run_dotnet exec "${OUTPUT_DLL}" "${FILE}"
fi
