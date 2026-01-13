#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--build-only] [--print-path] [--rebuild] [readstat args...]" >&2
  exit 1
}

BUILD_ONLY=false
PRINT_PATH=false
REBUILD=false
if [[ "${1:-}" == "--build-only" ]]; then
  BUILD_ONLY=true
  shift
fi
if [[ "${1:-}" == "--print-path" ]]; then
  PRINT_PATH=true
  shift
fi
if [[ "${1:-}" == "--rebuild" ]]; then
  REBUILD=true
  shift
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC_DIR="${ROOT}/benchmarks/lib/c"
BUILD_DIR="${ROOT}/benchmarks/.build/readstat-cli"
INSTALL_DIR="${BUILD_DIR}/install"
BIN="${INSTALL_DIR}/bin/readstat"

if [[ ! -d "${SRC_DIR}" ]]; then
  echo "ReadStat submodule not found at ${SRC_DIR}" >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"

if [[ ! -f "${SRC_DIR}/configure" ]]; then
  if [[ ! -x "${SRC_DIR}/autogen.sh" ]]; then
    echo "autogen.sh not found or not executable in ${SRC_DIR}" >&2
    exit 1
  fi
  (cd "${SRC_DIR}" && ./autogen.sh >/dev/null)
fi

if [[ ! -f "${BUILD_DIR}/Makefile" || "${SRC_DIR}/configure" -nt "${BUILD_DIR}/Makefile" ]]; then
  (cd "${BUILD_DIR}" && "${SRC_DIR}/configure" --prefix="${INSTALL_DIR}" >/dev/null)
fi

make -C "${BUILD_DIR}" >/dev/null
if [[ "${REBUILD}" == true ]]; then
  make -C "${BUILD_DIR}" clean >/dev/null
  make -C "${BUILD_DIR}" >/dev/null
fi
make -C "${BUILD_DIR}" install >/dev/null

if [[ "${PRINT_PATH}" == true ]]; then
  echo "${BIN}"
  exit 0
fi

if [[ "${BUILD_ONLY}" == true ]]; then
  exit 0
fi

if [[ $# -eq 0 ]]; then
  usage
fi

exec "${BIN}" "$@"
