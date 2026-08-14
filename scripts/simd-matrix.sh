#!/usr/bin/env bash
# Sweep the SIMD backends across -C target-cpu levels and print one table.
#
# The axis that matters on x86-64 is target-cpu. `std::simd` has no runtime
# dispatch: it lowers to whatever target features were enabled at compile time, so
# a portable build executes 512-bit logical vectors as SSE2. `fearless_simd`
# detects AVX2/AVX-512 at runtime instead. The question this answers is how much of
# that gap `-C target-cpu` closes, and therefore whether the nightly `std::simd`
# backend still earns its place.
#
# Backends are compiled one at a time (they are mutually exclusive), so results are
# compared across runs. Same machine, same bench code, so that is sound.
#
# Usage: scripts/simd-matrix.sh [target-cpu ...]      default: baseline x86-64-v3 x86-64-v4
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

CPUS=("$@")
[ ${#CPUS[@]} -eq 0 ] && CPUS=(baseline x86-64-v3 x86-64-v4)

# --warm-up-time/--measurement-time keep a 3x3 sweep inside a runner's patience
# without making the samples meaningless.
CRIT_ARGS=(--warm-up-time 1 --measurement-time 3 --sample-size 20)

results=$(mktemp)
trap 'rm -f "$results"' EXIT

for cpu in "${CPUS[@]}"; do
  if [ "$cpu" = baseline ]; then
    export RUSTFLAGS=""
  else
    export RUSTFLAGS="-C target-cpu=$cpu"
  fi

  for backend in scalar fearless portable; do
    case "$backend" in
      scalar)   toolchain=""; features="internal-bench"; extra="--no-default-features" ;;
      fearless) toolchain=""; features="internal-bench"; extra="" ;;
      portable) toolchain="+nightly-2026-07-27"; features="internal-bench,nightly-simd"; extra="" ;;
    esac

    echo "::group::$cpu / $backend" >&2
    # shellcheck disable=SC2086
    out=$(cargo $toolchain bench -p sas7bdat --bench simd_backends \
            --features "$features" $extra -- "${CRIT_ARGS[@]}" 2>&1)
    status=$?
    echo "$out" >&2
    echo "::endgroup::" >&2

    if [ $status -ne 0 ]; then
      echo "FAILED: $cpu / $backend" >&2
      continue
    fi

    # Criterion prints `<backend>/<kernel>/kernel` on its own line, then a
    # `thrpt:` line holding [lower estimate upper]; keep the middle estimate.
    echo "$out" | awk -v cpu="$cpu" '
      /^(scalar|fearless|portable)\// { id = $1; next }
      /thrpt:/ && id != "" {
        for (i = 1; i <= NF; i++) {
          if ($i ~ /^\[/) { v = $(i+2); u = $(i+3); break }
        }
        n = split(id, seg, "/")
        backend = seg[1]
        kernel = seg[2]; for (j = 3; j < n; j++) kernel = kernel "/" seg[j]
        if (u ~ /^MiB/) v = v / 1024
        if (u ~ /^KiB/) v = v / 1048576
        printf "%s\t%s\t%s\t%.2f\n", cpu, kernel, backend, v
        id = ""
      }' >> "$results"
  done
done

echo
# Stamp the CPU into the table. GitHub's runner pool is heterogeneous — sweeps of
# this repo have landed on EPYC 7763 (no AVX-512), EPYC 9V74 and Xeon Platinum 8573C
# — and which one you get changes the answer. Only compare figures *within* one
# table; comparing a number here against a number from another run compares CPUs as
# much as code.
cpu=$(awk -F': ' '/^model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null \
      || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)
echo "=== throughput, GiB/s (higher is better) — $cpu ==="
awk -F'\t' '
  { key = $1 "|" $2; seen[key] = 1; val[key "|" $3] = $4; order[++n] = key }
  END {
    printf "%-12s %-26s %9s %9s %9s\n", "target-cpu", "kernel", "scalar", "fearless", "portable"
    for (i = 1; i <= n; i++) {
      key = order[i]
      if (done[key]++) continue
      split(key, k, "|")
      printf "%-12s %-26s %9s %9s %9s\n", k[1], k[2],
        (key "|scalar"   in val ? val[key "|scalar"]   : "-"),
        (key "|fearless" in val ? val[key "|fearless"] : "-"),
        (key "|portable" in val ? val[key "|portable"] : "-")
    }
  }' "$results"
