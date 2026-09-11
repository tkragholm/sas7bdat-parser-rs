#!/usr/bin/env bash
# Build the vendored-dependency tarball an R package needs to install on CRAN.
#
# CRAN build machines have no network, so a submitted package must carry every Rust
# dependency's source with it. This produces `src/rust/vendor.tar.xz`, which
# `src/Makevars` unpacks and builds against with `--offline`.
#
# Run this before `R CMD build`, not during it — vendoring needs the network, which
# is exactly what a CRAN build does not have. The tarball is deliberately not
# committed: it is a release artefact, regenerated from the committed lock.
#
# The tarball carries `Cargo.lock` alongside `vendor/`, so the lockfile and the
# vendored sources can never disagree. That matters because the lockfile in the
# repository is resolved *with* `src/.cargo/config.toml`'s `[patch.crates-io]`
# applied, so it records `sas7bdat` and `sas7bdat-convert` as path entries with no
# source. A tarball has no patch and must resolve the published crates instead.
#
# That resolution is committed, as `src/rust/vendor.lock`, and reused: the vendored
# set, and so `inst/AUTHORS`, then change only when someone asks. Before it was
# committed every run resolved afresh against crates.io, and the CI check that
# `inst/AUTHORS` is current went red whenever any transitive crate published a
# patch release, with nothing in the package changed. Pass `--refresh` to resolve
# again and rewrite the lock (after a core release, or to pick up dependency
# updates on purpose); a manifest that has moved past the committed lock fails
# with cargo's own message and the same advice.
set -euo pipefail

cd "$(dirname "$0")/.."
repo="$PWD"

pkg="${1:-}"
case "$pkg" in
  r-plugin | r-convert-plugin) ;;
  *)
    echo "usage: $0 <r-plugin|r-convert-plugin> [--no-verify] [--refresh]" >&2
    exit 2
    ;;
esac

verify=1
refresh=0
for flag in "${@:2}"; do
  case "$flag" in
    --no-verify) verify=0 ;;
    --refresh) refresh=1 ;;
    *) echo "unknown flag: $flag (--no-verify, --refresh)" >&2; exit 2 ;;
  esac
done

crate_dir="$repo/crates/$pkg/src/rust"
[ -f "$crate_dir/Cargo.toml" ] || { echo "no manifest at $crate_dir" >&2; exit 1; }
vendor_lock="$crate_dir/vendor.lock"

staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT

# Staged outside the repository on purpose: anywhere inside it, cargo would walk up
# and find `src/.cargo/config.toml`, re-apply the patch, and vendor this working tree
# instead of the published crates — the opposite of what a tarball needs.
cp "$crate_dir/Cargo.toml" "$staging/Cargo.toml"
mkdir -p "$staging/src"
cp -R "$crate_dir/src/." "$staging/src/"

cd "$staging"
if [ "$refresh" -eq 0 ] && [ -f "$vendor_lock" ]; then
  echo "==> vendoring $pkg from the committed src/rust/vendor.lock"
  cp "$vendor_lock" Cargo.lock
  if ! cargo vendor --locked vendor > /dev/null; then
    echo "  the committed vendor.lock no longer satisfies Cargo.toml;" >&2
    echo "  run: scripts/vendor-r-package.sh $pkg --refresh" >&2
    exit 1
  fi
else
  echo "==> resolving $pkg against crates.io (no [patch] applied) and vendoring"
  cargo vendor vendor > /dev/null
  cp Cargo.lock "$vendor_lock"
  echo "  wrote crates/$pkg/src/rust/vendor.lock; commit it with inst/AUTHORS"
fi

echo "==> pruning"
before=$(du -sm vendor | cut -f1)
python3 "$repo/scripts/prune-vendor.py" vendor
after=$(du -sm vendor | cut -f1)
echo "  vendor tree ${before} MiB -> ${after} MiB"

if [ "$verify" -eq 1 ]; then
  echo "==> verifying the pruned tree builds offline"
  # This is the step that catches a prune which removed something a build reads.
  # It is the whole reason the script is worth having over a one-liner.
  mkdir -p .cargo
  printf '[source.crates-io]\nreplace-with = "vendored-sources"\n\n[source.vendored-sources]\ndirectory = "vendor"\n' > .cargo/config.toml
  if ! cargo build --lib --release --offline > "$staging/build.log" 2>&1; then
    echo "  OFFLINE BUILD FAILED — the prune removed something the build needs:" >&2
    tail -20 "$staging/build.log" >&2
    exit 1
  fi
  echo "  ok"
  rm -rf .cargo target
fi

echo "==> writing inst/AUTHORS"
# Generated here rather than by hand so it can never describe a different dependency
# set from the one being packed two lines below. CRAN requires the copyright holders
# of bundled code to be identifiable from the package itself.
pkg_name=$(awk '/^Package: /{print $2; exit}' "$repo/crates/$pkg/DESCRIPTION")
mkdir -p "$repo/crates/$pkg/inst"
python3 "$repo/scripts/vendor-authors.py" vendor "$pkg_name" \
  > "$repo/crates/$pkg/inst/AUTHORS"
echo "  $(grep -c '^  Licence:' "$repo/crates/$pkg/inst/AUTHORS") crates listed"

echo "==> packing"
# `Cargo.lock` rides along so the offline build resolves exactly what was verified.
#
# COPYFILE_DISABLE is load-bearing on macOS. Every file here carries a
# `com.apple.provenance` xattr, and macOS's bsdtar stores xattrs as AppleDouble
# members — which its *own* `tar tf` then hides, because it merges them back on read.
# So the archive looks clean on the machine that made it, and unpacks on Linux with a
# `._`-prefixed junk file beside every real one: 4195 of them, last measured. That is
# not cosmetic. `zstd-sys`'s build script compiles every `.c` it finds in the zstd
# source tree, finds `._debug.c`, and dies on `stray '\5' in program`. A release cut
# from a Mac would fail on every CRAN build machine and pass every local test.
COPYFILE_DISABLE=1 tar cf - vendor Cargo.lock | xz -9e -T0 > "$crate_dir/vendor.tar.xz"

# Verified by reading the raw member names out of the stream rather than with
# `tar tf`, which is exactly the tool that cannot see them.
if xz -dc "$crate_dir/vendor.tar.xz" | grep -qa '/\._'; then
  echo "  ERROR: the archive contains AppleDouble members; it would break on Linux" >&2
  exit 1
fi

size=$(wc -c < "$crate_dir/vendor.tar.xz")
printf '==> wrote %s (%.2f MB)\n' "crates/$pkg/src/rust/vendor.tar.xz" "$(echo "$size" | awk '{print $1/1000000}')"
echo
echo "Next: R CMD build crates/$pkg"
