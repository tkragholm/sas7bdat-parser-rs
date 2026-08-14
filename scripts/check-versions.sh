#!/usr/bin/env bash
# Fail when a version requirement that must move in lockstep has drifted.
#
# The R bindings are separate crates outside the workspace. They depend on the core
# *by version*, so that a tarball built by `R CMD build` resolves it from crates.io,
# and `src/.cargo/config.toml` patches that back to this checkout for local work.
#
# The trap is that a `[patch]` only applies when its version satisfies the
# requirement. Bump `crates/sas7bdat` to 0.8.0 and leave a binding asking for "0.7"
# and cargo does not error — it prints `patch ... was not used`, a warning, and
# silently resolves the *published* 0.7.0 instead. The binding then builds against a
# core that is one release behind, or fails somewhere unrelated: what actually
# surfaced it in practice was a `--locked` lockfile mismatch, which points nowhere
# near the real cause.
#
# So this makes the coupling explicit and loud. It is deliberately a check rather
# than an automatic rewrite — see the note at the bottom of this file.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

status=0

# The `major.minor` of a workspace crate, which is what a caret requirement on a 0.x
# crate must match: `^0.7` does not accept 0.8.0.
crate_version() {
  awk -F'"' '/^version = /{print $2; exit}' "crates/$1/Cargo.toml"
}

# The requirement a binding manifest states for a dependency.
required_version() {
  awk -F'"' -v dep="$2" '
    $0 ~ "^" dep " = " {
      for (i = 1; i <= NF; i++) if ($(i-1) ~ /version = $/) { print $i; exit }
    }' "$1"
}

check() {
  local manifest="$1" dep="$2" crate="$3"
  local want req
  want="$(crate_version "$crate")"
  want="${want%.*}"                      # 0.8.0 -> 0.8
  req="$(required_version "$manifest" "$dep")"

  if [ -z "$req" ]; then
    echo "  ?  $manifest: no version requirement found for '$dep'" >&2
    status=1
    return
  fi

  if [ "$req" != "$want" ]; then
    echo "  ✗  $manifest wants $dep \"$req\", but crates/$crate is $(crate_version "$crate") (needs \"$want\")" >&2
    status=1
  else
    printf '  ok %-46s %s = "%s"\n' "$manifest" "$dep" "$req"
  fi
}

echo "R binding version requirements vs the workspace crates:"
check crates/r-plugin/src/rust/Cargo.toml         sas7bdat         sas7bdat
check crates/r-convert-plugin/src/rust/Cargo.toml sas7bdat         sas7bdat
check crates/r-convert-plugin/src/rust/Cargo.toml sas7bdat-convert sas7bdat-convert
# sas7bdat-convert depends on the core by path *and* version; the version half is
# what a published sas7bdat-convert resolves, so it drifts the same way.
check crates/sas7bdat-convert/Cargo.toml          sas7bdat         sas7bdat

# The Python packages state their version twice: in Cargo.toml, which maturin builds
# from, and in pyproject.toml, which is what actually lands on PyPI. Nothing forces
# them to agree, and a mismatch means the wheel is built from one version and labelled
# with the other. `wheels.yml`'s tag check reads Cargo.toml, so a drifted pyproject
# would sail past it too.
echo
echo "Python package versions, Cargo.toml vs pyproject.toml:"
for dir in polars-plugin sas7bdat-cli; do
  cargo_v="$(awk -F'"' '/^version = /{print $2; exit}' "crates/$dir/Cargo.toml")"
  py_v="$(awk -F'"' '/^version = /{print $2; exit}' "crates/$dir/pyproject.toml")"
  if [ "$cargo_v" != "$py_v" ]; then
    echo "  ✗  crates/$dir: Cargo.toml is $cargo_v but pyproject.toml is $py_v" >&2
    status=1
  else
    printf '  ok %-46s %s\n' "crates/$dir" "$cargo_v"
  fi
done

if [ $status -ne 0 ]; then
  cat >&2 <<'MSG'

Bump the requirement(s) above, then regenerate the affected lockfiles from the
directory the build actually uses, so `src/.cargo`'s redirect applies:

    cd crates/r-plugin/src         && cargo check --manifest-path rust/Cargo.toml
    cd crates/r-convert-plugin/src && cargo check --manifest-path rust/Cargo.toml

Note this is checked, not fixed automatically, on purpose. Bumping a binding to a
core version that is not on crates.io yet breaks R-universe, which builds from a
tarball where the local patch is stripped — so the bump has to be sequenced against
the publish by a human who knows which half has landed.
MSG
fi

exit $status
