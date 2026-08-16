#!/usr/bin/env python3
"""Drop files a build never reads from a `cargo vendor` directory.

CRAN builds offline, so the R packages ship their Rust dependencies as vendored
source. That tarball is the largest thing in the package by an order of magnitude,
and roughly a fifth of it is test fixtures, benchmark corpora and repository
scaffolding that no build ever opens — `csv/examples/data/bench/` alone is 6 MB of
sample CSV.

Cargo verifies a vendored crate against `.cargo-checksum.json`, and only against the
files that manifest lists. So a file can be removed as long as its entry goes with
it. That is the whole mechanism.

Two things about it are easy to get wrong, and both produce a tree that builds on the
maintainer's machine and fails on CRAN:

  * **Delete only what the manifest names.** Matching filenames case-insensitively
    against a wanted-list and unlinking what you find breaks on macOS, where
    `bytemuck/changelog.md` answers to `CHANGELOG.md`: the file disappears and the
    manifest keeps its real, differently-cased key. Cargo then fails with
    `failed to calculate checksum`. Every deletion here is driven by a manifest key.

  * **Documentation can be source.** `getrandom` has
    `#![doc = include_str!("../README.md")]`, so removing its README is a compile
    error, not a saving. READMEs and changelogs stay. This costs about 0.9 MB and is
    not negotiable without checking every crate by hand.

Licence files are never touched — they have to ship.
"""

from __future__ import annotations

import json
import os
import shutil
import sys

# Top-level directories within a crate that a `cargo build` never reads. Anything
# a build script might compile (`src`, `zstd`, `wasm-shim`, ...) is absent by design.
DROP_DIRS = frozenset(
    {"tests", "benches", "examples", "fuzz", ".github", "ci"}
)

# Inert files, matched case-insensitively against the *manifest key's* basename.
# Note the absence of readme.md and changelog.md: see the module docstring.
#
# The last two entries are here because `R CMD check` complains about them rather
# than because of their size:
#
#   citation.cff  -> NOTE "Found the following CITATION file in a non-standard
#                    place: src/rust/vendor/chrono/CITATION.cff"
#   makefile      -> WARNING "Found the following file(s) containing GNU
#                    extensions", from `r-efi`'s Makefile. Cargo never runs these;
#                    a crate that genuinely drove `make` from a build script would
#                    fail the offline build that `vendor-r-package.sh` runs after
#                    pruning, which is that step's job to catch.
DROP_FILES = frozenset(
    {
        "cargo.toml.orig",
        ".cargo_vcs_info.json",
        "cargo.lock",
        "rustfmt.toml",
        ".rustfmt.toml",
        "clippy.toml",
        ".clippy.toml",
        "issue_template.md",
        "contributing.md",
        "code_of_conduct.md",
        ".gitignore",
        ".editorconfig",
        ".travis.yml",
        "appveyor.yml",
        "citation.cff",
        "makefile",
    }
)


def prune(root: str) -> tuple[int, int]:
    """Prune every crate under `root`. Returns (bytes removed, crates touched)."""
    removed = 0
    touched = 0
    for crate in sorted(os.listdir(root)):
        crate_dir = os.path.join(root, crate)
        manifest = os.path.join(crate_dir, ".cargo-checksum.json")
        if not os.path.isfile(manifest):
            continue

        with open(manifest, encoding="utf-8") as handle:
            meta = json.load(handle)
        files = meta.get("files", {})
        before = len(files)

        for rel in list(files):
            head = rel.split("/")[0]
            base = rel.split("/")[-1].lower()
            if head in DROP_DIRS or base in DROP_FILES:
                path = os.path.join(crate_dir, rel)
                if os.path.isfile(path):
                    removed += os.path.getsize(path)
                    os.remove(path)
                del files[rel]

        # Sweep the now-unlisted directories. A crate may ship files inside these
        # that the manifest never listed; they are equally unread, and leaving empty
        # trees behind would be untidy rather than wrong.
        for name in DROP_DIRS:
            path = os.path.join(crate_dir, name)
            if os.path.isdir(path):
                for dirpath, _, filenames in os.walk(path):
                    for filename in filenames:
                        full = os.path.join(dirpath, filename)
                        if os.path.isfile(full):
                            removed += os.path.getsize(full)
                shutil.rmtree(path)

        if len(files) != before:
            touched += 1
        meta["files"] = files
        with open(manifest, "w", encoding="utf-8") as handle:
            json.dump(meta, handle)

    return removed, touched


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <vendor-directory>", file=sys.stderr)
        return 2
    root = sys.argv[1]
    if not os.path.isdir(root):
        print(f"not a directory: {root}", file=sys.stderr)
        return 1
    removed, touched = prune(root)
    print(f"  pruned {removed / 1048576:.1f} MiB from {touched} crates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
