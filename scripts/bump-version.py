#!/usr/bin/env python3
"""Bump one crate's version and bring everything that must move with it.

A version bump in this repository is never a one-file edit, and the parts that get
missed are the ones nothing local complains about:

  * The Python packages state their version twice -- `Cargo.toml`, which maturin
    builds from, and `pyproject.toml`, which is what lands on PyPI. `wheels.yml`
    reads the first, so a drifted second sails past its tag check.
  * Three lockfiles pin these crates, and two of them sit outside the workspace.
    They are only re-resolved when cargo runs from inside `crates/<binding>/src`,
    where `.cargo/config.toml` redirects the version requirement back to this
    checkout. Run cargo from the repository root instead and it misses the
    redirect and goes looking for an unpublished version on crates.io.

That second point is why this is a script rather than a note: a patch bump leaves
every `^0.x` requirement satisfied, so nothing fails until CI's
`cargo clippy --locked` on the R bindings trips on staleness, with an error that
points nowhere near the version that moved.

What it deliberately does NOT do is edit the R bindings' version *requirements*.
Pointing a binding at a core version that is not on crates.io yet breaks R-universe,
which builds from a tarball with the local patch stripped, so that bump has to be
sequenced against the publish by someone who knows which half has landed.
`check-versions.sh`, which this runs at the end, is where that shows up.

    python scripts/bump-version.py sas7bdat 0.8.1
    python scripts/bump-version.py polars-plugin 0.9.1
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Lockfiles that pin workspace crates, and the directory cargo has to run from for
# the redirect in `.cargo/config.toml` to apply. `None` means the workspace root.
LOCKFILES = [
    (Path("Cargo.lock"), None),
    (Path("crates/r-plugin/src/rust/Cargo.lock"), Path("crates/r-plugin/src")),
    (Path("crates/r-convert-plugin/src/rust/Cargo.lock"), Path("crates/r-convert-plugin/src")),
]

VERSION_RE = re.compile(r"^(version = )\"[^\"]+\"", re.MULTILINE)
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")


def read_version(manifest: Path) -> str:
    match = VERSION_RE.search(manifest.read_text())
    if not match:
        raise SystemExit(f"no top-level version in {manifest}")
    return manifest.read_text()[match.start() : match.end()].split('"')[1]


def write_version(manifest: Path, version: str) -> None:
    text = manifest.read_text()
    # Only the first `version = ` line, which is the package's own; later ones belong
    # to dependency tables.
    manifest.write_text(VERSION_RE.sub(rf'\1"{version}"', text, count=1))


def package_name(manifest: Path) -> str:
    match = re.search(r'^name = "([^"]+)"', manifest.read_text(), re.MULTILINE)
    if not match:
        raise SystemExit(f"no package name in {manifest}")
    return match.group(1)


def lock_pins(lockfile: Path, package: str) -> bool:
    """Whether `lockfile` has a [[package]] entry for `package` at all."""
    return f'name = "{package}"' in lockfile.read_text()


def run(command: list[str], cwd: Path) -> None:
    print(f"  $ {' '.join(command)}" + (f"   (in {cwd.relative_to(ROOT)})" if cwd != ROOT else ""))
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"failed: {' '.join(command)}")


def semver_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("-")[0].split("."))


def open_changelog_section(changelog: Path, version: str) -> bool:
    """Insert an empty section for `version`, and its link definitions.

    The entry text is deliberately left blank: this file's entries are curated prose,
    and a generated stub is worth less than the reminder to write one.
    """
    text = changelog.read_text()
    if f"## [{version}]" in text:
        return False
    today = dt.date.today().isoformat()
    text = text.replace(
        "## [Unreleased]\n", f"## [Unreleased]\n\n## [{version}] - {today}\n", 1
    )

    repo = "https://github.com/tkragholm/sas7bdat-parser-rs"
    text = re.sub(
        r"^\[Unreleased\]: .*$",
        f"[Unreleased]: {repo}/compare/sas7bdat-v{version}...HEAD\n"
        f"[{version}]: {repo}/releases/tag/sas7bdat-v{version}",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    changelog.write_text(text)
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("crate", help="directory under crates/, e.g. sas7bdat or polars-plugin")
    ap.add_argument("version", help="the new version, e.g. 0.8.1")
    ap.add_argument(
        "--allow-downgrade", action="store_true", help="permit a version that sorts lower"
    )
    args = ap.parse_args(argv)

    if not SEMVER_RE.match(args.version):
        print(f"not a version: {args.version}", file=sys.stderr)
        return 2

    manifest = ROOT / "crates" / args.crate / "Cargo.toml"
    if not manifest.is_file():
        print(f"no such crate: {manifest.relative_to(ROOT)}", file=sys.stderr)
        return 2

    name = package_name(manifest)
    current = read_version(manifest)
    if current == args.version:
        print(f"{name} is already {args.version}", file=sys.stderr)
        return 2
    if not args.allow_downgrade and semver_key(args.version) < semver_key(current):
        print(
            f"{args.version} sorts below {current}; pass --allow-downgrade if that is intended",
            file=sys.stderr,
        )
        return 2

    print(f"{name}: {current} -> {args.version}\n")
    print("manifests:")
    write_version(manifest, args.version)
    print(f"  {manifest.relative_to(ROOT)}")
    pyproject = manifest.with_name("pyproject.toml")
    if pyproject.is_file():
        write_version(pyproject, args.version)
        print(f"  {pyproject.relative_to(ROOT)}   (what actually lands on PyPI)")

    print("\nlockfiles:")
    for lockfile, workdir in LOCKFILES:
        path = ROOT / lockfile
        if not path.is_file() or not lock_pins(path, name):
            continue
        if workdir is None:
            run(["cargo", "update", "--workspace"], ROOT)
        else:
            run(
                ["cargo", "update", "--manifest-path", "rust/Cargo.toml", "-p", name],
                ROOT / workdir,
            )

    changelog = manifest.with_name("CHANGELOG.md")
    wrote_section = changelog.is_file() and open_changelog_section(changelog, args.version)
    if wrote_section:
        print(f"\nchangelog:\n  opened an empty [{args.version}] section in "
              f"{changelog.relative_to(ROOT)} -- write the entry before committing")

    print("\nchecking:")
    check = subprocess.run(
        ["bash", "scripts/check-versions.sh"], cwd=ROOT, capture_output=True, text=True
    )
    sys.stdout.write("".join(f"  {line}\n" for line in check.stdout.splitlines()))
    if check.returncode != 0:
        sys.stderr.write(check.stderr)
        print("\ncheck-versions.sh is unhappy -- see above before going further.", file=sys.stderr)
        return 1

    print("\nnext:")
    print(f"  just release-preflight {name}")
    print(f"  git commit -am 'chore(release): {name} {args.version}'")
    tag = f"{name}-v{args.version}" if not pyproject.is_file() else f"v{args.version}"
    print(f"  git tag -a {tag} -m '{name} {args.version}' && git push origin main {tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
