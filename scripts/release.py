#!/usr/bin/env python3
"""Plan and run a release, including the parts it forces on other crates.

A release here is never one crate and never one step, and the ordering has until now
lived in prose comments and in whoever last cut one. Releasing 0.9.0 meant finding
out, by failing three times in a row, that:

  * `sas7bdat-convert` depends on the core by *version* as well as by path, so its
    requirement had to cross to "0.9" -- which makes it a breaking release of its
    own, 0.2.1 -> 0.3.0. `just bump` died at `cargo update` without saying so.
  * `sas7bdat-convert` cannot even be packaged until the core is on crates.io.
  * `vendor-r-package.sh` cannot regenerate `inst/AUTHORS` until then either,
    because it strips `src/.cargo` exactly as `R CMD build` does. That check had
    been failing in CI since the previous release and could not have been fixed
    before publishing.

So this prints the whole sequence first, marks what is already done, and names what
each remaining step is waiting for. Re-run it at any point: every step decides for
itself whether it has happened, so resuming is just running it again.

    just release sas7bdat 0.9.0                      # plan only, changes nothing
    just release sas7bdat 0.9.0 --execute            # local work, and push main
    just release sas7bdat 0.9.0 --execute --publish  # also push tags (irreversible)

`--execute` stops at each human gate rather than guessing past it: the changelog
prose is yours to write, and a tag push uploads to crates.io permanently, so it
needs `--publish` on top.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TICK, NEXT, WAIT, TODO = "  ok ", "  -> ", "  .. ", "     "


def run(command: list[str], cwd: Path | None = None, check: bool = True):
    result = subprocess.run(command, cwd=cwd or ROOT, capture_output=True, text=True)
    if check and result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"failed: {' '.join(command)}")
    return result


def manifest_of(crate: str) -> Path:
    return ROOT / "crates" / crate / "Cargo.toml"


def read_version(manifest: Path) -> str:
    match = re.search(r'^version = "([^"]+)"', manifest.read_text(), re.MULTILINE)
    if not match:
        raise SystemExit(f"no top-level version in {manifest}")
    return match.group(1)


def package_name(manifest: Path) -> str:
    match = re.search(r'^name = "([^"]+)"', manifest.read_text(), re.MULTILINE)
    return match.group(1) if match else manifest.parent.name


def minor_bump(version: str) -> str:
    major, minor, *_ = version.split(".")
    return f"{major}.{int(minor) + 1}.0"


def series(version: str) -> str:
    """The `major.minor` a requirement must name. Same rule as check-versions.sh."""
    return version.rsplit(".", 1)[0]


def all_manifests() -> list[Path]:
    """Every manifest that can carry a version requirement on one of our crates.

    Globbed rather than listed, so a new binding is picked up without editing this.
    The R bindings sit outside the workspace, which is why `cargo metadata` is not
    enough on its own.
    """
    found = sorted((ROOT / "crates").glob("*/Cargo.toml"))
    found += sorted((ROOT / "crates").glob("*/src/rust/Cargo.toml"))
    return found


def requirement(manifest: Path, dep: str) -> str | None:
    data = tomllib.loads(manifest.read_text())
    for table in ("dependencies", "dev-dependencies", "build-dependencies"):
        entry = data.get(table, {}).get(dep)
        if isinstance(entry, dict) and "version" in entry:
            return entry["version"]
        if isinstance(entry, str):
            return entry
    return None


def published_workspace_crates() -> dict[str, Path]:
    """Workspace crates that go to crates.io, by package name."""
    meta = json.loads(run(["cargo", "metadata", "--format-version", "1", "--no-deps"]).stdout)
    out = {}
    for package in meta["packages"]:
        if package.get("publish") == []:
            continue
        out[package["name"]] = Path(package["manifest_path"])
    return out


def is_published(name: str, version: str) -> bool:
    """Whether crates.io already carries this exact version, via the sparse index."""
    lowered = name.lower()
    if len(lowered) >= 4:
        prefix = f"{lowered[:2]}/{lowered[2:4]}"
    elif len(lowered) == 3:
        prefix = f"3/{lowered[0]}"
    else:
        prefix = str(len(lowered))
    url = f"https://index.crates.io/{prefix}/{lowered}"
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            body = response.read().decode()
    except (urllib.error.URLError, TimeoutError):
        # No network is not evidence of absence. `cargo search` is the fallback.
        result = run(["cargo", "search", name, "--limit", "1"], check=False)
        return f'{name} = "{version}"' in result.stdout
    for line in body.splitlines():
        if not line.strip():
            continue
        if json.loads(line).get("vers") == version:
            return True
    return False


@dataclass
class Move:
    """One crate's version moving, and why."""
    name: str
    manifest: Path
    current: str
    target: str
    because: str


@dataclass
class Plan:
    moves: list[Move]
    requirement_edits: list[tuple[Path, str, str, str]] = field(default_factory=list)

    @property
    def publish_order(self) -> list[Move]:
        """Core first: a dependent cannot be packaged until what it needs is up."""
        return self.moves


def build_plan(crate: str, version: str) -> Plan:
    manifest = manifest_of(crate)
    if not manifest.is_file():
        raise SystemExit(f"no such crate: crates/{crate}")
    name = package_name(manifest)
    moves = [Move(name, manifest, read_version(manifest), version, "asked for")]

    published = published_workspace_crates()
    manifests = all_manifests()

    # Anything whose stated requirement will no longer match has to move too: a
    # published crate needs its own release, an unpublished one just needs the edit.
    queue = [(name, version)]
    seen = {name}
    edits: list[tuple[Path, str, str, str]] = []
    while queue:
        dep, dep_version = queue.pop(0)
        wanted = series(dep_version)
        for other in manifests:
            req = requirement(other, dep)
            if req is None or req == wanted:
                continue
            edits.append((other, dep, req, wanted))
            owner = package_name(other)
            if owner in seen or owner not in published:
                continue
            # A published crate whose dependency requirement crosses an incompatible
            # boundary is itself a breaking release.
            current = read_version(other)
            target = minor_bump(current)
            moves.append(Move(owner, other, current, target,
                              f'requires {dep} "{req}", which no longer matches'))
            seen.add(owner)
            queue.append((owner, target))

    return Plan(moves=moves, requirement_edits=edits)


# --------------------------------------------------------------------------- steps

def changelog_of(move: Move) -> Path | None:
    path = move.manifest.parent / "CHANGELOG.md"
    return path if path.is_file() else None


def changelog_written(move: Move) -> bool:
    changelog = changelog_of(move)
    if changelog is None:
        return True
    text = changelog.read_text()
    match = re.search(rf"^## \[{re.escape(move.target)}\][^\n]*\n(.*?)(?=^## \[|\Z)",
                      text, re.MULTILINE | re.DOTALL)
    return bool(match and match.group(1).strip())


def versions_moved(plan: Plan) -> bool:
    return all(read_version(m.manifest) == m.target for m in plan.moves)


def couplings_green() -> bool:
    return run(["bash", "scripts/check-versions.sh"], check=False).returncode == 0


def tree_clean() -> bool:
    return not run(["git", "status", "--porcelain"]).stdout.strip()


def tag_exists(tag: str) -> bool:
    return bool(run(["git", "tag", "-l", tag]).stdout.strip())


def authors_current() -> bool:
    """Only knowable by vendoring, which needs every crate published first."""
    return run(["git", "diff", "--quiet", "--", "crates/*/inst/AUTHORS"], check=False).returncode == 0


def describe(plan: Plan, publish_state: dict[str, bool]) -> int:
    """Print the sequence. Returns the index of the first step not yet done."""
    steps: list[tuple[bool, str, str]] = []

    for move in plan.moves:
        steps.append((read_version(move.manifest) == move.target,
                      f"bump {move.name} {move.current} -> {move.target}",
                      "" if move.because == "asked for" else move.because))

    for path, dep, req, wanted in plan.requirement_edits:
        rel = path.relative_to(ROOT)
        steps.append((requirement(path, dep) == wanted,
                      f'{rel}: {dep} "{req}" -> "{wanted}"', ""))

    steps.append((versions_moved(plan) and couplings_green(),
                  "regenerate lockfiles, check-versions.sh green", ""))

    for move in plan.moves:
        if changelog_of(move) is not None:
            steps.append((changelog_written(move),
                          f"write CHANGELOG [{move.target}] for {move.name}",
                          "yours to write; this only opens the section"))

    steps.append((versions_moved(plan) and tree_clean(), "commit the release", ""))

    for move in plan.publish_order:
        tag = f"{move.name}-v{move.target}"
        done = publish_state.get(move.name, False)
        steps.append((done, f"tag + push {tag}",
                      "" if done else "uploads to crates.io; needs --publish"))

    steps.append((all(publish_state.values()) and authors_current(),
                  "regenerate inst/AUTHORS, commit, push",
                  "impossible before the crates are up; this is the CI gate"))

    first_open = next((i for i, (done, _, _) in enumerate(steps) if not done), len(steps))
    for i, (done, what, why) in enumerate(steps):
        marker = TICK if done else (NEXT if i == first_open else TODO)
        line = f"{marker}{i + 1:>2}. {what}"
        print(f"{line}\n{'':>8}{why}" if why and not done else line)
    return first_open


def do_bumps(plan: Plan) -> None:
    for move in plan.moves:
        if read_version(move.manifest) == move.target:
            continue
        for target in (move.manifest, move.manifest.with_name("pyproject.toml")):
            if not target.is_file():
                continue
            text = target.read_text()
            target.write_text(re.sub(r'^(version = )"[^"]+"', rf'\1"{move.target}"',
                                     text, count=1, flags=re.MULTILINE))
        print(f"  bumped {move.name} -> {move.target}")


def do_requirements(plan: Plan) -> None:
    for path, dep, req, wanted in plan.requirement_edits:
        text = path.read_text()
        pattern = re.compile(rf'(^{re.escape(dep)} = .*?version = ")({re.escape(req)})(")',
                             re.MULTILINE)
        updated, count = pattern.subn(rf'\g<1>{wanted}\g<3>', text)
        if count:
            path.write_text(updated)
            print(f'  {path.relative_to(ROOT)}: {dep} -> "{wanted}"')


def do_lockfiles() -> None:
    run(["cargo", "update", "--workspace"])
    for binding in sorted((ROOT / "crates").glob("*/src/rust/Cargo.lock")):
        cwd = binding.parents[1]
        run(["cargo", "update", "--manifest-path", "rust/Cargo.toml"], cwd=cwd)
    print("  lockfiles regenerated")


def do_changelog_sections(plan: Plan) -> None:
    for move in plan.moves:
        changelog = changelog_of(move)
        if changelog is None or f"## [{move.target}]" in changelog.read_text():
            continue
        today = time.strftime("%Y-%m-%d")
        text = changelog.read_text().replace(
            "## [Unreleased]\n", f"## [Unreleased]\n\n## [{move.target}] - {today}\n", 1)
        changelog.write_text(text)
        print(f"  opened [{move.target}] in {changelog.relative_to(ROOT)}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("crate")
    ap.add_argument("version")
    ap.add_argument("--execute", action="store_true", help="do the local work and push main")
    ap.add_argument("--publish", action="store_true",
                    help="also push release tags, which uploads to crates.io permanently")
    args = ap.parse_args()

    if not re.fullmatch(r"\d+\.\d+\.\d+", args.version):
        raise SystemExit(f"not a version: {args.version}")

    plan = build_plan(args.crate, args.version)

    print(f"\nRelease {args.crate} {args.version}")
    if len(plan.moves) > 1:
        forced = ", ".join(f"{m.name} {m.current} -> {m.target}" for m in plan.moves[1:])
        print(f"This forces: {forced}")
    print()

    publish_state = {m.name: is_published(m.name, m.target) for m in plan.moves}
    describe(plan, publish_state)
    print()

    if not args.execute:
        print("  Nothing has run. Add --execute for the local steps,")
        print("  and --publish on top of it to push tags.\n")
        return 0

    if not versions_moved(plan):
        do_bumps(plan)
        do_requirements(plan)
        do_lockfiles()
        do_changelog_sections(plan)

    if not couplings_green():
        run(["bash", "scripts/check-versions.sh"], check=False)
        raise SystemExit("check-versions.sh is unhappy; stopping")

    unwritten = [m.name for m in plan.moves if not changelog_written(m)]
    if unwritten:
        print(f"\n  Stopped: the changelog section for {', '.join(unwritten)} is empty.")
        print("  Write it, commit, then run this again.\n")
        return 0

    if not tree_clean():
        print("\n  Stopped: uncommitted changes. Review them, commit, then run this again.")
        print("  Suggested subject:")
        moved = ", ".join(f"{m.name} {m.target}" for m in plan.moves)
        print(f"    chore(release): {moved}\n")
        return 0

    if not args.publish:
        print("\n  Local work is done and committed. Add --publish to push the tags.\n")
        return 0

    for move in plan.publish_order:
        tag = f"{move.name}-v{move.target}"
        if publish_state.get(move.name):
            continue
        print(f"\n==> {tag}")
        run(["just", "release-preflight", move.name])
        if not tag_exists(tag):
            run(["git", "tag", "-a", tag, "-m", f"{move.name} {move.target}"])
        run(["git", "push", "origin", tag])
        print("  waiting for crates.io ...")
        for _ in range(60):
            if is_published(move.name, move.target):
                print(f"  {move.name} {move.target} is live")
                break
            time.sleep(20)
        else:
            raise SystemExit(f"{tag} pushed but {move.name} {move.target} has not appeared; "
                             "check the release-crate workflow, then run this again")

    print("\n==> inst/AUTHORS")
    for package in sorted(p.parent.name for p in (ROOT / "crates").glob("*/inst/AUTHORS")):
        run(["./scripts/vendor-r-package.sh", package])
    if authors_current():
        print("  already current")
    else:
        print("  regenerated; commit and push it to turn the CRAN check green")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
