#!/usr/bin/env python3
"""Release every package that has changed, in one run.

This repository ships six packages, each with its own version and tag:

  sas7bdat          crates.io  sas7bdat-v*          the reader
  sas7bdat-convert  crates.io  sas7bdat-convert-v*  conversion to Parquet/CSV/TSV
  sas7bdat-polars   PyPI       sas7bdat-polars-v*   the Polars plugin
  sas7bdat-cli      PyPI       sas7bdat-cli-v*      the command line converter
  fastsas           R-universe fastsas-v*           the R reader
  fastsasconvert    R-universe fastsasconvert-v*    the R converter

A package owes a release when the code it ships has changed since its last tag.
For the crates that is their own directory. The wheels compile the crates in by
path, so a change to the reader's source is owed by both wheels as well. The R
packages vendor the crates from crates.io, so they owe a release whenever a crate
they vendor is released, and whenever their own directory changes.

    just release                     # the plan: what is owed, and at what version
    just release sas7bdat=0.11.0     # the same, with a version of your choosing
    just release --execute           # bump, draft the changelogs; changes files only
    just release --publish           # commit, push, tag, upload, vendor R, release
    just release-status              # where every package stands right now

`--execute` writes each package's new version, the lockfiles, CITATION.cff and a
changelog entry drafted from the commit subjects, and stops: the entries are
yours to edit. Each draft carries a marker line, and `--publish` refuses to run
while one is left. Every step decides for itself whether it has happened, so an
interrupted run resumes by running it again.

`--publish` pushes main, uploads to crates.io and PyPI (permanent: neither takes
a version back), regenerates the R packages' vendored set once the crates are
up, and publishes a GitHub release for every tag. Pushes go through git, so a
pre-push hook such as a publication scan still gets the last word.
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
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = "tkragholm/sas7bdat-parser-rs"
R_UNIVERSE = "https://tkragholm.r-universe.dev/api/packages"

#: Marks a changelog entry the script wrote. `--publish` refuses while one is left.
DRAFT = "<!-- drafted from the commit subjects by release.py: edit, then delete this line -->"


def run(command: list[str], cwd: Path | None = None, check: bool = True):
    result = subprocess.run(command, cwd=cwd or ROOT, capture_output=True, text=True)
    if check and result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"failed: {' '.join(command)}")
    return result


def git(*args: str, check: bool = True) -> str:
    return run(["git", *args], check=check).stdout


# ------------------------------------------------------------------ packages


@dataclass(frozen=True)
class Package:
    name: str
    dir: str
    kind: str  # "crate", "wheel" or "r"
    #: Older tag prefixes still accepted for the version a manifest claims.
    legacy_prefix: str = ""

    @property
    def path(self) -> Path:
        return ROOT / "crates" / self.dir

    @property
    def prefix(self) -> str:
        return f"{self.name}-v"

    def tag(self, version: str) -> str:
        return f"{self.prefix}{version}"

    @property
    def manifest(self) -> Path:
        return self.path / ("DESCRIPTION" if self.kind == "r" else "Cargo.toml")

    @property
    def changelog(self) -> Path:
        return self.path / ("NEWS.md" if self.kind == "r" else "CHANGELOG.md")

    @property
    def where(self) -> str:
        return {"crate": "crates.io", "wheel": "PyPI", "r": "R-universe"}[self.kind]


#: Publish order: a crate before anything that builds against it.
PACKAGES = [
    Package("sas7bdat", "sas7bdat", "crate"),
    Package("sas7bdat-convert", "sas7bdat-convert", "crate"),
    Package("sas7bdat-polars", "polars-plugin", "wheel", legacy_prefix="v"),
    Package("sas7bdat-cli", "sas7bdat-cli", "wheel", legacy_prefix="v"),
    Package("fastsas", "r-plugin", "r"),
    Package("fastsasconvert", "r-convert-plugin", "r"),
]
BY_NAME = {p.name: p for p in PACKAGES} | {p.dir: p for p in PACKAGES}


def version_of(pkg: Package) -> str:
    pattern = r"^Version: (\S+)" if pkg.kind == "r" else r'^version = "([^"]+)"'
    match = re.search(pattern, pkg.manifest.read_text(), re.MULTILINE)
    if not match:
        raise SystemExit(f"no version in {pkg.manifest}")
    return match.group(1)


def bump_patch(version: str) -> str:
    major, minor, patch = version.split(".")
    return f"{major}.{minor}.{int(patch) + 1}"


def bump_minor(version: str) -> str:
    major, minor, *_ = version.split(".")
    return f"{major}.{int(minor) + 1}.0"


def series(version: str) -> str:
    """The `major.minor` a caret requirement on a 0.x crate has to match."""
    return version.rsplit(".", 1)[0]


def compatible(requirement: str, version: str) -> bool:
    """Whether `requirement` (as written in a manifest) accepts `version`.

    "0.10" and "0.10.2" both accept 0.10.3; neither accepts 0.11.0.
    """
    want = series(version)
    if requirement == want:
        return True
    if requirement.startswith(want + "."):
        return int(requirement.rsplit(".", 1)[1]) <= int(version.rsplit(".", 1)[1])
    return False


def requirement(manifest: Path, dep: str) -> str | None:
    data = tomllib.loads(manifest.read_text())
    for table in ("dependencies", "dev-dependencies", "build-dependencies"):
        entry = data.get(table, {}).get(dep)
        if isinstance(entry, dict) and "version" in entry:
            return entry["version"]
        if isinstance(entry, str):
            return entry
    return None


def cargo_manifest(pkg: Package) -> Path:
    if pkg.kind == "r":
        return pkg.path / "src" / "rust" / "Cargo.toml"
    return pkg.manifest


def depends_on(manifest: Path, dep: str) -> bool:
    data = tomllib.loads(manifest.read_text())
    return any(
        dep in data.get(table, {}) for table in ("dependencies", "build-dependencies")
    )


def vendored(pkg: Package) -> list[Package]:
    """The crates of ours that a package builds in: by path for the workspace
    members, by version for the R bindings, which build from crates.io.
    """
    manifest = cargo_manifest(pkg)
    return [
        dep
        for dep in PACKAGES
        if dep.kind == "crate" and dep is not pkg and depends_on(manifest, dep.name)
    ]


def locked_version(lockfile: Path, name: str) -> str | None:
    if not lockfile.is_file():
        return None
    for block in tomllib.loads(lockfile.read_text()).get("package", []):
        if block.get("name") == name:
            return block.get("version")
    return None


def vendor_stale(pkg: Package) -> bool:
    """Whether an R package's vendored set predates a crate it vendors."""
    lock = pkg.path / "src" / "rust" / "vendor.lock"
    return any(
        locked_version(lock, dep.name) != version_of(dep) for dep in vendored(pkg)
    )


def tag_exists(tag: str) -> bool:
    return bool(git("tag", "-l", tag).strip())


def released_tag(pkg: Package) -> str | None:
    """The tag of the version the manifest claims, if that version was released."""
    version = version_of(pkg)
    for tag in (
        pkg.tag(version),
        f"{pkg.legacy_prefix}{version}" if pkg.legacy_prefix else "",
    ):
        if tag and tag_exists(tag):
            return tag
    return None


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", version))


def last_tag(pkg: Package) -> str | None:
    """The newest release tag of this package, whatever the manifest says now.

    Ordered by the version in the tag, not by the tag's name: a legacy `v0.10.1`
    sorts after `sas7bdat-polars-v0.10.3` as text.
    """
    patterns = [f"{pkg.prefix}[0-9]*"] + (
        [f"{pkg.legacy_prefix}[0-9]*"] if pkg.legacy_prefix else []
    )
    tags = git("tag", "-l", *patterns).split()
    if not tags:
        return None
    return max(tags, key=lambda tag: _version_key(tag.rsplit("v", 1)[-1]))


def _shipped_manifest(text: str) -> str:
    """A manifest without what does not change the build of a path dependent:
    the crate's own version line, and the version requirements on path
    dependencies, which only a build from crates.io reads."""
    text = re.sub(r'^version = "[^"]+"\n', "", text, count=1, flags=re.MULTILINE)
    return re.sub(r'(path = "[^"]+"), version = "[^"]+"', r"\1", text)


def changed_since(pkg: Package, tag: str) -> list[str]:
    """Why `pkg` differs from what `tag` released; empty when it does not.

    Compared against the working tree, so the plan sees edits not yet committed.

    A crate's examples and benches are developer tools and do not ship, and
    the changelog is written by the release itself. A wheel
    also ships the source of the crates it compiles in, but not their version
    lines: a crate's own release would otherwise owe every wheel one too.
    """
    reasons = []
    own = [str(pkg.path.relative_to(ROOT))]
    own.append(f":(exclude){pkg.changelog.relative_to(ROOT)}")
    if pkg.kind == "crate":
        own += [f":(exclude){own[0]}/examples", f":(exclude){own[0]}/benches"]
    if git("diff", "--name-only", tag, "--", *own).strip():
        reasons.append(f"crates/{pkg.dir} changed")
    if pkg.kind == "wheel":
        for dep in vendored(pkg):
            base = f"crates/{dep.dir}"
            if git(
                "diff",
                "--name-only",
                tag,
                "--",
                f"{base}/src",
                f"{base}/build.rs",
            ).strip():
                reasons.append(f"{dep.name}'s source changed")
                continue
            old = git("show", f"{tag}:{base}/Cargo.toml", check=False)
            if _shipped_manifest(old) != _shipped_manifest(
                (ROOT / base / "Cargo.toml").read_text()
            ):
                reasons.append(f"{dep.name}'s manifest changed")
    return reasons


# ---------------------------------------------------------------------- plan


@dataclass
class Move:
    pkg: Package
    current: str
    target: str
    because: str

    @property
    def tag(self) -> str:
        return self.pkg.tag(self.target)


@dataclass
class Plan:
    moves: list[Move]
    #: (manifest, dependency, old requirement, new requirement)
    requirement_edits: list[tuple[Path, str, str, str]]

    def moving(self, pkg: Package) -> Move | None:
        return next((m for m in self.moves if m.pkg is pkg), None)


def shipped_paths(pkg: Package) -> list[str]:
    """Where the commits that change what a package ships are found."""
    paths = [
        str(pkg.path.relative_to(ROOT)),
        f":(exclude){pkg.changelog.relative_to(ROOT)}",
    ]
    if pkg.kind != "crate":
        paths += [f"crates/{d.dir}/src" for d in vendored(pkg)]
    return paths


def commits_since(pkg: Package, tag: str | None) -> str:
    """Subjects and bodies of the commits since `tag` that touch what `pkg` ships."""
    span = f"{tag}..HEAD" if tag else "HEAD"
    return git(
        "log", "--reverse", "--format=%s%n%b%x00", span, "--", *shipped_paths(pkg)
    )


def bump_for(pkg: Package, tag: str) -> tuple[str, str]:
    """The version a package's next release should have, and why.

    A breaking commit is a minor release. So is a new feature in an R package:
    a 0.x crate may add API in a patch release under Cargo's rules, which R
    has no counterpart for. Only a package's own commits count here: a crate
    it compiles in or vendors can add API without changing the package's own.
    """
    current = version_of(pkg)
    span = f"{tag}..HEAD"
    log = git("log", "--format=%s%n%b%x00", span, "--", str(pkg.path.relative_to(ROOT)))
    if re.search(r"^\w+(\([^)]*\))?!:|^BREAKING CHANGE", log, re.MULTILINE):
        return bump_minor(current), " (a breaking commit: minor)"
    if pkg.kind == "r" and re.search(r"^feat(\([^)]*\))?:", log, re.MULTILINE):
        return bump_minor(current), " (new features: minor)"
    return bump_patch(current), ""


def owed() -> list[Move]:
    """Every package whose shipped code has moved past its last release."""
    moves = []
    for pkg in PACKAGES:
        current = version_of(pkg)
        tag = released_tag(pkg)
        if tag is None:
            since = last_tag(pkg)
            moves.append(
                Move(
                    pkg,
                    since.rsplit("v", 1)[-1] if since else "-",
                    current,
                    "bumped, not yet tagged",
                )
            )
            continue
        reasons = changed_since(pkg, tag)
        if reasons:
            target, note = bump_for(pkg, tag)
            moves.append(Move(pkg, current, target, "; ".join(reasons) + note))
    return moves


def build_plan(overrides: dict[str, str]) -> Plan:
    moves = {m.pkg.name: m for m in owed()}
    for name, version in overrides.items():
        pkg = BY_NAME[name]
        if pkg.name in moves:
            moves[pkg.name].target = version
        else:
            moves[pkg.name] = Move(pkg, version_of(pkg), version, "asked for")

    # A requirement that no longer accepts a moving crate has to move with it. In a
    # published crate that is a breaking change of its own.
    edits: list[tuple[Path, str, str, str]] = []
    queue = [m for m in moves.values() if m.pkg.kind == "crate"]
    while queue:
        dep = queue.pop(0)
        for other in PACKAGES:
            if other is dep.pkg:
                continue
            manifest = cargo_manifest(other)
            req = requirement(manifest, dep.pkg.name)
            if req is None or compatible(req, dep.target):
                continue
            edits.append((manifest, dep.pkg.name, req, series(dep.target)))
            already = moves.get(other.name)
            if already is None or (
                other.kind == "crate"
                and already.target == bump_patch(version_of(other))
            ):
                current = version_of(other)
                target = (
                    bump_minor(current)
                    if other.kind == "crate"
                    else bump_patch(current)
                )
                moves[other.name] = Move(
                    other,
                    current,
                    target,
                    f'requires {dep.pkg.name} "{req}", which {dep.target} does not meet',
                )
                if other.kind == "crate":
                    queue.append(moves[other.name])

    # The R packages vendor the crates from crates.io, so a crate release moves them.
    for pkg in PACKAGES:
        if pkg.kind != "r" or pkg.name in moves:
            continue
        released = [d.name for d in vendored(pkg) if d.name in moves]
        if released:
            current = version_of(pkg)
            moves[pkg.name] = Move(
                pkg, current, bump_patch(current), f"vendors {', '.join(released)}"
            )

    ordered = [moves[p.name] for p in PACKAGES if p.name in moves]
    return Plan(ordered, edits)


# ---------------------------------------------------------------- registries


def _get(url: str) -> bytes | None:
    request = urllib.request.Request(
        url, headers={"User-Agent": "sas7bdat-release-script"}
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.read()
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return None
        raise
    except (urllib.error.URLError, TimeoutError):
        return None


def crates_io_versions(name: str) -> list[str]:
    lowered = name.lower()
    prefix = f"{lowered[:2]}/{lowered[2:4]}" if len(lowered) >= 4 else f"3/{lowered[0]}"
    body = _get(f"https://index.crates.io/{prefix}/{lowered}")
    if body is None:
        return []
    return [
        json.loads(line)["vers"] for line in body.decode().splitlines() if line.strip()
    ]


def pypi_versions(name: str) -> list[str]:
    body = _get(f"https://pypi.org/pypi/{name}/json")
    return list(json.loads(body)["releases"]) if body else []


def r_universe(name: str) -> dict:
    body = _get(f"{R_UNIVERSE}/{name}")
    return json.loads(body) if body else {}


def remote_tags() -> set[str]:
    out = git("ls-remote", "--tags", "origin", check=False)
    return {
        line.split("refs/tags/")[1].removesuffix("^{}")
        for line in out.splitlines()
        if "refs/tags/" in line
    }


def is_published(move: Move) -> bool:
    if move.pkg.kind == "crate":
        return move.target in crates_io_versions(move.pkg.name)
    if move.pkg.kind == "wheel":
        return move.target in pypi_versions(move.pkg.name)
    # R-universe builds from main on its own schedule; the pushed tag is the release.
    return move.tag in remote_tags()


def newest(versions: list[str]) -> str:
    def key(v: str):
        return tuple(int(x) if x.isdigit() else 0 for x in re.split(r"[.+-]", v))

    return max(versions, key=key) if versions else "-"


# ---------------------------------------------------------------- changelogs


def section_heading(pkg: Package, version: str) -> str:
    if pkg.kind == "r":
        return f"# {pkg.name} {version}"
    return f"## [{version}] - {time.strftime('%Y-%m-%d')}"


def section(pkg: Package, version: str) -> str | None:
    """The body of a version's changelog entry, or None if there is none."""
    if not pkg.changelog.is_file():
        return None
    text = pkg.changelog.read_text()
    if pkg.kind == "r":
        pattern = rf"^# {re.escape(pkg.name)} {re.escape(version)}\n(.*?)(?=^# |\Z)"
    else:
        pattern = rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|\Z)"
    match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    return match.group(1).strip() if match else None


GROUPS = {"feat": "Added", "fix": "Fixed", "perf": "Changed", "refactor": "Changed"}


def draft_entry(move: Move, targets: dict[str, str]) -> str:
    """A changelog entry from the conventional commit subjects that touched what
    this package ships. A starting point: the subjects are written for the
    history, not for the people who use the package.
    """
    pkg = move.pkg
    since = released_tag(pkg) or last_tag(pkg)
    subjects = [
        c.strip().split("\n", 1)[0] for c in commits_since(pkg, since).split("\0")
    ]
    grouped: dict[str, list[str]] = {}
    for subject in subjects:
        match = re.match(r"^(\w+)(?:\(([^)]*)\))?(!)?: (.+)$", subject)
        if not match or match.group(1) not in GROUPS:
            continue
        kind, _scope, bang, text = match.groups()
        group = "Changed" if bang else GROUPS[kind]
        grouped.setdefault(group, []).append(text[0].upper() + text[1:])
    lines = [DRAFT]
    built = [f"{d.name} {targets.get(d.name, version_of(d))}" for d in vendored(pkg)]
    if pkg.kind == "r":
        lines += [
            f"- {item}." if not item.endswith(".") else f"- {item}"
            for g in ("Added", "Changed", "Fixed")
            for item in grouped.get(g, [])
        ]
        if built:
            lines.append(f"- Built with {', '.join(built)}.")
        if len(lines) == 1:
            lines.append(
                "- Describe this release: no user-facing commit subjects were found."
            )
        return "\n".join(lines)
    for group in ("Added", "Changed", "Fixed"):
        if grouped.get(group):
            lines += ["", f"### {group}", ""] + [f"- {item}" for item in grouped[group]]
    if pkg.kind == "wheel" and built:
        lines += ["", f"Built with {', '.join(built)}."]
    if len(lines) == 1:
        lines += [
            "",
            "Describe this release: no user-facing commit subjects were found.",
        ]
    return "\n".join(lines)


CHANGELOG_HEADER = {
    "crate": "# Changelog\n\nAll notable changes to the `{name}` crate. The format follows\n[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).\n\n## [Unreleased]\n",
    "wheel": "# Changelog\n\nAll notable changes to the `{name}` Python package. The format follows\n[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).\n\n## [Unreleased]\n",
}


def write_draft(move: Move, targets: dict[str, str]) -> bool:
    pkg = move.pkg
    if section(pkg, move.target) is not None:
        return False
    entry = draft_entry(move, targets)
    heading = section_heading(pkg, move.target)
    if pkg.kind == "r":
        old = pkg.changelog.read_text() if pkg.changelog.is_file() else ""
        pkg.changelog.write_text(f"{heading}\n\n{entry}\n\n{old}".rstrip() + "\n")
    else:
        if not pkg.changelog.is_file():
            pkg.changelog.write_text(CHANGELOG_HEADER[pkg.kind].format(name=pkg.name))
        text = pkg.changelog.read_text()
        if "## [Unreleased]\n" not in text:
            raise SystemExit(
                f"{pkg.changelog.relative_to(ROOT)} has no '## [Unreleased]' heading"
            )
        pkg.changelog.write_text(
            text.replace(
                "## [Unreleased]\n", f"## [Unreleased]\n\n{heading}\n\n{entry}\n", 1
            )
        )
    return True


def drafts_left(plan: Plan) -> list[Path]:
    return [
        m.pkg.changelog.relative_to(ROOT)
        for m in plan.moves
        if DRAFT in (section(m.pkg, m.target) or "")
    ]


# --------------------------------------------------------------- local work


def write_version(pkg: Package, version: str) -> None:
    targets = [pkg.manifest]
    if pkg.kind == "wheel":
        targets.append(pkg.path / "pyproject.toml")
    for target in targets:
        text = target.read_text()
        if pkg.kind == "r":
            text = re.sub(
                r"^Version: \S+",
                f"Version: {version}",
                text,
                count=1,
                flags=re.MULTILINE,
            )
        else:
            text = re.sub(
                r'^(version = )"[^"]+"',
                rf'\1"{version}"',
                text,
                count=1,
                flags=re.MULTILINE,
            )
        target.write_text(text)


def write_requirement(path: Path, dep: str, old: str, new: str) -> None:
    pattern = re.compile(
        rf'(^{re.escape(dep)} = .*?version = ")({re.escape(old)})(")', re.MULTILINE
    )
    path.write_text(pattern.sub(rf"\g<1>{new}\g<3>", path.read_text(), count=1))


def write_citation(version: str) -> bool:
    path = ROOT / "CITATION.cff"
    text = path.read_text()
    updated = re.sub(
        r"^version: .*$", f"version: {version}", text, count=1, flags=re.MULTILINE
    )
    updated = re.sub(
        r"^date-released: .*$",
        f"date-released: {time.strftime('%Y-%m-%d')}",
        updated,
        count=1,
        flags=re.MULTILINE,
    )
    if updated != text and f"version: {version}\n" not in text:
        path.write_text(updated)
        return True
    return False


def regenerate_lockfiles() -> None:
    run(["cargo", "update", "--workspace"])
    # The binding lockfiles sit outside the workspace and only see `src/.cargo`'s
    # redirect to this checkout when cargo runs from inside their `src/`.
    for pkg in PACKAGES:
        if pkg.kind == "r":
            deps = [d.name for d in vendored(pkg)]
            run(
                [
                    "cargo",
                    "update",
                    "--manifest-path",
                    "rust/Cargo.toml",
                    *sum((["-p", d] for d in deps), []),
                ],
                cwd=pkg.path / "src",
            )


def prepare(plan: Plan) -> None:
    for move in plan.moves:
        if version_of(move.pkg) != move.target:
            write_version(move.pkg, move.target)
            print(f"  {move.pkg.name} {move.current} -> {move.target}")
    for path, dep, old, new in plan.requirement_edits:
        if requirement(path, dep) == old:
            write_requirement(path, dep, old, new)
            print(f'  {path.relative_to(ROOT)}: {dep} "{old}" -> "{new}"')
    regenerate_lockfiles()
    print("  lockfiles regenerated")
    core = plan.moving(BY_NAME["sas7bdat"])
    if core and write_citation(core.target):
        print(f"  CITATION.cff -> {core.target}")
    for move in plan.moves:
        if write_draft(move, {m.pkg.name: m.target for m in plan.moves}):
            print(f"  drafted {move.pkg.changelog.relative_to(ROOT)} [{move.target}]")


def tree_clean() -> bool:
    return not git("status", "--porcelain").strip()


def couplings_green() -> bool:
    return run(["bash", "scripts/check-versions.sh"], check=False).returncode == 0


# ------------------------------------------------------------------ publish


def github_release(move: Move, latest: bool) -> None:
    """Publish the GitHub release for a tag, unless the tag has one."""
    if run(["gh", "release", "view", move.tag], check=False).returncode == 0:
        return
    notes = section(move.pkg, move.target) or ""
    link = {
        "crate": f"https://crates.io/crates/{move.pkg.name}/{move.target}",
        "wheel": f"https://pypi.org/project/{move.pkg.name}/{move.target}/",
        "r": f"https://tkragholm.r-universe.dev/{move.pkg.name}",
    }[move.pkg.kind]
    body = f"{notes}\n\n{link}".strip()
    run(
        [
            "gh",
            "release",
            "create",
            move.tag,
            "--title",
            f"{move.pkg.name} {move.target}",
            "--notes",
            body,
            f"--latest={'true' if latest else 'false'}",
        ]
    )
    print(f"  GitHub release {move.tag}")


def push_tag(move: Move) -> None:
    if not tag_exists(move.tag):
        run(["git", "tag", "-a", move.tag, "-m", f"{move.pkg.name} {move.target}"])
    if move.tag not in remote_tags():
        run(["git", "push", "origin", move.tag])
        print(f"  pushed {move.tag}")


def wait_for(move: Move, minutes: int) -> None:
    print(f"  waiting for {move.pkg.name} {move.target} on {move.pkg.where} ...")
    deadline = time.time() + minutes * 60
    while time.time() < deadline:
        if is_published(move):
            print(f"  {move.pkg.name} {move.target} is live")
            return
        time.sleep(20)
    workflow = "release-crate" if move.pkg.kind == "crate" else "wheels"
    raise SystemExit(
        f"{move.tag} is pushed but {move.pkg.name} {move.target} has not appeared; check the {workflow} workflow, then run this again"
    )


def push_main() -> None:
    ahead = git("rev-list", "--count", "origin/main..HEAD").strip()
    if ahead != "0":
        run(["git", "push", "origin", "HEAD:main"])
        print(f"  pushed main ({ahead} commits)")


def publish(plan: Plan) -> None:
    left = drafts_left(plan)
    if left:
        raise SystemExit(
            "Edit the drafted changelog entries, and delete their marker line, in: "
            + ", ".join(map(str, left))
        )
    missing = [m.pkg.name for m in plan.moves if not section(m.pkg, m.target)]
    if missing:
        raise SystemExit(
            f"no changelog entry for {', '.join(missing)}; run --execute first"
        )
    if not couplings_green():
        run(["bash", "scripts/check-versions.sh"], check=False)
        raise SystemExit("check-versions.sh is unhappy; stopping")
    if git("rev-parse", "--abbrev-ref", "HEAD").strip() != "main":
        raise SystemExit("release from main")

    if not tree_clean():
        run(["git", "add", "-u"])
        run(
            [
                "git",
                "add",
                "--",
                *[str(m.pkg.changelog.relative_to(ROOT)) for m in plan.moves],
            ]
        )
        subject = "chore(release): " + ", ".join(
            f"{m.pkg.name} {m.target}" for m in plan.moves
        )
        run(["git", "commit", "-q", "-m", subject])
        print(f"  committed: {subject}")

    crates = [m for m in plan.moves if m.pkg.kind == "crate" and not is_published(m)]
    wheels = [m for m in plan.moves if m.pkg.kind == "wheel" and not is_published(m)]
    rpkgs = [m for m in plan.moves if m.pkg.kind == "r"]

    if crates or wheels:
        print("\n==> preflight")
        subprocess.run(
            ["just", "release-preflight", (crates or wheels)[0].pkg.name],
            cwd=ROOT,
            check=True,
        )

    print("\n==> main")
    push_main()

    for i, move in enumerate(crates):
        print(f"\n==> {move.tag}")
        if i:
            # Packaging resolves against crates.io, so it can only be checked once
            # the crates before it are up.
            run(["cargo", "publish", "--dry-run", "--locked", "-p", move.pkg.name])
        push_tag(move)
        wait_for(move, 20)

    # The wheels build from this commit; they need nothing from crates.io.
    for move in wheels:
        print(f"\n==> {move.tag}")
        push_tag(move)

    if rpkgs:
        print("\n==> R packages")
        stale = [m for m in rpkgs if vendor_stale(m.pkg)]
        for move in stale:
            # `--refresh`: the vendored set is pinned by a committed lock, and a
            # crate release is exactly when it should move. It resolves from
            # crates.io, which is why this waits until the crates are up.
            run(["./scripts/vendor-r-package.sh", move.pkg.dir, "--refresh"])
        if not tree_clean():
            run(["git", "add", "-u", "--", *[f"crates/{m.pkg.dir}" for m in stale]])
            against = " and ".join(
                f"{d.name} {version_of(d)}"
                for d in PACKAGES
                if any(d in vendored(m.pkg) for m in stale)
            )
            subject = f"chore(release): vendor the R packages against {against}"
            run(["git", "commit", "-q", "-m", subject])
            print(f"  committed: {subject}")
        push_main()
        for move in rpkgs:
            push_tag(move)

    for move in wheels:
        wait_for(move, 40)

    print("\n==> GitHub releases")
    # release-crate.yml publishes the crates' releases; the rest are made here.
    for move in plan.moves:
        if move.pkg.kind != "crate":
            github_release(move, latest=False)


# ------------------------------------------------------------------- status


def ci_on(sha: str) -> list[tuple[str, str]]:
    out = run(
        [
            "gh",
            "run",
            "list",
            "--commit",
            sha,
            "--json",
            "workflowName,status,conclusion",
            "--limit",
            "20",
        ],
        check=False,
    ).stdout
    try:
        runs = json.loads(out or "[]")
    except json.JSONDecodeError:
        return []
    seen: dict[str, str] = {}
    for r in runs:
        seen.setdefault(r["workflowName"], r["conclusion"] or r["status"])
    return sorted(seen.items())


def status() -> int:
    head = git("rev-parse", "HEAD").strip()
    pending = {m.pkg.name: m for m in owed()}
    releases = {}
    out = run(
        [
            "gh",
            "release",
            "list",
            "--limit",
            "60",
            "--json",
            "tagName,isDraft,isLatest",
        ],
        check=False,
    ).stdout
    for r in json.loads(out or "[]"):
        releases[r["tagName"]] = (
            "draft" if r["isDraft"] else ("latest" if r["isLatest"] else "published")
        )
    print(f"\n{'package':<17}{'here':<9}{'registry':<10}{'GitHub':<11}owed")
    for pkg in PACKAGES:
        here = version_of(pkg)
        if pkg.kind == "crate":
            registry = newest(crates_io_versions(pkg.name))
        elif pkg.kind == "wheel":
            registry = newest(pypi_versions(pkg.name))
        else:
            registry = r_universe(pkg.name).get("Version", "-")
        tag = released_tag(pkg) or last_tag(pkg) or ""
        gh = releases.get(tag, "none") if tag else "-"
        move = pending.get(pkg.name)
        note = f"{move.target}: {move.because}" if move else ""
        print(f"{pkg.name:<17}{here:<9}{registry:<10}{gh:<11}{note}")
    print(
        f"\nCI on {head[:7]}: "
        + (", ".join(f"{w} {c}" for w, c in ci_on(head)) or "no runs")
    )
    for pkg in PACKAGES:
        if pkg.kind == "r":
            info = r_universe(pkg.name)
            built = info.get("RemoteSha", "")
            state = (
                "current"
                if built == head
                else (f"{built[:7]}, behind main" if built else "unknown")
            )
            print(
                f"R-universe {pkg.name}: {info.get('_status', '?')}, built from {state}"
            )
    print()
    return 0


# --------------------------------------------------------------------- main


def describe(plan: Plan) -> None:
    if not plan.moves:
        print("\n  Nothing is owed: every package matches its last release.\n")
        return
    print()
    for move in plan.moves:
        state = (
            "published"
            if is_published(move)
            else ("tagged" if tag_exists(move.tag) else "")
        )
        print(
            f"  {move.pkg.name:<17}{move.current:>8} -> {move.target:<8} {move.pkg.where:<11}{state:<10}{move.because}"
        )
    for path, dep, old, new in plan.requirement_edits:
        print(f'  {path.relative_to(ROOT)}: {dep} "{old}" -> "{new}"')
    print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "versions",
        nargs="*",
        metavar="PACKAGE=VERSION",
        help="a version other than the proposed one",
    )
    ap.add_argument(
        "--execute",
        action="store_true",
        help="write versions, lockfiles, CITATION.cff and changelog drafts",
    )
    ap.add_argument(
        "--publish",
        action="store_true",
        help="commit, push, tag and upload (permanent)",
    )
    ap.add_argument("--status", action="store_true", help="where every package stands")
    ap.add_argument(
        "--owed", action="store_true", help="list owed releases, for check-versions.sh"
    )
    args = ap.parse_args()

    if args.status:
        return status()
    if args.owed:
        for move in owed():
            print(
                f"  owed  {move.pkg.name} {move.current} -> {move.target}: {move.because}"
            )
        return 0

    overrides = {}
    for item in args.versions:
        name, _, version = item.partition("=")
        if name not in BY_NAME or not re.fullmatch(r"\d+\.\d+\.\d+", version):
            raise SystemExit(
                f"expected PACKAGE=VERSION with a package from {', '.join(p.name for p in PACKAGES)}: {item}"
            )
        overrides[BY_NAME[name].name] = version

    plan = build_plan(overrides)
    describe(plan)
    if not plan.moves:
        return 0

    if args.execute or args.publish:
        prepare(plan)
        left = drafts_left(plan)
        if left:
            print(
                "\n  Edit the drafted entries, delete their marker line, then run --publish:"
            )
            for path in left:
                print(f"    {path}")
            print()
            return 0
    if args.publish:
        publish(plan)
        status()
        return 0
    if not args.execute:
        print("  Nothing has run. --execute writes the release, --publish ships it.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
