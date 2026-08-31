#!/usr/bin/env python3
"""Report what is missing on this machine before it wastes an afternoon.

Every check here exists because its absence cost real time once, and because the
symptom pointed somewhere else:

  * No `.venv`, and maturin says "could not determine version from interpreter name".
  * `fixtures/ahs2013n.sas7bdat` absent, and 26 plugin tests skip. Nothing else says so.
  * R packages never installed, so `just test-r` looks like a compile failure.
  * The version couplings drift, and the first sign is a `--locked` lockfile error in
    CI pointing nowhere near the version that moved.
  * CI on main has been red since a release, and nobody looks until the next one.

Read-only. It never installs or edits anything; each finding names the command that
would fix it.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

OK, WARN, BAD = "ok  ", "warn", "MISS"
findings: list[tuple[str, str, str]] = []


def note(status: str, what: str, detail: str = "") -> None:
    findings.append((status, what, detail))


def run(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd or ROOT, capture_output=True, text=True)


def check_toolchain() -> None:
    pinned = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    have = run(["cargo", "--version"]).stdout.strip()
    if pinned in have:
        note(OK, f"rust {pinned}")
    else:
        note(WARN, f"rust: pinned {pinned}", f"cargo reports: {have or 'not found'}")


def check_python() -> None:
    venv = ROOT / ".venv" / "bin" / "python"
    if not venv.exists():
        note(BAD, ".venv missing", "just setup-python  (the plugin tests cannot run without it)")
        return
    version = run([str(venv), "--version"]).stdout.strip()
    # `pip list` is wrong here: uv builds a venv without pip, so it reports nothing
    # and every package looks missing. The interpreter's own metadata always works.
    installed = run([str(venv), "-c",
                     "import importlib.metadata as m;"
                     "print('\\n'.join(sorted(d.name.lower() for d in m.distributions())))"])
    names = set(installed.stdout.split()) if installed.returncode == 0 else set()
    missing = {"polars", "pytest", "pytest-xdist"} - names
    if missing:
        note(WARN, f".venv ({version})", f"missing {', '.join(sorted(missing))} -> just setup-python")
    else:
        note(OK, f".venv ({version})")

    built = run([str(venv), "-c", "import sas7bdat_polars as m; print(m.__core_version__)"])
    if built.returncode == 0:
        core = built.stdout.strip()
        declared = tomllib.loads((ROOT / "crates/sas7bdat/Cargo.toml").read_text())["package"]["version"]
        if core == declared:
            note(OK, f"sas7bdat_polars built against core {core}")
        else:
            note(WARN, f"sas7bdat_polars carries core {core}, tree is {declared}",
                 "just test-polars-plugin  (rebuilds it)")
    else:
        note(WARN, "sas7bdat_polars not importable", "just test-polars-plugin  (builds it)")


def check_fixtures() -> None:
    corpus = ROOT / "fixtures" / "raw_data"
    if not corpus.exists():
        note(BAD, "fixtures/raw_data missing", "see fixtures/README.md; the corpus is not in git")
    else:
        count = len(list(corpus.rglob("*.sas7bdat")))
        note(OK, f"fixtures/raw_data ({count} files)")

    named = ROOT / "fixtures" / "ahs2013n.sas7bdat"
    if not named.exists():
        note(WARN, "fixtures/ahs2013n.sas7bdat missing",
             "26 polars-plugin tests skip; `just bench-plugin-vs-raw` defaults name it too")


def check_r() -> None:
    if not shutil.which("Rscript"):
        note(WARN, "R not installed", "just test-r cannot run")
        return
    script = (
        'cat(R.version.string, "\\n");'
        'for (p in c("testthat","haven","fastsas","fastsasconvert"))'
        ' cat(p, requireNamespace(p, quietly=TRUE), "\\n")'
    )
    out = run(["Rscript", "-e", script])
    present = {}
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] in ("TRUE", "FALSE"):
            present[parts[0]] = parts[1] == "TRUE"
    note(OK, out.stdout.splitlines()[0].strip() if out.stdout else "R present")

    for pkg in ("testthat", "haven"):
        if not present.get(pkg):
            note(WARN, f"R package {pkg} missing", f'Rscript -e \'install.packages("{pkg}")\'')
    for pkg in ("fastsas", "fastsasconvert"):
        if not present.get(pkg):
            note(WARN, f"R package {pkg} not installed", "just test-r  (installs it)")


def check_versions() -> None:
    result = run(["bash", "scripts/check-versions.sh"])
    if result.returncode == 0:
        note(OK, "version couplings")
    else:
        bad = [line.strip() for line in result.stderr.splitlines() if line.strip()]
        note(BAD, "version couplings drifted", "; ".join(bad[:2]) or "bash scripts/check-versions.sh")


def check_hook() -> None:
    hooks = run(["git", "rev-parse", "--git-path", "hooks"]).stdout.strip()
    hook = Path(hooks)
    if not hook.is_absolute():
        hook = ROOT / hook
    if (hook / "commit-msg").exists():
        note(OK, "commit-msg hook installed")
    else:
        note(WARN, "commit-msg hook not installed",
             "just install-hooks  (git-cliff silently drops unconventional subjects)")


def check_ci() -> None:
    if not shutil.which("gh"):
        return
    result = run(["gh", "run", "list", "--workflow=ci", "--branch=main", "--limit=1",
                  "--json", "conclusion,displayTitle"])
    if result.returncode != 0 or not result.stdout.strip():
        return
    try:
        runs = json.loads(result.stdout)
    except json.JSONDecodeError:
        return
    if not runs:
        return
    conclusion = runs[0].get("conclusion") or "in progress"
    title = runs[0].get("displayTitle", "")[:52]
    if conclusion == "success":
        note(OK, f"CI on main: success ({title})")
    elif conclusion == "in progress":
        note(OK, f"CI on main: still running ({title})")
    else:
        note(BAD, f"CI on main: {conclusion}", f"{title}  ->  gh run view --log-failed")


def main() -> int:
    for check in (check_toolchain, check_python, check_fixtures, check_r,
                  check_versions, check_hook, check_ci):
        try:
            check()
        except Exception as exc:  # a broken check must not hide the others
            note(WARN, f"{check.__name__} could not run", str(exc))

    width = max(len(what) for _, what, _ in findings)
    for status, what, detail in findings:
        line = f"  {status}  {what.ljust(width)}"
        print(f"{line}   {detail}" if detail else line.rstrip())

    blocking = sum(1 for status, _, _ in findings if status == BAD)
    warnings = sum(1 for status, _, _ in findings if status == WARN)
    print()
    if blocking:
        print(f"  {blocking} blocking, {warnings} worth knowing about")
    elif warnings:
        print(f"  nothing blocking; {warnings} worth knowing about")
    else:
        print("  everything this checks is in place")
    return 1 if blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
