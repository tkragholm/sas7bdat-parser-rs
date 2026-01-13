#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PARSERS = {
    "rust": "benchmarks/runners/run_rust_bench.sh",
    "readstat": "benchmarks/runners/run_readstat.sh",
    "cpp": "benchmarks/runners/run_cpp.sh",
    "csharp": "benchmarks/runners/run_csharp.sh",
}

UNSUPPORTED_RE = re.compile(r"unsupported character set", re.IGNORECASE)

OUTPUT_PATTERNS = {
    "rows": re.compile(r"^Rows processed\s*:\s*(\d+)", re.IGNORECASE),
    "cols": re.compile(r"^Columns\s*:\s*(\d+)", re.IGNORECASE),
    "elapsed": re.compile(r"^Elapsed \(ms\)\s*:\s*([0-9.]+)", re.IGNORECASE),
}


@dataclass
class Result:
    parser: str
    fixture: str
    status: str
    elapsed_ms: float | None
    rows: int | None
    columns: int | None
    stderr: str | None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def list_fixtures(dirs: Iterable[Path], pattern: str | None) -> list[Path]:
    fixtures: list[Path] = []
    for directory in dirs:
        for path in sorted(directory.rglob("*.sas7bdat")):
            if pattern and not re.search(pattern, str(path)):
                continue
            fixtures.append(path)
    return fixtures


def parse_output(output: str) -> tuple[int | None, int | None, float | None]:
    rows = cols = elapsed = None
    for line in output.splitlines():
        if rows is None:
            match = OUTPUT_PATTERNS["rows"].match(line)
            if match:
                rows = int(match.group(1))
                continue
        if cols is None:
            match = OUTPUT_PATTERNS["cols"].match(line)
            if match:
                cols = int(match.group(1))
                continue
        if elapsed is None:
            match = OUTPUT_PATTERNS["elapsed"].match(line)
            if match:
                elapsed = float(match.group(1))
                continue
    return rows, cols, elapsed


def parser_available(parser: str, root: Path) -> tuple[bool, str | None]:
    if parser == "cpp":
        if subprocess.run(["cmake", "--version"], capture_output=True).returncode != 0:
            return False, "CMake not found"
    if parser == "csharp":
        if subprocess.run(["dotnet", "--info"], capture_output=True).returncode != 0:
            return False, ".NET SDK not found"
    if parser == "readstat":
        if not (root / "benchmarks" / "lib" / "c").exists():
            return False, "ReadStat sources not found"
    return True, None


def run_parser(parser: str, fixture: Path, root: Path) -> Result:
    script = root / PARSERS[parser]
    cmd = [str(script), str(fixture)]
    proc = subprocess.run(cmd, cwd=root, capture_output=True, text=True)

    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    if proc.returncode != 0:
        status = "unsupported" if UNSUPPORTED_RE.search(stderr) else "error"
        return Result(parser, str(fixture), status, None, None, None, stderr or None)

    rows, cols, elapsed = parse_output(stdout)
    return Result(parser, str(fixture), "ok", elapsed, rows, cols, stderr or None)


def build_all(root: Path, parsers: list[str], fixture: Path) -> dict[str, str | None]:
    availability: dict[str, str | None] = {}
    for parser in parsers:
        ok, reason = parser_available(parser, root)
        if not ok:
            availability[parser] = reason
            continue
        script = root / PARSERS[parser]
        proc = subprocess.run(
            [str(script), "--build-only", str(fixture)],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            availability[parser] = proc.stderr.strip() or proc.stdout.strip() or "build failed"
        else:
            availability[parser] = None
    return availability


def summarize(results: list[Result]) -> None:
    by_parser: dict[str, dict[str, int]] = {}
    for result in results:
        summary = by_parser.setdefault(
            result.parser, {"ok": 0, "error": 0, "unsupported": 0, "unavailable": 0}
        )
        summary[result.status] += 1

    print("Summary:")
    for parser, summary in sorted(by_parser.items()):
        print(
            f"  {parser:8} ok={summary['ok']} unsupported={summary['unsupported']} "
            f"error={summary['error']} unavailable={summary['unavailable']}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run benchmark suite across fixtures.")
    parser.add_argument(
        "--fixtures-dir",
        action="append",
        type=Path,
        help="Directory containing .sas7bdat fixtures (repeatable)",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=None,
        help="Regex filter applied to fixture paths",
    )
    parser.add_argument(
        "--parsers",
        nargs="+",
        choices=sorted(PARSERS.keys()),
        default=sorted(PARSERS.keys()),
        help="Parsers to run",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output file",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of fixtures",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip build-only pass",
    )
    args = parser.parse_args()

    root = repo_root()
    dirs = args.fixtures_dir or [root / "fixtures" / "raw_data"]

    fixtures = list_fixtures([d.resolve() for d in dirs], args.pattern)
    if args.limit:
        fixtures = fixtures[: args.limit]

    if not fixtures:
        print("No fixtures found.")
        return 1

    availability: dict[str, str | None] = {parser: None for parser in args.parsers}
    if not args.no_build:
        availability = build_all(root, args.parsers, fixtures[0])

    results: list[Result] = []
    for fixture in fixtures:
        for parser_name in args.parsers:
            reason = availability.get(parser_name)
            if reason:
                result = Result(
                    parser_name,
                    str(fixture),
                    "unavailable",
                    None,
                    None,
                    None,
                    reason,
                )
            else:
                result = run_parser(parser_name, fixture, root)
            results.append(result)
            status = result.status
            elapsed = f"{result.elapsed_ms:.2f}ms" if result.elapsed_ms is not None else "-"
            print(f"{parser_name:8} {status:11} {elapsed:>10} {fixture}")

    summarize(results)

    if args.output:
        data = [result.__dict__ for result in results]
        args.output.write_text(json.dumps(data, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
