#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

PARSERS = {
    "rust": "benchmarks/runners/run_rust_bench.sh",
    "readstat": "benchmarks/runners/run_readstat.sh",
    "cpp": "benchmarks/runners/run_cpp.sh",
    "csharp": "benchmarks/runners/run_csharp.sh",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build benchmark harnesses.")
    parser.add_argument(
        "--fixture",
        type=Path,
        default=None,
        help="Fixture path passed to build scripts",
    )
    parser.add_argument(
        "--parsers",
        nargs="+",
        choices=sorted(PARSERS.keys()),
        default=sorted(PARSERS.keys()),
        help="Parsers to build",
    )
    parser.add_argument(
        "--allow-fail",
        action="store_true",
        help="Exit zero even if some builds fail",
    )
    args = parser.parse_args()

    root = repo_root()
    fixture = args.fixture or (root / "fixtures" / "raw_data" / "pandas" / "airline.sas7bdat")
    fixture = fixture.resolve()

    if not fixture.exists():
        parser.error(f"fixture not found: {fixture}")

    failures: list[str] = []

    for name in args.parsers:
        script = root / PARSERS[name]
        proc = subprocess.run(
            [str(script), "--build-only", str(fixture)],
            cwd=root,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            print(f"{name:8} ok")
        else:
            msg = proc.stderr.strip() or proc.stdout.strip() or "build failed"
            print(f"{name:8} error: {msg}")
            failures.append(name)

    if failures and not args.allow_fail:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
