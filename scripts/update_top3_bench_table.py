#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

START_MARKER = "<!-- TOP3_BENCH_TABLE:START -->"
END_MARKER = "<!-- TOP3_BENCH_TABLE:END -->"


def format_time_ns(ns: float) -> str:
    if ns < 1_000.0:
        return f"{ns:.2f} ns"
    if ns < 1_000_000.0:
        return f"{ns / 1_000.0:.2f} µs"
    if ns < 1_000_000_000.0:
        return f"{ns / 1_000_000.0:.2f} ms"
    return f"{ns / 1_000_000_000.0:.2f} s"


def format_throughput(elems_per_sec: float) -> str:
    if elems_per_sec < 1_000.0:
        return f"{elems_per_sec:.2f} elem/s"
    if elems_per_sec < 1_000_000.0:
        return f"{elems_per_sec / 1_000.0:.2f} Kelem/s"
    if elems_per_sec < 1_000_000_000.0:
        return f"{elems_per_sec / 1_000_000.0:.2f} Melem/s"
    return f"{elems_per_sec / 1_000_000_000.0:.2f} Gelem/s"


def parse_estimates(path: Path) -> Tuple[float, float, float]:
    data = json.loads(path.read_text())
    ci = data["mean"]["confidence_interval"]
    return (
        float(ci["lower_bound"]),
        float(data["mean"]["point_estimate"]),
        float(ci["upper_bound"]),
    )


def parse_elements(path: Path) -> int:
    data = json.loads(path.read_text())
    return int(data["throughput"]["Elements"])


def gather_rows(criterion_root: Path) -> List[Dict[str, str]]:
    grouped: Dict[str, Dict[str, str]] = {}
    for bench_json in criterion_root.glob(
        "top3_target*/raw_rows/all/new/benchmark.json"
    ):
        group = json.loads(bench_json.read_text())["group_id"]
        base_dir = bench_json.parents[3]
        raw_estimates = base_dir / "raw_rows" / "all" / "new" / "estimates.json"
        batch_estimates = (
            base_dir / "typed_batches" / "all" / "new" / "estimates.json"
        )
        batch_bench = base_dir / "typed_batches" / "all" / "new" / "benchmark.json"
        if not (raw_estimates.exists() and batch_estimates.exists() and batch_bench.exists()):
            continue

        raw_low, raw_mid, raw_high = parse_estimates(raw_estimates)
        batch_low, batch_mid, batch_high = parse_estimates(batch_estimates)
        elems = parse_elements(bench_json)

        raw_thrpt_low = elems * 1_000_000_000.0 / raw_high
        raw_thrpt_mid = elems * 1_000_000_000.0 / raw_mid
        raw_thrpt_high = elems * 1_000_000_000.0 / raw_low
        batch_thrpt_low = elems * 1_000_000_000.0 / batch_high
        batch_thrpt_mid = elems * 1_000_000_000.0 / batch_mid
        batch_thrpt_high = elems * 1_000_000_000.0 / batch_low

        grouped[group] = {
            "fixture": f"`{group}`",
            "raw_time": f"[{format_time_ns(raw_low)} {format_time_ns(raw_mid)} {format_time_ns(raw_high)}]",
            "raw_thrpt": f"[{format_throughput(raw_thrpt_low)} {format_throughput(raw_thrpt_mid)} {format_throughput(raw_thrpt_high)}]",
            "batch_time": f"[{format_time_ns(batch_low)} {format_time_ns(batch_mid)} {format_time_ns(batch_high)}]",
            "batch_thrpt": f"[{format_throughput(batch_thrpt_low)} {format_throughput(batch_thrpt_mid)} {format_throughput(batch_thrpt_high)}]",
            "notes": "auto-generated from `target/criterion/*/new/estimates.json`",
        }

    rows = list(grouped.values())
    rows.sort(key=lambda r: r["fixture"])
    return rows


def render_table(rows: List[Dict[str, str]]) -> str:
    lines = [
        START_MARKER,
        "",
        "| Fixture | raw_rows time | raw_rows throughput | typed_batches time | typed_batches throughput | Notes |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['fixture']} | {row['raw_time']} | {row['raw_thrpt']} | {row['batch_time']} | {row['batch_thrpt']} | {row['notes']} |"
        )
    lines.extend(["", END_MARKER])
    return "\n".join(lines)


def update_readme(readme: Path, table_block: str) -> None:
    text = readme.read_text()
    start = text.find(START_MARKER)
    end = text.find(END_MARKER)
    if start == -1 or end == -1 or end < start:
        raise SystemExit(
            "README markers not found. Expected TOP3_BENCH_TABLE start/end markers."
        )
    end += len(END_MARKER)
    updated = text[:start] + table_block + text[end:]
    readme.write_text(updated)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh README top3 benchmark table from Criterion outputs."
    )
    parser.add_argument(
        "--criterion-root", default="target/criterion", help="Criterion output root"
    )
    parser.add_argument("--readme", default="README.md", help="README path")
    args = parser.parse_args()

    rows = gather_rows(Path(args.criterion_root))
    if not rows:
        raise SystemExit("No top3_target Criterion runs found to summarize.")
    update_readme(Path(args.readme), render_table(rows))
    print(f"Updated {args.readme} with {len(rows)} top3 rows.")


if __name__ == "__main__":
    main()
