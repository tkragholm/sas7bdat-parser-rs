#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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


def format_ci(mid: str, low: str, high: str) -> str:
    return f"{mid} [{low}; {high}]"


def parse_change_estimate(path: Path) -> Tuple[float, float, float]:
    data = json.loads(path.read_text())
    ci = data["mean"]["confidence_interval"]
    return (
        float(ci["lower_bound"]),
        float(data["mean"]["point_estimate"]),
        float(ci["upper_bound"]),
    )


def classify_change(point_estimate: float, noise_threshold: float) -> str:
    if point_estimate < -noise_threshold:
        return "improved"
    if point_estimate <= noise_threshold:
        return "within_noise"
    return "regressed"


def get_commit_id(explicit_commit_id: Optional[str]) -> str:
    if explicit_commit_id:
        return explicit_commit_id
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                text=True,
            )
            .strip()
        )
    except Exception:
        return "unknown"


def gather_rows(
    criterion_root: Path, noise_threshold: float, commit_id: str
) -> Tuple[List[Dict[str, str]], List[str]]:
    grouped: Dict[str, Dict[str, str]] = {}
    regressions: List[str] = []
    for bench_json in criterion_root.glob(
        "top3_target*/typed_batches/all/new/benchmark.json"
    ):
        group = json.loads(bench_json.read_text())["group_id"]
        base_dir = bench_json.parents[3]
        batch_estimates = base_dir / "typed_batches" / "all" / "new" / "estimates.json"
        batch_change_estimates = (
            base_dir / "typed_batches" / "all" / "change" / "estimates.json"
        )

        if not (batch_estimates.exists() and batch_change_estimates.exists()):
            continue

        batch_low, batch_mid, batch_high = parse_estimates(batch_estimates)
        change_low, change_mid, change_high = parse_change_estimate(batch_change_estimates)
        elems = parse_elements(bench_json)

        batch_thrpt_low = elems * 1_000_000_000.0 / batch_high
        batch_thrpt_mid = elems * 1_000_000_000.0 / batch_mid
        batch_thrpt_high = elems * 1_000_000_000.0 / batch_low

        # Throughput relative change is inverse of time: (1 / (1 + dt)) - 1
        thrpt_change_low = (1.0 / (1.0 + change_high)) - 1.0
        thrpt_change_mid = (1.0 / (1.0 + change_mid)) - 1.0
        thrpt_change_high = (1.0 / (1.0 + change_low)) - 1.0

        runtime_status = classify_change(change_mid, noise_threshold)
        # For throughput, positive is good, so classify the negated value.
        thrpt_status = classify_change(-thrpt_change_mid, noise_threshold)
        if runtime_status == "regressed" or thrpt_status == "regressed":
            regressions.append(
                f"{group}: runtime_change={change_mid:+.3%} "
                f"throughput_change={thrpt_change_mid:+.3%}"
            )
            continue

        grouped[group] = {
            "filename": f"`{group.rsplit('/', 1)[-1]}`",
            "runtime": format_ci(
                format_time_ns(batch_mid),
                format_time_ns(batch_low),
                format_time_ns(batch_high),
            ),
            "thrpt": format_ci(
                format_throughput(batch_thrpt_mid),
                format_throughput(batch_thrpt_low),
                format_throughput(batch_thrpt_high),
            ),
            "commit_id": f"`{commit_id}`",
        }

    rows = list(grouped.values())
    rows.sort(key=lambda r: r["filename"])
    return rows, regressions


def render_table(rows: List[Dict[str, str]]) -> str:
    lines = [
        START_MARKER,
        "",
        "| filename | runtime | thrpt | commit-id |",
        "| --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['filename']} | {row['runtime']} | {row['thrpt']} | {row['commit_id']} |"
        )
    lines.extend(["", END_MARKER])
    return "\n".join(lines)


def parse_existing_rows(readme: Path) -> Dict[str, Dict[str, str]]:
    text = readme.read_text()
    start = text.find(START_MARKER)
    end = text.find(END_MARKER)
    if start == -1 or end == -1 or end < start:
        return {}
    block = text[start:end]
    rows: Dict[str, Dict[str, str]] = {}
    for line in block.splitlines():
        striped = line.strip()
        if not striped.startswith("|"):
            continue
        cells = [cell.strip() for cell in striped.strip("|").split("|")]
        if len(cells) != 4:
            continue
        if cells[0].lower() == "filename" or cells[0] == "---":
            continue
        rows[cells[0]] = {
            "runtime": cells[1],
            "thrpt": cells[2],
            "commit_id": cells[3],
        }
    return rows


def preserve_commit_for_unchanged_rows(
    rows: List[Dict[str, str]], existing: Dict[str, Dict[str, str]]
) -> None:
    for row in rows:
        previous = existing.get(row["filename"])
        if previous is None:
            continue
        if previous["runtime"] == row["runtime"] and previous["thrpt"] == row["thrpt"]:
            row["commit_id"] = previous["commit_id"]


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
    parser.add_argument(
        "--noise-threshold",
        type=float,
        default=0.01,
        help="Relative noise threshold used to classify change (default: 0.01 = 1%%).",
    )
    parser.add_argument(
        "--commit-id",
        default=None,
        help="Commit id to include in table (default: current `git rev-parse --short HEAD`).",
    )
    args = parser.parse_args()

    commit_id = get_commit_id(args.commit_id)
    rows, regressions = gather_rows(
        Path(args.criterion_root), args.noise_threshold, commit_id
    )
    if regressions:
        print("Skipped README table update due to regressions:")
        for line in regressions:
            print(f"- {line}")
        raise SystemExit(0)
    if not rows:
        raise SystemExit(
            "No eligible top3_target typed_batches runs found to summarize."
        )
    existing_rows = parse_existing_rows(Path(args.readme))
    preserve_commit_for_unchanged_rows(rows, existing_rows)
    update_readme(Path(args.readme), render_table(rows))
    print(f"Updated {args.readme} with {len(rows)} top3 rows.")


if __name__ == "__main__":
    main()
