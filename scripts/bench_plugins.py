#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


def format_rows_per_second(rps: float) -> str:
    if rps >= 1_000_000:
        return f"{rps / 1_000_000:.2f}M"
    if rps >= 1_000:
        return f"{rps / 1_000:.1f}K"
    return f"{rps:.0f}"


def run_benchmarks(args: list[str]) -> dict[str, Any]:
    script_path = Path(__file__).parent / "compare_plugin_vs_raw.py"
    cmd = [".venv/bin/python", str(script_path), "--external-readers", "polars-native"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def reader_metric(
    result: dict[str, Any], key: str, external_key: str
) -> tuple[float | None, str]:
    if key in result:
        return result[key]["rows_per_second"], "sas7bdat-polars"

    external = result.get("external_readers", {})
    reader = external.get(external_key)
    if not reader or reader.get("status") != "ok":
        return None, external_key
    return reader[key]["rows_per_second"], external_key.replace("_", "-")


def print_reader_table(title: str, rows: list[tuple[str, float | None, float | None, float | None]]) -> None:
    print(f"\n### {title}")
    print("| Case | sas7bdat-polars | polars-readstat | polars-io |")
    print("| :--- | ---: | ---: | ---: |")

    for case_name, plugin_rps, readstat_rps, io_rps in rows:
        rps_values = [value for value in (plugin_rps, readstat_rps, io_rps) if value is not None]
        max_rps = max(rps_values) if rps_values else None

        def fmt(value: float | None) -> str:
            if value is None:
                return "N/A"
            display = format_rows_per_second(value)
            if max_rps is not None and value == max_rps:
                return f"**{display}**"
            return display

        print(f"| {case_name} | {fmt(plugin_rps)} | {fmt(readstat_rps)} | {fmt(io_rps)} |")


def print_single_case_table(data: dict[str, Any]) -> None:
    cold_rows = [
        (
            "cold_one_shot",
            data["plugin_cold_lazy_collect_once"]["rows_per_second"],
            data.get("external_readers", {}).get("polars_readstat", {}).get(
                "cold_lazy_collect_once", {}
            ).get("rows_per_second"),
            data.get("external_readers", {}).get("polars_io", {}).get(
                "cold_lazy_collect_once", {}
            ).get("rows_per_second"),
        )
    ]
    warm_rows = [
        (
            "warm_or_repeated",
            data["plugin_warm_lazy_collect"]["rows_per_second"],
            data.get("external_readers", {}).get("polars_readstat", {}).get(
                "repeated_fresh_lazy_collect", {}
            ).get("rows_per_second"),
            data.get("external_readers", {}).get("polars_io", {}).get(
                "repeated_fresh_lazy_collect", {}
            ).get("rows_per_second"),
        )
    ]

    print_reader_table("Cold One-Shot Lazy Collect", cold_rows)
    print_reader_table("Warm / Repeated Lazy Collect", warm_rows)
    print(f"\n*Fixture: `{data['raw']['fixture']}` ({data['raw']['rows_last']:,} rows)*")
    print(
        "*Cold is a fresh one-shot collect. Warm uses `SasDataset` reuse for "
        "`sas7bdat-polars`; external readers are repeated fresh scans.*"
    )


def print_suite_table(data: dict[str, Any]) -> None:
    cases = data.get("cases", [])
    cold_rows = []
    warm_rows = []
    for case in cases:
        name = case["name"]
        res = case["result"]
        external = res.get("external_readers", {})

        cold_rows.append(
            (
                name,
                res["plugin_cold_lazy_collect_once"]["rows_per_second"],
                external.get("polars_readstat", {}).get("cold_lazy_collect_once", {}).get(
                    "rows_per_second"
                ),
                external.get("polars_io", {}).get("cold_lazy_collect_once", {}).get(
                    "rows_per_second"
                ),
            )
        )
        warm_rows.append(
            (
                name,
                res["plugin_warm_lazy_collect"]["rows_per_second"],
                external.get("polars_readstat", {})
                .get("repeated_fresh_lazy_collect", {})
                .get("rows_per_second"),
                external.get("polars_io", {})
                .get("repeated_fresh_lazy_collect", {})
                .get("rows_per_second"),
            )
        )

    print_reader_table("Cold One-Shot Lazy Collect", cold_rows)
    print_reader_table("Warm / Repeated Lazy Collect", warm_rows)

    summary = data.get("summary", {})
    if summary:
        print("\n### 📈 Summary Statistics")
        overhead_ratio = summary['warm_lazy_collect_over_raw_avg_mean']
        efficiency = 1.0 / overhead_ratio if overhead_ratio > 0 else 0
        cases = data.get("cases", [])
        wins = 0
        counted = 0
        for case in cases:
            if not case.get("summary_included", True):
                continue
            res = case["result"]
            our_rps = res["plugin_warm_lazy_collect"]["rows_per_second"]
            external = res.get("external_readers", {})
            competitors = []
            if "polars_readstat" in external and external["polars_readstat"]["status"] == "ok":
                competitors.append(
                    external["polars_readstat"]["repeated_fresh_lazy_collect"][
                        "rows_per_second"
                    ]
                )
            if "polars_io" in external and external["polars_io"]["status"] == "ok":
                competitors.append(
                    external["polars_io"]["repeated_fresh_lazy_collect"][
                        "rows_per_second"
                    ]
                )
            if competitors:
                counted += 1
                if our_rps >= max(competitors):
                    wins += 1
        
        print(f"- **Plugin Efficiency**: {efficiency:.1%} of raw Rust engine throughput")
        print(f"- **Suite Cases**: {summary['summary_case_count']}")
        if counted:
            print(
                "- **Warm Reader Wins**: "
                f"sas7bdat-polars led {wins}/{counted} comparable cases"
            )

    print(
        "\n*All values are rows per second. Cold is a fresh one-shot collect. "
        "Warm uses `SasDataset` reuse for `sas7bdat-polars`; external readers "
        "are repeated fresh scans.*"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean benchmark summary for SAS7BDAT plugins")
    parser.add_argument("--fixture", help="Path to a single SAS7BDAT file")
    parser.add_argument("--suite", choices=["corpus-local"], help="Run a benchmark suite")
    parser.add_argument("--repeat", type=int, default=5, help="Number of repetitions")
    
    args, extra = parser.parse_known_args()
    
    bench_args = []
    if args.suite:
        bench_args.extend(["--suite", args.suite])
    if args.fixture:
        bench_args.extend(["--fixture", args.fixture])
    if args.repeat:
        bench_args.extend(["--repeat", str(args.repeat)])
    bench_args.extend(extra)
    
    print(f"Running benchmarks (repeat={args.repeat})...")
    data = run_benchmarks(bench_args)
    
    print(f"Context: batch_rows={data.get('batch_rows', 'N/A')}, limit={data.get('limit', 'N/A')}")
    
    if "cases" in data:
        print_suite_table(data)
    else:
        print_single_case_table(data)


if __name__ == "__main__":
    main()
