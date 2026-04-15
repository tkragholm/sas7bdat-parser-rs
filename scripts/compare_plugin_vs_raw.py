#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import polars as pl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare sas7bdat-polars batch_reader against the raw Rust batch scan."
    )
    parser.add_argument(
        "--fixture",
        default="fixtures/ahs2013n.sas7bdat",
        help="SAS7BDAT fixture path",
    )
    parser.add_argument(
        "--columns",
        default="CONTROL,DEGREE,LMED",
        help="Comma-separated projection columns",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=5,
        help="Number of timed repetitions",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=4096,
        help="Batch hint for both measurements",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit, 0 disables it",
    )
    parser.add_argument(
        "--suite",
        choices=["single", "corpus-local"],
        default="single",
        help="Benchmark a single fixture or a local corpus-shaped suite",
    )
    parser.add_argument(
        "--catalog",
        default="fixtures/fixture_catalog.local.json",
        help="Fixture catalog path used for corpus-local suite selection",
    )
    return parser.parse_args()


def load_catalog(catalog_path: Path) -> list[dict[str, Any]]:
    return json.loads(catalog_path.read_text())["fixtures"]


def sample_ascii_ratio(fixture: dict[str, Any]) -> float | None:
    sample = fixture.get("sample", {})
    string_cells = sample.get("string_cells", 0)
    if string_cells <= 0:
        return None
    return sample.get("ascii_string_cells", 0) / string_cells


def is_string_dtype(dtype: pl.DataType) -> bool:
    return dtype in {pl.String, pl.Binary}


def is_numeric_like_dtype(dtype: pl.DataType) -> bool:
    return dtype.is_numeric() or dtype.is_temporal()


def resolve_projection_columns(fixture: str, preset: str) -> list[str]:
    import sas7bdat_polars as sp  # noqa: E402

    if preset == "full":
        return []

    schema = sp.schema_for_file(fixture)
    columns = list(schema.items())
    string_columns = [name for name, dtype in columns if is_string_dtype(dtype)]
    numeric_columns = [name for name, dtype in columns if is_numeric_like_dtype(dtype)]

    if preset == "strings":
        return string_columns
    if preset == "numeric":
        return numeric_columns
    if preset == "mixed":
        return numeric_columns[:4] + string_columns[:4]
    raise ValueError(f"unsupported projection preset: {preset}")


def fixture_numeric_count(fixture: dict[str, Any]) -> int:
    logical_types = fixture.get("logical_types", {})
    return sum(
        logical_types.get(key, 0)
        for key in ("integer", "float", "date", "datetime", "time")
    )


def choose_fixture(
    fixtures: list[dict[str, Any]], predicate, *, sort_key
) -> dict[str, Any]:
    matches = [fixture for fixture in fixtures if fixture.get("status") == "profiled" and predicate(fixture)]
    if not matches:
        raise ValueError("no fixture matched the requested corpus benchmark class")
    return max(matches, key=sort_key)


def build_corpus_suite_cases(catalog_path: Path) -> list[dict[str, Any]]:
    fixtures = load_catalog(catalog_path)
    cases = [
        {
            "name": "macro_mixed_projection",
            "fixture": choose_fixture(
                fixtures,
                lambda fixture: "benchmark-macro" in fixture.get("tags", []),
                sort_key=lambda fixture: (fixture.get("size_bytes", 0), fixture.get("row_count", 0)),
            )["path"],
            "projection": "mixed",
            "summary_included": True,
        },
        {
            "name": "legacy_string_heavy_wide",
            "fixture": choose_fixture(
                fixtures,
                lambda fixture: "benchmark-standard" in fixture.get("tags", [])
                and "string-heavy" in fixture.get("tags", [])
                and "wide" in fixture.get("tags", []),
                sort_key=lambda fixture: (
                    fixture.get("logical_types", {}).get("string", 0),
                    fixture.get("row_count", 0),
                    fixture.get("size_bytes", 0),
                ),
            )["path"],
            "projection": "strings",
            "summary_included": True,
        },
        {
            "name": "legacy_string_heavy_narrow",
            "fixture": choose_fixture(
                fixtures,
                lambda fixture: "benchmark-standard" in fixture.get("tags", [])
                and "string-heavy" in fixture.get("tags", [])
                and "narrow" in fixture.get("tags", []),
                sort_key=lambda fixture: (
                    fixture.get("row_count", 0),
                    fixture.get("logical_types", {}).get("string", 0),
                    fixture.get("size_bytes", 0),
                ),
            )["path"],
            "projection": "strings",
            "summary_included": True,
        },
        {
            "name": "legacy_numeric_heavy_wide",
            "fixture": choose_fixture(
                fixtures,
                lambda fixture: "benchmark-standard" in fixture.get("tags", [])
                and "numeric-heavy" in fixture.get("tags", [])
                and "legacy-encoding" in fixture.get("tags", [])
                and "wide" in fixture.get("tags", []),
                sort_key=lambda fixture: (
                    fixture.get("row_count", 0),
                    fixture_numeric_count(fixture),
                    fixture.get("size_bytes", 0),
                ),
            )["path"],
            "projection": "numeric",
            "summary_included": True,
        },
        {
            "name": "legacy_non_ascii_probe",
            "fixture": choose_fixture(
                fixtures,
                lambda fixture: fixture.get("sample", {}).get("non_ascii_string_cells", 0) > 0,
                sort_key=lambda fixture: (
                    fixture.get("sample", {}).get("non_ascii_string_cells", 0),
                    fixture.get("row_count", 0),
                    fixture.get("size_bytes", 0),
                ),
            )["path"],
            "projection": "mixed",
            "summary_included": False,
        },
    ]

    for case in cases:
        fixture = next(entry for entry in fixtures if entry["path"] == case["fixture"])
        case["tags"] = fixture.get("tags", [])
        case["ascii_ratio"] = sample_ascii_ratio(fixture)
        case["row_count"] = fixture.get("row_count")
        case["size_bytes"] = fixture.get("size_bytes")
    return cases


def run_raw(
    repo_root: Path,
    fixture: str,
    columns: str,
    repeat: int,
    batch_rows: int,
    limit: int,
) -> dict[str, object]:
    cmd = [
        "cargo",
        "run",
        "--quiet",
        "--release",
        "--features",
        "arrow",
        "--example",
        "raw_batch_compare",
        "--",
        "--fixture",
        fixture,
        "--columns",
        columns,
        "--repeat",
        str(repeat),
        "--batch-rows",
        str(batch_rows),
    ]
    if limit > 0:
        cmd.extend(["--limit", str(limit)])

    completed = subprocess.run(
        cmd,
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(completed.stdout)


def run_raw_owned(
    repo_root: Path,
    fixture: str,
    columns: str,
    repeat: int,
    batch_rows: int,
    limit: int,
) -> dict[str, object]:
    cmd = [
        "cargo",
        "run",
        "--quiet",
        "--release",
        "--features",
        "arrow",
        "--example",
        "raw_owned_batch_compare",
        "--",
        "--fixture",
        fixture,
        "--columns",
        columns,
        "--repeat",
        str(repeat),
        "--batch-rows",
        str(batch_rows),
    ]
    if limit > 0:
        cmd.extend(["--limit", str(limit)])

    completed = subprocess.run(
        cmd,
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(completed.stdout)


def run_plugin_cold_batch_reader(
    fixture: str, columns: list[str], repeat: int, batch_rows: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    start = time.perf_counter_ns()
    rows_last = 0
    batches_last = 0
    for _ in range(repeat):
        reader = sp.batch_reader(
            fixture,
            columns,
            None,
            limit if limit > 0 else None,
            batch_rows,
        )
        rows_last = 0
        batches_last = 0
        for frame in reader:
            rows_last += frame.height
            batches_last += 1
    elapsed_total = time.perf_counter_ns() - start
    elapsed_avg = elapsed_total // repeat
    seconds = elapsed_avg / 1_000_000_000.0
    rows_per_second = rows_last / seconds if seconds > 0.0 else 0.0
    batches_per_second = batches_last / seconds if seconds > 0.0 else 0.0

    return {
        "fixture": fixture,
        "columns": columns,
        "repeat": repeat,
        "batch_rows": batch_rows,
        "limit": None if limit <= 0 else limit,
        "elapsed_ns_total": elapsed_total,
        "elapsed_ns_avg": elapsed_avg,
        "rows_last": rows_last,
        "batches_last": batches_last,
        "rows_per_second": rows_per_second,
        "batches_per_second": batches_per_second,
    }


def run_plugin_warm_batch_reader(
    fixture: str, columns: list[str], repeat: int, batch_rows: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    ds_start = time.perf_counter_ns()
    ds = sp.SasDataset(fixture)
    open_elapsed = time.perf_counter_ns() - ds_start

    prime_start = time.perf_counter_ns()
    prime_reader = ds.batch_reader(
        columns,
        None,
        limit if limit > 0 else None,
        batch_rows,
    )
    prime_rows = sum(frame.height for frame in prime_reader)
    prime_elapsed = time.perf_counter_ns() - prime_start

    start = time.perf_counter_ns()
    rows_last = 0
    batches_last = 0
    for _ in range(repeat):
        reader = ds.batch_reader(
            columns,
            None,
            limit if limit > 0 else None,
            batch_rows,
        )
        rows_last = 0
        batches_last = 0
        for frame in reader:
            rows_last += frame.height
            batches_last += 1
    elapsed_total = time.perf_counter_ns() - start
    elapsed_avg = elapsed_total // repeat
    seconds = elapsed_avg / 1_000_000_000.0
    rows_per_second = rows_last / seconds if seconds > 0.0 else 0.0
    batches_per_second = batches_last / seconds if seconds > 0.0 else 0.0

    return {
        "fixture": fixture,
        "columns": columns,
        "repeat": repeat,
        "batch_rows": batch_rows,
        "limit": None if limit <= 0 else limit,
        "dataset_open_ns": open_elapsed,
        "priming_ns": prime_elapsed,
        "priming_rows": prime_rows,
        "steady_elapsed_ns_total": elapsed_total,
        "steady_elapsed_ns_avg": elapsed_avg,
        "rows_last": rows_last,
        "batches_last": batches_last,
        "rows_per_second": rows_per_second,
        "batches_per_second": batches_per_second,
    }


def run_plugin_inner_batch_to_dataframe(
    fixture: str, columns: list[str], repeat: int, batch_rows: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    result = sp.benchmark_batch_to_dataframe(
        fixture,
        columns,
        limit if limit > 0 else None,
        batch_rows,
        repeat,
    )
    result["columns"] = columns
    return result


def run_plugin_inner_dataframe_to_python(
    fixture: str, columns: list[str], repeat: int, batch_rows: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    result = sp.benchmark_dataframe_to_python(
        fixture,
        columns,
        limit if limit > 0 else None,
        batch_rows,
        repeat,
    )
    result["columns"] = columns
    return result


def run_plugin_inner_scan_to_dataframes(
    fixture: str, columns: list[str], repeat: int, batch_rows: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    result = sp.benchmark_scan_to_dataframes(
        fixture,
        columns,
        limit if limit > 0 else None,
        batch_rows,
        repeat,
    )
    result["columns"] = columns
    return result


def run_plugin_cold_lazy_collect(
    fixture: str, columns: list[str], repeat: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    start = time.perf_counter_ns()
    rows_last = 0
    for _ in range(repeat):
        lf = sp.scan_sas(fixture)
        if columns:
            lf = lf.select(columns)
        if limit > 0:
            lf = lf.head(limit)
        df = lf.collect()
        rows_last = df.height
    elapsed_total = time.perf_counter_ns() - start
    elapsed_avg = elapsed_total // repeat
    seconds = elapsed_avg / 1_000_000_000.0
    rows_per_second = rows_last / seconds if seconds > 0.0 else 0.0
    return {
        "fixture": fixture,
        "columns": columns,
        "repeat": repeat,
        "limit": None if limit <= 0 else limit,
        "elapsed_ns_total": elapsed_total,
        "elapsed_ns_avg": elapsed_avg,
        "rows_last": rows_last,
        "rows_per_second": rows_per_second,
    }


def run_plugin_warm_lazy_collect(
    fixture: str, columns: list[str], repeat: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    ds_start = time.perf_counter_ns()
    ds = sp.SasDataset(fixture)
    open_elapsed = time.perf_counter_ns() - ds_start

    prime_start = time.perf_counter_ns()
    prime_lf = ds.scan_sas()
    if columns:
        prime_lf = prime_lf.select(columns)
    if limit > 0:
        prime_lf = prime_lf.head(limit)
    prime_df = prime_lf.collect()
    prime_elapsed = time.perf_counter_ns() - prime_start

    start = time.perf_counter_ns()
    rows_last = 0
    for _ in range(repeat):
        lf = ds.scan_sas()
        if columns:
            lf = lf.select(columns)
        if limit > 0:
            lf = lf.head(limit)
        df = lf.collect()
        rows_last = df.height
    elapsed_total = time.perf_counter_ns() - start
    elapsed_avg = elapsed_total // repeat
    seconds = elapsed_avg / 1_000_000_000.0
    rows_per_second = rows_last / seconds if seconds > 0.0 else 0.0
    return {
        "fixture": fixture,
        "columns": columns,
        "repeat": repeat,
        "limit": None if limit <= 0 else limit,
        "dataset_open_ns": open_elapsed,
        "priming_ns": prime_elapsed,
        "priming_rows": prime_df.height,
        "steady_elapsed_ns_total": elapsed_total,
        "steady_elapsed_ns_avg": elapsed_avg,
        "rows_last": rows_last,
        "rows_per_second": rows_per_second,
    }


def benchmark_case(
    repo_root: Path,
    fixture: str,
    columns: list[str],
    repeat: int,
    batch_rows: int,
    limit: int,
) -> dict[str, Any]:
    columns_arg = ",".join(columns)
    raw = run_raw(repo_root, fixture, columns_arg, repeat, batch_rows, limit)
    raw_owned = run_raw_owned(repo_root, fixture, columns_arg, repeat, batch_rows, limit)
    plugin_cold_batch_reader = run_plugin_cold_batch_reader(
        fixture, columns, repeat, batch_rows, limit
    )
    plugin_warm_batch_reader = run_plugin_warm_batch_reader(
        fixture, columns, repeat, batch_rows, limit
    )
    plugin_inner_batch_to_dataframe = run_plugin_inner_batch_to_dataframe(
        fixture, columns, repeat, batch_rows, limit
    )
    plugin_inner_scan_to_dataframes = run_plugin_inner_scan_to_dataframes(
        fixture, columns, repeat, batch_rows, limit
    )
    plugin_inner_dataframe_to_python = run_plugin_inner_dataframe_to_python(
        fixture, columns, repeat, batch_rows, limit
    )
    plugin_cold_lazy_collect = run_plugin_cold_lazy_collect(
        fixture, columns, repeat, limit
    )
    plugin_warm_lazy_collect = run_plugin_warm_lazy_collect(
        fixture, columns, repeat, limit
    )

    raw_avg = raw["elapsed_ns_avg"]
    return {
        "raw": raw,
        "raw_owned": raw_owned,
        "plugin_cold_batch_reader": plugin_cold_batch_reader,
        "plugin_warm_batch_reader": plugin_warm_batch_reader,
        "plugin_inner_batch_to_dataframe": plugin_inner_batch_to_dataframe,
        "plugin_inner_scan_to_dataframes": plugin_inner_scan_to_dataframes,
        "plugin_inner_dataframe_to_python": plugin_inner_dataframe_to_python,
        "plugin_cold_lazy_collect": plugin_cold_lazy_collect,
        "plugin_warm_lazy_collect": plugin_warm_lazy_collect,
        "ratios": {
            "raw_owned_over_raw_avg": raw_owned["elapsed_ns_avg"] / raw_avg if raw_avg else None,
            "cold_batch_reader_over_raw_avg": (
                plugin_cold_batch_reader["elapsed_ns_avg"] / raw_avg if raw_avg else None
            ),
            "warm_batch_reader_over_raw_avg": (
                plugin_warm_batch_reader["steady_elapsed_ns_avg"] / raw_avg if raw_avg else None
            ),
            "inner_batch_to_dataframe_over_raw_avg": (
                plugin_inner_batch_to_dataframe["steady_elapsed_ns_avg"] / raw_avg
                if raw_avg
                else None
            ),
            "inner_scan_to_dataframes_over_raw_avg": (
                plugin_inner_scan_to_dataframes["steady_elapsed_ns_avg"] / raw_avg
                if raw_avg
                else None
            ),
            "inner_dataframe_to_python_over_raw_avg": (
                plugin_inner_dataframe_to_python["steady_elapsed_ns_avg"] / raw_avg
                if raw_avg
                else None
            ),
            "warm_batch_reader_over_inner_batch_to_dataframe": (
                plugin_warm_batch_reader["steady_elapsed_ns_avg"]
                / plugin_inner_batch_to_dataframe["steady_elapsed_ns_avg"]
                if plugin_inner_batch_to_dataframe["steady_elapsed_ns_avg"]
                else None
            ),
            "warm_batch_reader_over_inner_scan_to_dataframes": (
                plugin_warm_batch_reader["steady_elapsed_ns_avg"]
                / plugin_inner_scan_to_dataframes["steady_elapsed_ns_avg"]
                if plugin_inner_scan_to_dataframes["steady_elapsed_ns_avg"]
                else None
            ),
            "warm_batch_reader_over_inner_dataframe_to_python": (
                plugin_warm_batch_reader["steady_elapsed_ns_avg"]
                / plugin_inner_dataframe_to_python["steady_elapsed_ns_avg"]
                if plugin_inner_dataframe_to_python["steady_elapsed_ns_avg"]
                else None
            ),
            "warm_batch_reader_over_raw_owned_avg": (
                plugin_warm_batch_reader["steady_elapsed_ns_avg"] / raw_owned["elapsed_ns_avg"]
                if raw_owned["elapsed_ns_avg"]
                else None
            ),
            "cold_lazy_collect_over_raw_avg": (
                plugin_cold_lazy_collect["elapsed_ns_avg"] / raw_avg if raw_avg else None
            ),
            "warm_lazy_collect_over_raw_avg": (
                plugin_warm_lazy_collect["steady_elapsed_ns_avg"] / raw_avg if raw_avg else None
            ),
        },
    }


def compact_case_result_for_suite(result: dict[str, Any]) -> dict[str, Any]:
    compact = json.loads(json.dumps(result))
    for key, value in compact.items():
        if isinstance(value, dict) and "columns" in value:
            value.pop("columns")
    return compact


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    if args.suite == "corpus-local":
        catalog_path = (repo_root / args.catalog).resolve()
        cases = []
        for case in build_corpus_suite_cases(catalog_path):
            columns = resolve_projection_columns(case["fixture"], case["projection"])
            cases.append(
                {
                    "name": case["name"],
                    "fixture": case["fixture"],
                    "projection": case["projection"],
                    "column_count": len(columns),
                    "column_preview": columns[:8],
                    "tags": case["tags"],
                    "ascii_ratio": case["ascii_ratio"],
                    "row_count": case["row_count"],
                    "size_bytes": case["size_bytes"],
                    "summary_included": case["summary_included"],
                    "result": compact_case_result_for_suite(
                        benchmark_case(
                            repo_root,
                            case["fixture"],
                            columns,
                            args.repeat,
                            args.batch_rows,
                            args.limit,
                        )
                    ),
                }
            )

        summary_cases = [case for case in cases if case["summary_included"]]
        summary = {
            "case_count": len(cases),
            "summary_case_count": len(summary_cases),
            "summary_case_names": [case["name"] for case in summary_cases],
            "warm_batch_reader_over_raw_avg_mean": sum(
                case["result"]["ratios"]["warm_batch_reader_over_raw_avg"]
                for case in summary_cases
            )
            / len(summary_cases),
            "warm_lazy_collect_over_raw_avg_mean": sum(
                case["result"]["ratios"]["warm_lazy_collect_over_raw_avg"]
                for case in summary_cases
            )
            / len(summary_cases),
            "inner_batch_to_dataframe_over_raw_avg_mean": sum(
                case["result"]["ratios"]["inner_batch_to_dataframe_over_raw_avg"]
                for case in summary_cases
            )
            / len(summary_cases),
        }
        print(
            json.dumps(
                {
                    "suite": args.suite,
                    "repeat": args.repeat,
                    "batch_rows": args.batch_rows,
                    "limit": None if args.limit <= 0 else args.limit,
                    "cases": cases,
                    "summary": summary,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    columns_arg = "" if args.columns == "__ALL__" else args.columns
    columns = [column.strip() for column in columns_arg.split(",") if column.strip()]
    print(
        json.dumps(
            benchmark_case(
                repo_root,
                args.fixture,
                columns,
                args.repeat,
                args.batch_rows,
                args.limit,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
