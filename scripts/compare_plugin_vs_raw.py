#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

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
    return parser.parse_args()


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


def run_plugin_cold_lazy_collect(
    fixture: str, columns: list[str], repeat: int, limit: int
) -> dict[str, object]:
    import sas7bdat_polars as sp  # noqa: E402

    start = time.perf_counter_ns()
    rows_last = 0
    for _ in range(repeat):
        lf = sp.scan_sas(fixture).select(columns)
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
    prime_lf = ds.scan_sas().select(columns)
    if limit > 0:
        prime_lf = prime_lf.head(limit)
    prime_df = prime_lf.collect()
    prime_elapsed = time.perf_counter_ns() - prime_start

    start = time.perf_counter_ns()
    rows_last = 0
    for _ in range(repeat):
        lf = ds.scan_sas().select(columns)
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


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    columns = [column.strip() for column in args.columns.split(",") if column.strip()]

    raw = run_raw(repo_root, args.fixture, args.columns, args.repeat, args.batch_rows, args.limit)
    plugin_cold_batch_reader = run_plugin_cold_batch_reader(
        args.fixture, columns, args.repeat, args.batch_rows, args.limit
    )
    plugin_warm_batch_reader = run_plugin_warm_batch_reader(
        args.fixture, columns, args.repeat, args.batch_rows, args.limit
    )
    plugin_cold_lazy_collect = run_plugin_cold_lazy_collect(
        args.fixture, columns, args.repeat, args.limit
    )
    plugin_warm_lazy_collect = run_plugin_warm_lazy_collect(
        args.fixture, columns, args.repeat, args.limit
    )

    raw_avg = raw["elapsed_ns_avg"]
    cold_batch_reader_ratio = (
        plugin_cold_batch_reader["elapsed_ns_avg"] / raw_avg if raw_avg else None
    )
    warm_batch_reader_ratio = (
        plugin_warm_batch_reader["steady_elapsed_ns_avg"] / raw_avg if raw_avg else None
    )
    cold_lazy_ratio = (
        plugin_cold_lazy_collect["elapsed_ns_avg"] / raw_avg if raw_avg else None
    )
    warm_lazy_ratio = (
        plugin_warm_lazy_collect["steady_elapsed_ns_avg"] / raw_avg if raw_avg else None
    )

    print(
        json.dumps(
            {
                "raw": raw,
                "plugin_cold_batch_reader": plugin_cold_batch_reader,
                "plugin_warm_batch_reader": plugin_warm_batch_reader,
                "plugin_cold_lazy_collect": plugin_cold_lazy_collect,
                "plugin_warm_lazy_collect": plugin_warm_lazy_collect,
                "ratios": {
                    "cold_batch_reader_over_raw_avg": cold_batch_reader_ratio,
                    "warm_batch_reader_over_raw_avg": warm_batch_reader_ratio,
                    "cold_lazy_collect_over_raw_avg": cold_lazy_ratio,
                    "warm_lazy_collect_over_raw_avg": warm_lazy_ratio,
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
