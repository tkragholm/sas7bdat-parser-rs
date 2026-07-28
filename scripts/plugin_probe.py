#!/usr/bin/env python3
"""Measure SAS7BDAT decode throughput through the installed sas7bdat-polars plugin.

    python plugin_probe.py "E:\\path\\big.sas7bdat"
    python plugin_probe.py "E:\\path\\big.sas7bdat" --rows 2000000 --threads 1,4,8,16
    python plugin_probe.py "E:\\path\\big.sas7bdat" --sink F:\\scratch\\probe.parquet

Bounded by --rows, so a probe takes minutes. Writes nothing unless --sink is given.
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import time

MB = 1024 * 1024
GB = 1024 * MB


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("file", help="the .sas7bdat to probe")
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="rows to decode per measurement (default 1,000,000)",
    )
    parser.add_argument(
        "--threads",
        default="1,2,4,8,16",
        help="comma-separated decode-thread counts to compare (default 1,2,4,8,16)",
    )
    parser.add_argument(
        "--columns",
        default=None,
        help="optional comma-separated column subset, to test projection pushdown",
    )
    parser.add_argument(
        "--sink",
        default=None,
        help="also time a streaming parquet write to this path (WRITES a file)",
    )
    args = parser.parse_args()

    try:
        import polars as pl
        import sas7bdat_polars as sp
    except ImportError as exc:
        print(
            f'import failed: {exc}\ninstall with: pip install "sas7bdat-polars[cli]"',
            file=sys.stderr,
        )
        return 1

    path = args.file
    if not os.path.isfile(path):
        print(f"not a file: {path}", file=sys.stderr)
        return 1

    info = sp.sas_info(path)
    size = int(info.get("size_bytes") or os.path.getsize(path))
    total_rows = int(info["n_rows"])
    row_len = int(info.get("row_length_bytes") or 0)
    rows = min(args.rows, total_rows)
    probe_bytes = rows * row_len if row_len else size * rows / max(total_rows, 1)

    print(f"=== {path} ===")
    print(f"  size            {size / GB:,.2f} GB")
    print(f"  rows x cols     {total_rows:,} x {info['n_columns']:,}")
    print(f"  row length      {row_len:,} bytes")
    print(f"  encoding        {info.get('encoding')}")
    print(f"  plugin version  {getattr(sp, '__version__', '?')}, polars {pl.__version__}")
    print(f"  default threads {sp.scan_threads()}")
    print(
        f"\n  probing {rows:,} rows = {probe_bytes / GB:,.2f} GB of source per measurement\n"
    )

    columns = [c.strip() for c in args.columns.split(",")] if args.columns else None
    if columns:
        print(f"  projecting {len(columns)} column(s)\n")

    counts = [int(t) for t in args.threads.split(",") if t.strip()]
    print(
        f"  {'threads':>8}  {'seconds':>9}  {'MB/s':>9}  {'vs 1 thread':>12}   full-file estimate"
    )
    base = 0.0
    for count in counts:
        sp.set_scan_threads(count)
        gc.collect()
        start = time.perf_counter()
        frame = sp.scan_sas(path, columns=columns, n_rows=rows).collect()
        elapsed = time.perf_counter() - start
        got = frame.height
        del frame
        rate = (probe_bytes / MB) / elapsed if elapsed else 0.0
        if count == counts[0]:
            base = rate
        speedup = f"{rate / base:,.2f}x" if base else "n/a"
        full_min = (size / MB / rate / 60) if rate else float("inf")
        flag = "" if got == rows else f"  (returned {got:,} rows)"
        print(
            f"  {count:>8}  {elapsed:>9,.1f}  {rate:>9,.0f}  {speedup:>12}   {full_min:>6,.1f} min{flag}"
        )

    if args.sink:
        best = max(counts)
        sp.set_scan_threads(best)
        gc.collect()
        print(f"\n  streaming parquet write ({best} decode threads) -> {args.sink}")
        start = time.perf_counter()
        sp.scan_sas(path, columns=columns, n_rows=rows).sink_parquet(args.sink)
        elapsed = time.perf_counter() - start
        out_size = os.path.getsize(args.sink) if os.path.isfile(args.sink) else 0
        rate = (probe_bytes / MB) / elapsed if elapsed else 0.0
        print(f"  {elapsed:,.1f}s  ({rate:,.0f} MB/s of source)")
        if out_size:
            print(
                f"  output {out_size / MB:,.1f} MB  ->  compression {probe_bytes / out_size:,.1f}x"
            )
            print(
                f"  full-file estimate: {size / MB / rate / 60:,.1f} min, "
                f"~{size / max(probe_bytes, 1) * out_size / GB:,.1f} GB of parquet"
            )
        else:
            print("  output missing")

    sp.set_scan_threads(0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
