#!/usr/bin/env python3
"""Time in-memory SAS7BDAT reads for the Polars plugin, pyreadstat, and pandas.

Usage: bench_py.py <path> <iters> [tools]
  tools: comma list from {polars,pyreadstat,pandas}; default all
"""
import os
import sys
import time


def stats(label, fn, path, iters):
    try:
        rows = fn(path)  # warmup
    except Exception as e:  # noqa: BLE001
        print(f"RESULT tool={label} file={os.path.basename(path)} ERROR={type(e).__name__}:{e}")
        return
    ts = []
    for _ in range(iters):
        t = time.perf_counter()
        fn(path)
        ts.append(time.perf_counter() - t)
    ts.sort()
    mb = os.path.getsize(path) / 1e6
    med = ts[len(ts) // 2]
    print(
        f"RESULT tool={label} file={os.path.basename(path)} "
        f"min={ts[0]:.3f} med={med:.3f} mbps={mb / ts[0]:.1f} rows={rows}"
    )


def read_polars(path):
    import sas7bdat_polars as sp

    df = sp.scan_sas(str(path)).collect()
    return df.height


def read_pyreadstat(path):
    import pyreadstat

    df, _meta = pyreadstat.read_sas7bdat(path)
    return len(df)


def read_pandas(path):
    import pandas as pd

    df = pd.read_sas(path, format="sas7bdat")
    return len(df)


READERS = {"polars": read_polars, "pyreadstat": read_pyreadstat, "pandas": read_pandas}


def main():
    path = sys.argv[1]
    iters = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    tools = sys.argv[3].split(",") if len(sys.argv) > 3 else list(READERS)
    for t in tools:
        if t in READERS:
            stats(t, READERS[t], path, iters)


if __name__ == "__main__":
    main()
