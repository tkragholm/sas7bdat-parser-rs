"""Read one SAS7BDAT file once with one Python reader and print one JSON line.

Usage: read_py.py <tool> <path>

The timed region is the read call alone: imports happen first, and the checksum
is computed after the clock stops, and only when BENCH_CHECKSUM=1. The checksum is per column: for numeric
columns the sum and the number of missing values, for string columns the total
UTF-8 length after removing trailing spaces. run.py compares it across readers.
"""

from __future__ import annotations

import json
import math
import os
import sys
import time


def read_sas7bdat_polars(path):
    import sas7bdat_polars as sp

    return sp.read_sas(path)


def read_polars_readstat(path):
    import polars_readstat as pr

    return pr.scan_readstat(path).collect()


def read_pyreadstat(path):
    import pyreadstat

    df, _ = pyreadstat.read_sas7bdat(path)
    return df


def read_pyreadstat_mp(path):
    import pyreadstat

    df, _ = pyreadstat.read_file_multiprocessing(
        pyreadstat.read_sas7bdat, path, num_processes=os.cpu_count()
    )
    return df


def read_pandas(path):
    import pandas as pd

    return pd.read_sas(path, format="sas7bdat", encoding="infer")


READERS = {
    "sas7bdat-polars": read_sas7bdat_polars,
    "polars-readstat": read_polars_readstat,
    "pyreadstat": read_pyreadstat,
    "pyreadstat-mp": read_pyreadstat_mp,
    "pandas": read_pandas,
}

IMPORTS = {
    "sas7bdat-polars": ["sas7bdat_polars", "polars"],
    "polars-readstat": ["polars_readstat", "polars"],
    "pyreadstat": ["pyreadstat", "pandas"],
    "pyreadstat-mp": ["pyreadstat", "pandas"],
    "pandas": ["pandas"],
}


def checksum(df) -> dict[str, list]:
    out: dict[str, list] = {}
    if hasattr(df, "iter_columns"):  # polars
        import polars as pl

        for s in df.iter_columns():
            if s.dtype.is_numeric():
                out[s.name] = [
                    "num",
                    float(s.cast(pl.Float64).sum() or 0.0),
                    s.null_count(),
                ]
            elif s.dtype == pl.String:
                lens = s.str.strip_chars_end(" ").str.len_bytes().fill_null(0)
                out[s.name] = ["str", int(lens.sum())]
        return out
    import pandas as pd

    for name in df.columns:
        col = df[name]
        if pd.api.types.is_numeric_dtype(col) and not pd.api.types.is_bool_dtype(col):
            vals = col.astype("float64")
            total = float(vals.sum(skipna=True))
            out[str(name)] = [
                "num",
                0.0 if math.isnan(total) else total,
                int(vals.isna().sum()),
            ]
        elif col.dtype == object or pd.api.types.is_string_dtype(col):
            lens = col.map(
                lambda v: len(v.rstrip(" ").encode()) if isinstance(v, str) else 0
            )
            out[str(name)] = ["str", int(lens.sum())]
    return out


def main() -> None:
    tool, path = sys.argv[1], sys.argv[2]
    for module in IMPORTS[tool]:
        __import__(module)
    start = time.perf_counter()
    df = READERS[tool](path)
    seconds = time.perf_counter() - start
    rows, cols = df.shape
    print(
        json.dumps(
            {
                "tool": tool,
                "seconds": seconds,
                "rows": rows,
                "cols": cols,
                "checksum": checksum(df) if os.environ.get("BENCH_CHECKSUM") == "1" else None,
            }
        )
    )


if __name__ == "__main__":
    main()
