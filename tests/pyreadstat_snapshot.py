#!/usr/bin/env python3
"""
Emit canonical JSON snapshots for SAS fixtures using pyreadstat.

The script prints a single JSON object mapping each SAS filename to its
column metadata and row values converted into a stable representation that
matches the Rust integration tests.  It is intended to be consumed by
Rust tests and by developers regenerating comparison data locally.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date as dt_date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import numpy as np
import pandas as pd
import pyreadstat

SKIP_FIXTURES: Set[str] = {
    "fixtures/raw_data/pandas/corrupt.sas7bdat",
    "fixtures/raw_data/pandas/zero_variables.sas7bdat",
    "fixtures/raw_data/csharp/54-class.sas7bdat",
    "fixtures/raw_data/csharp/54-cookie.sas7bdat",
    "fixtures/raw_data/csharp/charset_zpce.sas7bdat",
    "fixtures/raw_data/csharp/date_format_dtdate.sas7bdat",
    "fixtures/raw_data/csharp/date_formats.sas7bdat",
}

SAS_EPOCH = datetime(1960, 1, 1, tzinfo=timezone.utc)

KIND_NUMBER = "number"
KIND_STRING = "string"
KIND_NUMERIC_STRING = "numeric-string"
KIND_MISSING = "missing"
KIND_DATE = "date"
KIND_DATETIME = "datetime"
KIND_TIME = "time"
KIND_BYTES = "bytes"

DATE_FORMATS: Set[str] = {
    "date",
    "date9",
    "yymmdd",
    "ddmmyy",
    "mmddyy",
    "mmddyy10",
    "e8601da",
    "minguo",
    "monname",
}
DATETIME_FORMATS: Set[str] = {"datetime", "datetime20", "datetime22.3"}
TIME_FORMATS: Set[str] = {"time"}


def infer_kind_from_format(format_hint: str | None) -> str | None:
    if not format_hint:
        return None
    fmt = format_hint.lower()
    if fmt in DATETIME_FORMATS:
        return KIND_DATETIME
    if fmt in TIME_FORMATS:
        return KIND_TIME
    if fmt in DATE_FORMATS:
        return KIND_DATE
    return None


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, (np.floating,)) and math.isnan(float(value)):
        return True
    if isinstance(value, (np.generic,)):
        return is_missing(value.item())
    if pd.isna(value):
        return True
    return False


def convert_numeric(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, np.ndarray):
        return float(value.item())
    return float(value)


def convert_datetime_to_seconds(value: Any) -> float:
    if isinstance(value, pd.Timestamp):
        dt = value.to_pydatetime()
    elif isinstance(value, datetime):
        dt = value
    else:
        return convert_numeric(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return (dt - SAS_EPOCH).total_seconds()


def convert_date_to_days(value: dt_date) -> float:
    dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return (dt - SAS_EPOCH).total_seconds() / 86_400.0


def convert_time_to_seconds(value: Any) -> float:
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    if isinstance(value, np.timedelta64):
        return pd.to_timedelta(value).total_seconds()
    if isinstance(value, dt_time):
        return (
            value.hour * 3600
            + value.minute * 60
            + value.second
            + value.microsecond / 1_000_000.0
        )
    if isinstance(value, str):
        try:
            hour, minute, second = value.split(":")
            return int(hour) * 3600 + int(minute) * 60 + float(second)
        except Exception:
            return convert_numeric(value)
    return convert_numeric(value)


def coerce_to_datetime(value: Any) -> datetime | pd.Timestamp | None:
    if isinstance(value, pd.Timestamp):
        return value
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return pd.to_datetime(value, errors="raise")
        except Exception:
            return None
    return None


def coerce_to_date(value: Any) -> dt_date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, dt_date):
        return value
    if isinstance(value, str):
        try:
            return pd.to_datetime(value, errors="raise").date()
        except Exception:
            return None
    return None


def convert_with_format(value: Any, target_kind: str | None) -> Dict[str, Any] | None:
    if target_kind == KIND_DATETIME:
        dt_value = coerce_to_datetime(value)
        if dt_value is not None:
            return {"kind": KIND_DATETIME, "value": convert_datetime_to_seconds(dt_value)}
        if isinstance(value, (int, float)) or isinstance(value, np.number):
            return {"kind": KIND_DATETIME, "value": convert_numeric(value)}
        return None

    if target_kind == KIND_DATE:
        date_value = coerce_to_date(value)
        if date_value is not None:
            return {"kind": KIND_DATE, "value": convert_date_to_days(date_value)}
        if isinstance(value, (int, float)) or isinstance(value, np.number):
            return {"kind": KIND_DATE, "value": convert_numeric(value)}
        return None

    if target_kind == KIND_TIME:
        if isinstance(value, (pd.Timedelta, np.timedelta64, dt_time)):
            return {"kind": KIND_TIME, "value": convert_time_to_seconds(value)}
        if isinstance(value, str):
            return {"kind": KIND_TIME, "value": convert_time_to_seconds(value)}
        if isinstance(value, (int, float)) or isinstance(value, np.number):
            return {"kind": KIND_TIME, "value": convert_numeric(value)}
        return None

    return None


def convert_value(value: Any, format_hint: str | None) -> Dict[str, Any]:
    if is_missing(value):
        return {"kind": KIND_MISSING, "value": None}

    if isinstance(value, (np.generic,)):
        value = value.item()

    target_kind = infer_kind_from_format(format_hint)
    formatted = convert_with_format(value, target_kind)
    if formatted is not None:
        return formatted

    if isinstance(value, (bytes, bytearray, memoryview, np.ndarray)):
        return {"kind": KIND_BYTES, "value": list(bytes(value))}

    if isinstance(value, str):
        return {"kind": KIND_STRING, "value": value}

    if isinstance(value, pd.Timestamp) or isinstance(value, datetime):
        return {"kind": KIND_DATETIME, "value": convert_datetime_to_seconds(value)}

    if isinstance(value, dt_date):
        return {"kind": KIND_DATE, "value": convert_date_to_days(value)}

    if isinstance(value, pd.Timedelta) or isinstance(value, np.timedelta64):
        return {"kind": KIND_TIME, "value": convert_time_to_seconds(value)}

    if isinstance(value, dt_time):
        return {"kind": KIND_TIME, "value": convert_time_to_seconds(value)}

    if isinstance(value, (bool, int, float)) or isinstance(value, np.number):
        return {"kind": KIND_NUMBER, "value": convert_numeric(value)}

    return {"kind": KIND_STRING, "value": str(value)}


def snapshot_fixture(path: Path) -> Dict[str, Any]:
    df, meta = pyreadstat.read_sas7bdat(path)
    column_formats: List[str | None] = [
        meta.original_variable_types.get(name) for name in meta.column_names
    ]

    rows: List[List[Any]] = []
    for record in df.itertuples(index=False, name=None):
        row = [
            convert_value(value, column_formats[index])
            for index, value in enumerate(record)
        ]
        rows.append(row)

    return {
        "columns": meta.column_names,
        "row_count": meta.number_rows,
        "rows": rows,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate pyreadstat snapshots for SAS fixtures."
    )
    parser.add_argument(
        "--fixtures-dir",
        action="append",
        type=Path,
        help="Directory containing .sas7bdat fixtures (may be repeated)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional file to write JSON output; defaults to stdout",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    raw_directories = args.fixtures_dir or [Path("fixtures/raw_data")]
    directories: List[Path] = []
    for directory in raw_directories:
        if not directory.exists():
            parser.error(f"fixtures directory {directory} does not exist")
        directories.append(directory.resolve())

    cwd = Path.cwd().resolve()

    snapshots: Dict[str, Dict[str, Any]] = {}

    for directory in directories:
        for path in sorted(directory.rglob("*.sas7bdat")):
            relative_key = path.resolve().relative_to(cwd).as_posix()
            key = relative_key.replace("\\", "/")
            if key in SKIP_FIXTURES:
                print(f"Skipping unsupported fixture {key}", file=sys.stderr)
                continue
            if key in snapshots:
                raise SystemExit(
                    f"duplicate fixture key detected for {path} (existing entry for {key})"
                )
            try:
                snapshots[key] = snapshot_fixture(path)
            except Exception as err:  # pragma: no cover - surfaced in Rust integration
                raise RuntimeError(f"failed to snapshot {path}: {err}") from err

    ordered = {key: snapshots[key] for key in sorted(snapshots.keys())}
    output_data = json.dumps(ordered, indent=2, sort_keys=False)

    if args.output:
        args.output.write_text(output_data, encoding="utf-8")
    else:
        sys.stdout.write(output_data)
        sys.stdout.flush()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
