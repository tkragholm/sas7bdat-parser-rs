#!/usr/bin/env python3
"""
Generate JSON snapshots for SAS fixtures using pandas and pyreadstat.

This utility walks the SAS corpus under ``fixtures/raw_data/`` (or any custom
paths supplied via ``--fixtures-dir``), parses each dataset with the requested
parsers, and emits normalized JSON representations mirroring the directory
structure beneath ``crates/sas7bdat/tests/reference/{parser}/``.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date as dt_date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import numpy as np
import pandas as pd
import pyreadstat

SKIP_FIXTURES = {
    "fixtures/raw_data/pandas/corrupt.sas7bdat",
    "fixtures/raw_data/pandas/zero_variables.sas7bdat",
    "fixtures/raw_data/csharp/54-class.sas7bdat",
    "fixtures/raw_data/csharp/54-cookie.sas7bdat",
    "fixtures/raw_data/csharp/charset_zpce.sas7bdat",
    "fixtures/raw_data/csharp/date_format_dtdate.sas7bdat",
    "fixtures/raw_data/csharp/date_formats.sas7bdat",
    "fixtures/raw_data/ahs2013/topical.sas7bdat",
}

PARSER_NAMES = ("pandas", "pyreadstat")

SAS_EPOCH = datetime(1960, 1, 1, tzinfo=timezone.utc)

DATE_FORMATS = {
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
DATETIME_FORMATS = {"datetime", "datetime20", "datetime22.3"}
TIME_FORMATS = {"time"}


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate pandas and pyreadstat JSON snapshots for SAS fixtures."
    )
    parser.add_argument(
        "--fixtures-dir",
        action="append",
        type=Path,
        dest="fixtures_dirs",
        help="Directory containing .sas7bdat fixtures (may be repeated)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Base directory for generated JSON (defaults to crates/sas7bdat/tests/reference)",
    )
    parser.add_argument(
        "--parsers",
        nargs="+",
        choices=PARSER_NAMES,
        default=list(PARSER_NAMES),
        help="Subset of parsers to execute",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    repo_root = Path(__file__).resolve().parent.parent
    output_root = (
        (repo_root / "crates" / "sas7bdat" / "tests" / "reference")
        if args.output_dir is None
        else resolve_path(args.output_dir, repo_root)
    )

    fixtures_dirs = args.fixtures_dirs if args.fixtures_dirs else [Path("fixtures/raw_data")]
    fixtures = [resolve_path(directory, repo_root) for directory in fixtures_dirs]

    entries: Dict[str, Path] = {}
    for directory in fixtures:
        if not directory.exists():
            parser.error(f"fixtures directory {directory} does not exist")
        for path in sorted(directory.rglob("*.sas7bdat")):
            key = normalized_key(path, repo_root)
            if key in SKIP_FIXTURES:
                print(f"[skip] {key}", file=sys.stderr)
                continue
            if key in entries:
                raise SystemExit(
                    f"duplicate fixture key detected: {path} clashes with {entries[key]}"
                )
            entries[key] = path

    if not entries:
        print("No fixtures found; nothing to do.", file=sys.stderr)
        return 0

    failures: List[tuple[str, str, Exception]] = []

    for parser_name in args.parsers:
        output_dir = output_root / parser_name
        for key, path in entries.items():
            try:
                snapshot = (
                    snapshot_with_pandas(path)
                    if parser_name == "pandas"
                    else snapshot_with_pyreadstat(path)
                )
            except Exception as err:
                failures.append((parser_name, key, err))
                print(
                    f"[error] {parser_name} failed for {key}: {err}",
                    file=sys.stderr,
                )
                continue
            write_snapshot(output_dir, key, snapshot)

    if failures:
        print(
            f"{len(failures)} snapshot(s) failed; see log for details.",
            file=sys.stderr,
        )
        return 1

    return 0


def snapshot_with_pandas(path: Path) -> Dict[str, Any]:
    df = pd.read_sas(path, format="sas7bdat")
    encoding = df.attrs.get("encoding") or "utf-8"
    columns = [str(column) for column in df.columns]
    formats = extract_format_hints(df.attrs.get("formats"), columns)
    df = decode_object_columns(df, encoding)
    return snapshot_dataframe(df, columns, formats)


def snapshot_with_pyreadstat(path: Path) -> Dict[str, Any]:
    df, meta = pyreadstat.read_sas7bdat(path)
    columns = list(meta.column_names)
    formats = resolve_pyreadstat_formats(meta, columns)
    return snapshot_dataframe(df, columns, formats)


def resolve_pyreadstat_formats(
    meta: pyreadstat.readstat_metadata, columns: Sequence[str]
) -> Dict[str, str | None]:
    candidates: List[Mapping[str, str]] = []
    for attr in (
        "column_formats",
        "original_variable_formats",
        "original_variable_types",
    ):
        value = getattr(meta, attr, None)
        if isinstance(value, Mapping):
            candidates.append(value)

    resolved: Dict[str, str | None] = {}
    for name in columns:
        hint = None
        for mapping in candidates:
            hint = mapping.get(name)
            if hint:
                break
        resolved[name] = hint
    return resolved


def decode_object_columns(df: pd.DataFrame, encoding: str) -> pd.DataFrame:
    def decode_cell(value: Any) -> Any:
        if isinstance(value, (bytes, bytearray, memoryview)):
            try:
                return value.decode(encoding, errors="strict")
            except UnicodeDecodeError:
                return value
        return value

    decoded = {}
    for column in df.columns:
        series = df[column]
        if series.dtype == object:
            decoded[column] = series.apply(decode_cell)
        else:
            decoded[column] = series
    return pd.DataFrame(decoded, index=df.index, columns=df.columns)


def snapshot_dataframe(
    df: pd.DataFrame,
    columns: Sequence[str],
    format_hints: Mapping[str, str | None],
) -> Dict[str, Any]:
    rows: List[List[Dict[str, Any]]] = []
    for record in df.itertuples(index=False, name=None):
        row: List[Dict[str, Any]] = []
        for index, value in enumerate(record):
            column = columns[index]
            hint = format_hints.get(column)
            row.append(convert_value(value, hint))
        rows.append(row)
    return {
        "columns": list(columns),
        "row_count": len(df),
        "rows": rows,
    }


def extract_format_hints(formats: Any, columns: Sequence[str]) -> Dict[str, str | None]:
    if isinstance(formats, Mapping):
        return {name: formats.get(name) for name in columns}
    return {name: None for name in columns}


def convert_value(value: Any, format_hint: str | None) -> Dict[str, Any]:
    if is_missing(value):
        return {"kind": "missing", "value": None}

    if isinstance(value, (np.generic,)):
        value = value.item()

    target_kind = infer_kind_from_format(format_hint)
    formatted = convert_with_format(value, target_kind)
    if formatted is not None:
        return formatted

    if isinstance(value, (bytes, bytearray, memoryview, np.ndarray)):
        return {"kind": "bytes", "value": list(bytes(value))}

    if isinstance(value, str):
        return {"kind": "string", "value": value}

    if isinstance(value, pd.Timestamp) or isinstance(value, datetime):
        return {"kind": "datetime", "value": convert_datetime_to_seconds(value)}

    if isinstance(value, dt_date):
        return {"kind": "date", "value": convert_date_to_days(value)}

    if isinstance(value, pd.Timedelta) or isinstance(value, np.timedelta64):
        return {"kind": "time", "value": convert_time_to_seconds(value)}

    if isinstance(value, dt_time):
        return {"kind": "time", "value": convert_time_to_seconds(value)}

    if isinstance(value, (bool, int, float)) or isinstance(value, np.number):
        return {"kind": "number", "value": convert_numeric(value)}

    return {"kind": "string", "value": str(value)}


def infer_kind_from_format(format_hint: str | None) -> str | None:
    if not format_hint:
        return None
    fmt = format_hint.lower()
    if fmt in DATETIME_FORMATS:
        return "datetime"
    if fmt in TIME_FORMATS:
        return "time"
    if fmt in DATE_FORMATS:
        return "date"
    return None


def convert_with_format(value: Any, target_kind: str | None) -> Dict[str, Any] | None:
    if target_kind == "datetime":
        dt_value = coerce_to_datetime(value)
        if dt_value is not None:
            return {"kind": "datetime", "value": convert_datetime_to_seconds(dt_value)}
        if isinstance(value, (int, float)) or isinstance(value, np.number):
            return {"kind": "datetime", "value": convert_numeric(value)}
        return None

    if target_kind == "date":
        date_value = coerce_to_date(value)
        if date_value is not None:
            return {"kind": "date", "value": convert_date_to_days(date_value)}
        if isinstance(value, (int, float)) or isinstance(value, np.number):
            return {"kind": "date", "value": convert_numeric(value)}
        return None

    if target_kind == "time":
        if isinstance(value, (pd.Timedelta, np.timedelta64, dt_time)):
            return {"kind": "time", "value": convert_time_to_seconds(value)}
        if isinstance(value, str):
            return {"kind": "time", "value": convert_time_to_seconds(value)}
        if isinstance(value, (int, float)) or isinstance(value, np.number):
            return {"kind": "time", "value": convert_numeric(value)}
        return None

    return None


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, (np.floating,)) and math.isnan(float(value)):
        return True
    if isinstance(value, (np.generic,)) and math.isnan(float(value)):
        return True
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


def write_snapshot(output_root: Path, key: str, snapshot: Dict[str, Any]) -> None:
    target = output_root / Path(key).with_suffix(".json")
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def normalized_key(path: Path, repo_root: Path) -> str:
    relative = ensure_relative(path, repo_root)
    return relative.as_posix()


def ensure_relative(path: Path, base: Path) -> Path:
    try:
        return path.resolve().relative_to(base.resolve())
    except ValueError:
        return path.resolve()


def resolve_path(path: Path, base: Path) -> Path:
    return path if path.is_absolute() else base / path


if __name__ == "__main__":
    raise SystemExit(main())
