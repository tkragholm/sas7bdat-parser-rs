#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence

import pandas as pd
import pyreadstat

if TYPE_CHECKING:
    from pyreadstat import metadata_container as Metadata
else:  # pragma: no cover - runtime fallback for type hints
    class Metadata:  # type: ignore[too-many-ancestors]
        ...


def init_column_summaries(meta: "Metadata") -> Dict[str, Dict[str, Any]]:
    summaries: Dict[str, Dict[str, Any]] = {}
    variable_types = meta.readstat_variable_types or {}

    column_labels: Optional[Sequence[str]] = getattr(meta, "column_labels", None)
    formats: Optional[Sequence[str]] = getattr(meta, "formats", None)

    for idx, name in enumerate(meta.column_names):
        label = (
            column_labels[idx] if column_labels and idx < len(column_labels) else None
        )
        label = label.strip() if isinstance(label, str) else None
        if label == "":
            label = None

        fmt = formats[idx] if formats and idx < len(formats) else None
        fmt = fmt.strip() if isinstance(fmt, str) else None
        if fmt == "":
            fmt = None

        readstat_type = (variable_types.get(name) or "").lower()
        kind = "numeric" if readstat_type in {"double", "float", "numeric"} else "character"

        column_summary: Dict[str, Any] = {
            "index": idx,
            "name": name,
            "label": label,
            "format": fmt,
            "kind": kind,
            "non_missing": 0,
            "missing": 0,
        }
        if kind == "numeric":
            column_summary["numeric"] = {
                "count": 0,
                "sum": 0.0,
                "min": None,
                "max": None,
            }
        summaries[name] = column_summary

    return summaries


def update_numeric_stats(summary: Dict[str, Any], series: pd.Series) -> None:
    stats = summary.get("numeric")
    if stats is None:
        return

    valid = series.dropna()
    if valid.empty:
        return

    # Ensure we operate on float64 for consistent JSON output.
    valid_values = pd.to_numeric(valid, errors="coerce").dropna()
    if valid_values.empty:
        return

    stats["count"] += int(valid_values.count())
    stats["sum"] += float(valid_values.sum())

    current_min = stats["min"]
    current_max = stats["max"]
    value_min = float(valid_values.min())
    value_max = float(valid_values.max())

    stats["min"] = value_min if current_min is None else min(current_min, value_min)
    stats["max"] = value_max if current_max is None else max(current_max, value_max)


def summarize_file(path: Path, chunksize: int = 50_000) -> Dict[str, Any]:
    iterator = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        str(path),
        chunksize=chunksize,
        output_format="pandas",
    )

    summaries: Dict[str, Dict[str, Any]] = {}
    total_rows = 0

    for df, meta in iterator:
        if not summaries:
            summaries = init_column_summaries(meta)

        chunk_len = len(df)
        total_rows += chunk_len

        for column_name in meta.column_names:
            series = df[column_name]
            summary = summaries[column_name]

            missing = series.isna().sum()
            non_missing = chunk_len - int(missing)

            summary["missing"] += int(missing)
            summary["non_missing"] += non_missing

            if summary["kind"] == "numeric":
                update_numeric_stats(summary, series)

    columns_ordered = sorted(summaries.values(), key=lambda entry: entry["index"])
    return {"row_count": total_rows, "columns": columns_ordered}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize a SAS7BDAT file using pyreadstat."
    )
    parser.add_argument("path", type=Path, help="Path to the sas7bdat file")
    parser.add_argument(
        "--chunksize",
        type=int,
        default=50_000,
        help="Number of rows per chunk when reading (default: 50,000)",
    )
    args = parser.parse_args()

    summary = summarize_file(args.path, chunksize=args.chunksize)
    json.dump(summary, sys.stdout, ensure_ascii=False, indent=2)
    print()


if __name__ == "__main__":
    main()
