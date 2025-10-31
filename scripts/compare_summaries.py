#!/usr/bin/env python3

import argparse
import json
import math
from pathlib import Path
from typing import Dict, Any


def load_summary(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def compare_numeric(a: Dict[str, Any], b: Dict[str, Any], tolerance: float) -> Dict[str, float]:
    diffs: Dict[str, float] = {}
    if a.get("count") != b.get("count"):
        diffs["count"] = float(a.get("count", 0)) - float(b.get("count", 0))

    sum_a = float(a.get("sum", 0.0))
    sum_b = float(b.get("sum", 0.0))
    if not math.isclose(sum_a, sum_b, rel_tol=tolerance, abs_tol=tolerance):
        diffs["sum"] = sum_a - sum_b

    for key in ("min", "max"):
        aval = a.get(key)
        bval = b.get(key)
        if aval is None and bval is None:
            continue
        if aval is None or bval is None:
            diffs[key] = float(aval or 0.0) - float(bval or 0.0)
            continue
        if not math.isclose(float(aval), float(bval), rel_tol=tolerance, abs_tol=tolerance):
            diffs[key] = float(aval) - float(bval)
    return diffs


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two dataset summary JSON files.")
    parser.add_argument("rust", type=Path, help="Summary produced by the Rust parser")
    parser.add_argument("python", type=Path, help="Summary produced by pyreadstat")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-9,
        help="Tolerance for numeric comparisons (default: 1e-9)",
    )
    args = parser.parse_args()

    rust_summary = load_summary(args.rust)
    py_summary = load_summary(args.python)

    if rust_summary.get("row_count") != py_summary.get("row_count"):
        print(
            f"Row count mismatch: Rust={rust_summary.get('row_count')} Python={py_summary.get('row_count')}"
        )

    rust_columns = {col["name"]: col for col in rust_summary.get("columns", [])}
    py_columns = {col["name"]: col for col in py_summary.get("columns", [])}

    all_names = sorted(set(rust_columns) | set(py_columns))
    mismatches = 0

    for name in all_names:
        rust_col = rust_columns.get(name)
        py_col = py_columns.get(name)

        if rust_col is None:
            print(f"Column '{name}' missing from Rust summary")
            mismatches += 1
            continue
        if py_col is None:
            print(f"Column '{name}' missing from Python summary")
            mismatches += 1
            continue

        if rust_col.get("missing") != py_col.get("missing") or rust_col.get("non_missing") != py_col.get(
            "non_missing"
        ):
            print(
                f"Column '{name}' count mismatch: Rust missing/non-missing={rust_col.get('missing')}/"
                f"{rust_col.get('non_missing')} vs Python {py_col.get('missing')}/{py_col.get('non_missing')}"
            )
            mismatches += 1

        rust_num = rust_col.get("numeric")
        py_num = py_col.get("numeric")
        if rust_num is None and py_num is None:
            continue
        if rust_num is None or py_num is None:
            print(f"Column '{name}' numeric stats mismatch: one side missing")
            mismatches += 1
            continue

        diffs = compare_numeric(rust_num, py_num, args.tolerance)
        if diffs:
            print(f"Column '{name}' numeric differences: {diffs}")
            mismatches += 1

    if mismatches == 0:
        print("Summaries match within tolerance.")
    else:
        print(f"Found {mismatches} mismatched column(s).")


if __name__ == "__main__":
    main()
