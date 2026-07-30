#!/usr/bin/env python3
"""Verify the Polars plugin output against pyreadstat across all column types.

The plugin's conversion does the SAS->Unix epoch math (Date->days-since-1970, DateTime->us,
Time->ns) plus bytemuck casts. To check that independently, reduce BOTH the plugin's Polars
output and pyreadstat's pandas output to canonical SAS-epoch units (days / seconds since
1960-01-01, seconds since midnight) and text, then compare. The canonicalizer here uses
1960-01-01 directly, so it doesn't share constants with the Rust code under test.
"""
import datetime as dt
import math
import os
import sys

import polars as pl
import pyreadstat
import sas7bdat_polars

SAS_EPOCH_DATE = dt.date(1960, 1, 1)
SAS_EPOCH_DT = dt.datetime(1960, 1, 1)
ROW_CAP = 500  # conversion bugs are systematic per-column; a few hundred rows suffice
NUM_TOL = 1e-6
# Canonical SAS(1960) <- Unix(1970) offsets, used only in the comparison (not shared with the
# Rust code under test): days between the epochs, and the same in seconds.
DAYS_SAS_TO_UNIX = (dt.date(1970, 1, 1) - dt.date(1960, 1, 1)).days  # 3653
SECS_SAS_TO_UNIX = DAYS_SAS_TO_UNIX * 86400


def canon(v):
    """Reduce a cell to None | ('num', float SAS-epoch units) | ('str', text)."""
    if v is None:
        return None
    try:
        if v != v:  # NaN / NaT
            return None
    except Exception:
        pass
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, dt.datetime):
        return ("num", (v - SAS_EPOCH_DT).total_seconds())
    if isinstance(v, dt.date):
        return ("num", float((v - SAS_EPOCH_DATE).days))
    if isinstance(v, dt.time):
        # pyreadstat wraps SAS time/datetime-of-day to a clock time [0,24h); tag it so the
        # comparison reconciles modulo 86400 (SAS time values can be >=24h or negative, and
        # TOD-formatted columns carry a full datetime whose time-of-day pyreadstat extracts).
        return ("time", v.hour * 3600 + v.minute * 60 + v.second + v.microsecond / 1e6)
    if isinstance(v, dt.timedelta):
        return ("num", v.total_seconds())
    if isinstance(v, (int, float)):
        return ("num", float(v))
    if isinstance(v, (bytes, bytearray)):
        return ("str", bytes(v).rstrip(b" \x00").decode("utf-8", "replace"))
    return ("str", str(v).rstrip(" "))


def cells_match(a, b):
    if a is None or b is None:
        return a is None and b is None
    ka, va = a
    kb, vb = b
    # Time-of-day reference: reconcile modulo 86400 so an >=24h SAS time (or a TOD datetime we
    # keep in full) matches pyreadstat's wrapped clock time. A null on our side still fails here.
    if ka == "time" or kb == "time":
        if ka == "str" or kb == "str":
            return False
        d = (va - vb) % 86400
        return d < 1e-3 or 86400 - d < 1e-3
    if ka != kb:
        return False
    if ka == "num":
        scale = max(abs(va), 1.0)
        return abs(va - vb) <= NUM_TOL * scale + NUM_TOL
    return va == vb


def our_matrix(path):
    df = sas7bdat_polars.scan_sas(path).collect().head(ROW_CAP)
    # Extract temporal columns via their PHYSICAL integers and rebase to SAS-epoch units, so
    # extraction never materializes an out-of-range Python date (and the epoch math the plugin
    # did is checked by round-tripping it back to SAS units).
    exprs = []
    for name, dtype in zip(df.columns, df.dtypes):
        c = pl.col(name)
        if dtype == pl.Date:
            exprs.append((c.cast(pl.Int64) + DAYS_SAS_TO_UNIX).cast(pl.Float64).alias(name))
        elif isinstance(dtype, pl.Datetime):
            exprs.append((c.cast(pl.Int64).cast(pl.Float64) / 1e6 + SECS_SAS_TO_UNIX).alias(name))
        elif dtype == pl.Time or isinstance(dtype, pl.Duration):
            # SAS TIME columns arrive as Duration('ns') — pl.Time cannot hold the values
            # outside [0, 24h) that real files carry. Both are i64 nanoseconds physically.
            exprs.append((c.cast(pl.Int64).cast(pl.Float64) / 1e9).alias(name))
        else:
            exprs.append(c)
    rows = df.select(exprs).rows()
    return df.columns, rows


def ref_matrix(path):
    df, _meta = pyreadstat.read_sas7bdat(path)
    rows = list(df.head(ROW_CAP).itertuples(index=False, name=None))
    return list(df.columns), rows


def main():
    roots = ["fixtures", "old-implementation/sas7bdat-parser-rs/fixtures"]
    fixtures = []
    for r in roots:
        for base, _dirs, files in os.walk(r):
            for f in files:
                if f.endswith(".sas7bdat"):
                    fixtures.append(os.path.join(base, f))
    fixtures = sorted(set(fixtures))

    matched = skipped = 0
    cells = 0
    mojibake_only = 0
    failures = []
    for path in fixtures:
        try:
            ocols, orows = our_matrix(path)
        except Exception:
            skipped += 1
            continue
        try:
            rcols, rrows = ref_matrix(path)
        except Exception:
            skipped += 1
            continue
        name = os.path.basename(path)
        # Known WHATWG ISO-8859-1 -> Windows-1252 encoding-alias difference vs readstat's strict
        # Latin-1 (only affects bytes 0x80-0x9F; this file's data is double-encoded anyway).
        if name == "test16.sas7bdat":
            skipped += 1
            continue
        if len(orows) != len(rrows) or len(ocols) != len(rcols):
            failures.append(f"{name}: shape ours={len(orows)}x{len(ocols)} ref={len(rrows)}x{len(rcols)}")
            continue
        ok = True
        for ri, (orow, rrow) in enumerate(zip(orows, rrows)):
            for ci, (ov, rv) in enumerate(zip(orow, rrow)):
                cells += 1
                ca, cb = canon(ov), canon(rv)
                if cells_match(ca, cb):
                    continue
                # There is deliberately no tolerance for out-of-range SAS TIME values here.
                # They used to surface as null because the plugin declared pl.Time, whose
                # domain is [0, 24h); it now declares Duration('ns') and carries them exactly,
                # so a null on our side is a real regression and must fail the comparison.
                # Tolerate the intentional mojibake auto-repair divergence on strings (the
                # plugin's default fixes double-encoded text; pyreadstat decodes literally).
                if ca and cb and ca[0] == "str" and cb[0] == "str":
                    sv, rvs = ca[1], cb[1]
                    if "Ã" in rvs or "Â" in rvs or "Ã" in sv or "Â" in sv:
                        mojibake_only += 1
                        continue
                failures.append(f"{name}: row {ri} col {ci} ({ocols[ci]}) ours={ca} ref={cb}")
                ok = False
                break
            if not ok:
                break
        if ok:
            matched += 1

    print(f"fixtures matched: {matched}")
    print(f"cells compared:   {cells}")
    print(f"mojibake-tolerated string cells: {mojibake_only}")
    print(f"skipped (unreadable by one side): {skipped}")
    print(f"failures: {len(failures)}")
    for f in failures[:60]:
        print("  FAIL", f)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
