#!/usr/bin/env python3
"""Measure the Python reader against another build of itself, and say where its
memory goes.

Two things this repository needed on 7 September 2026 and had no script for:

  * `compare`: the same files read by two or three installed builds of
    `sas7bdat-polars`, each in its own interpreter, best of N over M processes.
    That is how the Arrow-stream rewrite was judged against the plugin it
    replaced, with the published wheel as a third column to keep the core's
    own changes apart from the transport's.
  * `memory`: one build, one file, peak resident set after each stage of a
    read in a fresh process: the Rust decode alone, the batches imported and
    dropped, the frames held, the eager read, the single-stream import. The
    difference between two stages is what that stage holds.

Interpreters are given as `label=/path/to/python`; each needs polars and a
`sas7bdat_polars`. Files are any `.sas7bdat`. The measures are wall time, best
of `--repeat` per process over `--processes` processes, so a slow first run
and a busy neighbour do not become the number.

    scripts/bench_reader.py compare old=/tmp/old/bin/python new=.venv/bin/python a.sas7bdat b.sas7bdat
    scripts/bench_reader.py memory .venv/bin/python a.sas7bdat
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

# Runs inside each interpreter under test. Prints one JSON object.
_PROBE = r'''
import json, os, resource, sys, time
import polars as pl, sas7bdat_polars as sp
files, repeat = sys.argv[1:-1], int(sys.argv[-1])
def best(fn):
    ts = []
    out = None
    for _ in range(repeat):
        t0 = time.perf_counter(); out = fn(); ts.append(time.perf_counter() - t0)
    return min(ts), out
result = {"version": sp.__version__, "core": getattr(sp, "__core_version__", None), "polars": pl.__version__, "files": {}}
for f in files:
    info = sp.sas_info(f); schema = sp.schema_for_file(f)
    names = list(schema)
    numeric = next((c for c in names if schema[c] in (pl.Float64, pl.Int64)), None)
    text = next((c for c in names if schema[c] == pl.String), None)
    projection = [c for c in (numeric, text) if c][:2] or names[:1]
    sp.read_sas(f, columns=projection)  # warm the page cache
    r = {"rows": info["n_rows"], "cols": info["n_columns"], "mb": round(info["size_bytes"] / 1e6, 1), "projection": projection}
    t, df = best(lambda: sp.read_sas(f)); r["full_read_ms"] = t * 1000
    t, _ = best(lambda: sp.read_sas(f, columns=projection)); r["projected_read_ms"] = t * 1000
    t, _ = best(lambda: sum(b.height for b in sp.batch_reader(f))); r["batch_iteration_ms"] = t * 1000
    t, _ = best(lambda: sp.scan_sas(f, columns=projection).head(1000).collect()); r["scan_head_1000_ms"] = t * 1000
    if numeric:
        median = df[numeric].median()
        t, _ = best(lambda: sp.scan_sas(f).filter(pl.col(numeric) > median).select(projection).collect()); r["scan_filter_ms"] = t * 1000
    result["files"][os.path.basename(f)] = r
result["peak_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20
print(json.dumps(result))
'''

_STAGE = r'''
import resource, sys, polars as pl, sas7bdat_polars as sp
f, stage = sys.argv[1], sys.argv[2]
ds = sp.SasDataset(f)
base = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20
if stage == "rust":
    n = sum(1 for _ in ds._native.batches(None, None, None, None))
elif stage == "import":
    n = sum(pl.DataFrame(b).height for b in ds._native.batches(None, None, None, None))
elif stage == "hold":
    frames = [pl.DataFrame(b) for b in ds._native.batches(None, None, None, None)]
elif stage == "read":
    df = ds.read()
elif stage == "stream":
    df = pl.DataFrame(ds.stream())
print(base, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20)
'''

MEASURES = ["full_read_ms", "projected_read_ms", "batch_iteration_ms", "scan_filter_ms", "scan_head_1000_ms"]


def interpreter(spec: str) -> tuple[str, str]:
    label, _, path = spec.partition("=")
    if not path:
        raise SystemExit(f"interpreter must be label=/path/to/python, got {spec!r}")
    return label, path


def compare(args: argparse.Namespace) -> int:
    runs: dict[str, list[dict]] = {}
    for label, python in map(interpreter, args.interpreter):
        runs[label] = []
        for _ in range(args.processes):
            out = subprocess.run(
                [python, "-c", _PROBE, *args.file, str(args.repeat)],
                capture_output=True, text=True, check=True,
            ).stdout
            runs[label].append(json.loads(out))
    labels = list(runs)
    versions = {label: f"{runs[label][0]['version']} (core {runs[label][0]['core']}, polars {runs[label][0]['polars']})" for label in labels}
    for label, version in versions.items():
        print(f"{label}: sas7bdat-polars {version}")
    print()
    head = f"{'file':22} {'rows':>9} {'cols':>4}"
    for measure in MEASURES:
        head += f"  {measure[:-3]:>{7 * len(labels) + 2}}"
    print(head)
    for name in runs[labels[0]][0]["files"]:
        first = runs[labels[0]][0]["files"][name]
        row = f"{name[:22]:22} {first['rows']:>9} {first['cols']:>4}"
        for measure in MEASURES:
            cell = ""
            for label in labels:
                values = [r["files"][name].get(measure) for r in runs[label]]
                values = [v for v in values if v is not None]
                cell += f"{min(values):7.1f}" if values else f"{'':>7}"
            row += f"  {cell:>{7 * len(labels) + 2}}"
        print(row)
    print(f"\nms, best of {args.repeat} per process over {args.processes} processes; columns in each measure: {' / '.join(labels)}")
    print("peak RSS of the whole probe, MB: " + ", ".join(f"{label} {min(r['peak_rss_mb'] for r in runs[label]):.0f}" for label in labels))
    if args.json:
        Path(args.json).write_text(json.dumps(runs, indent=1))
    return 0


def memory(args: argparse.Namespace) -> int:
    print(f"{'stage':8} {'base MB':>8} {'peak MB':>8}   what the stage holds")
    notes = {
        "rust": "the decode alone: file mapping (if mapped), worker buffers, the channel",
        "import": "plus each batch imported into polars and dropped",
        "hold": "plus every imported frame kept",
        "read": "the eager read: frames concatenated without a rechunk",
        "stream": "the whole stream imported by polars in one call",
    }
    for stage in ("rust", "import", "hold", "read", "stream"):
        out = subprocess.run([args.python, "-c", _STAGE, args.file, stage], capture_output=True, text=True, check=True).stdout
        base, peak = (float(v) for v in out.split())
        print(f"{stage:8} {base:8.0f} {peak:8.0f}   {notes[stage]}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    c = sub.add_parser("compare", help="time two or more builds on the same files")
    c.add_argument("interpreter", nargs="+", help="label=/path/to/python, at least one; files follow")
    c.add_argument("--file", action="append", required=True, help="a .sas7bdat; repeatable")
    c.add_argument("--repeat", type=int, default=5)
    c.add_argument("--processes", type=int, default=2)
    c.add_argument("--json", help="also write every run's raw numbers here")
    c.set_defaults(run=compare)
    m = sub.add_parser("memory", help="peak RSS after each stage of a read")
    m.add_argument("python")
    m.add_argument("file")
    m.set_defaults(run=memory)
    args = parser.parse_args()
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
