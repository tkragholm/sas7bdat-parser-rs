#!/usr/bin/env python3
"""Run the reader benchmark and write the results.

Usage: run.py [--reps N] [--files a,b] [--tools x,y] [--out DIR]

Every measurement is one fresh process that reads one file once. Tools are
interleaved, in an order that rotates each repetition, so drift on the machine
spreads over all of them. Each file is read twice before its runs so that every
tool starts from a warm page cache.

Every finished run is appended to runs.jsonl at once. Stopped with Ctrl-C or
SIGTERM, the run in progress is discarded and the command to continue is printed;
`--resume` then skips what is recorded, with the first session's plan.

Per run it records the read time measured inside the process (excluding
interpreter start-up and imports), the wall time of the whole process, and its
peak memory as footprint and as resident set (see measure.py for why both). After the runs it compares each reader's
checksum (rows, columns, per-column sums and string lengths) with the Rust
core's, so a fast but wrong reader shows up.

Runs on Linux and macOS. On Windows it runs without peak memory and without the
check for other busy processes. It builds the Rust binaries it needs with cargo
(profile `dist`) and compiles readstat_read.c with the C compiler on PATH.

Files are expected in fixtures/benchmark/ (fetch.py downloads them) and are
checked against the SHA-256 values below. Tools are found on PATH, or through the
environment variables RSCRIPT, READSTAT and READSTAT_PREFIX, and the Python readers
run in benchmarks/.venv.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "fixtures" / "benchmark"
TARGET = Path(
    os.environ.get("CARGO_TARGET_DIR")
    or json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )["target_directory"]
)
WINDOWS = os.name == "nt"
EXE = ".exe" if WINDOWS else ""
VENV = ROOT / "benchmarks" / ".venv"
PYTHON = VENV / ("Scripts/python.exe" if WINDOWS else "bin/python")
RSCRIPT = os.environ.get("RSCRIPT") or shutil.which("Rscript") or "Rscript"
READSTAT = os.environ.get("READSTAT") or shutil.which("readstat") or "readstat"
DIST = TARGET / "dist"
CORE = DIST / "examples" / f"bench_one{EXE}"
CLI = DIST / f"sas7bdat{EXE}"
READSTAT_C = DIST / f"readstat_read{EXE}"
MEASURE = ROOT / "benchmarks" / "measure.py"


def build_rust() -> None:
    """Build the core benchmark and the CLI with the release profile users get."""
    for args in (["-p", "sas7bdat", "--example", "bench_one"],
                 ["-p", "sas7bdat-cli", "--bin", "sas7bdat"]):
        subprocess.run(["cargo", "build", "--quiet", "--profile", "dist", *args],
                       cwd=ROOT, check=True)


def build_readstat_c() -> None:
    """Compile benchmarks/readstat_read.c against the installed ReadStat library."""
    src = ROOT / "benchmarks" / "readstat_read.c"
    if READSTAT_C.exists() and READSTAT_C.stat().st_mtime > src.stat().st_mtime:
        return
    prefix = Path(os.environ.get("READSTAT_PREFIX")
                  or Path(shutil.which(READSTAT) or READSTAT).resolve().parent.parent)
    cc = os.environ.get("CC") or shutil.which("cc") or shutil.which("gcc") or "clang"
    cmd = [cc, "-O2", "-o", str(READSTAT_C), str(src)]
    if (prefix / "include" / "readstat.h").exists():
        cmd.insert(2, f"-I{prefix / 'include'}")
    if (prefix / "lib").is_dir():
        cmd.append(f"-L{prefix / 'lib'}")
    cmd += ["-lreadstat", "-lm"]
    subprocess.run(cmd, check=True)

FILES = {
    "ahs2013n": (
        "ahs2013n.sas7bdat",
        "e9b54d1c2e43a0615b0bcd94a6fb88bf8201a36a8a67cb5e1da11896bc794625",
    ),
    "ahs2013_household": (
        "ahs2013_household.sas7bdat",
        "83a98f165a6f95318d8ca801cc8ca95b637860e7c5433dfdeafdb24742c5220a",
    ),
    "meps2021_h233": (
        "meps2021_h233.sas7bdat",
        "c3f1a0ecaccb61798b99a778d0a4c72c9f327c663d5baf29fa7658c75dc76275",
    ),
    "ahs2013_person": (
        "ahs2013_person.sas7bdat",
        "3c460bbdf2b4465fbef611dee1d5b8801f8258c88606809ce76cdf2cebf52393",
    ),
    "ahs2013_topical": (
        "ahs2013_topical.sas7bdat",
        "e64991b567f110bb5d0e5dddc21ae32b543373c15550262bba713b5c30b3d4b4",
    ),
}


# (group, command builder). The group is the comparison a tool belongs to.
def _core(path, *extra):
    return [str(CORE), str(path), *extra]


def _py(tool):
    return lambda path: [
        str(PYTHON),
        str(ROOT / "benchmarks" / "read_py.py"),
        tool,
        str(path),
    ]


def _r(tool):
    return lambda path: [
        RSCRIPT,
        str(ROOT / "benchmarks" / "read_r.R"),
        tool,
        str(path),
    ]


TOOLS = {
    "sas7bdat-core": ("rust", lambda p: _core(p)),
    "sas7bdat-core-1t": ("rust", lambda p: _core(p, "1")),
    "readstat-c": ("rust", lambda p: [str(READSTAT_C), str(p)]),
    "sas7bdat-polars": ("python", _py("sas7bdat-polars")),
    "polars-readstat": ("python", _py("polars-readstat")),
    "pyreadstat": ("python", _py("pyreadstat")),
    "pyreadstat-mp": ("python", _py("pyreadstat-mp")),
    "pandas": ("python", _py("pandas")),
    "fastsas": ("r", _r("fastsas")),
    "haven": ("r", _r("haven")),
    "sas7bdat-cli-csv": ("csv", None),
    "readstat-cli-csv": ("csv", None),
}

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _procs_linux(interval: float = 0.5) -> list[tuple[int, int, float, str]]:
    """(pid, ppid, cpu percent over `interval`, name) from two reads of /proc."""

    def read() -> dict[int, tuple[int, int, str]]:
        out = {}
        for stat in Path("/proc").glob("[0-9]*/stat"):
            try:
                raw = stat.read_text()
            except OSError:
                continue
            name = raw[raw.index("(") + 1 : raw.rindex(")")]
            fields = raw[raw.rindex(")") + 2 :].split()
            # After the name: state ppid ... utime is field 14, stime 15 (1-based).
            out[int(stat.parent.name)] = (int(fields[1]), int(fields[11]) + int(fields[12]), name)
        return out

    first = read()
    time.sleep(interval)
    second = read()
    ticks = os.sysconf("SC_CLK_TCK") * interval
    return [(pid, ppid, 100 * (used - first.get(pid, (0, used, ""))[1]) / ticks, name)
            for pid, (ppid, used, name) in second.items()]


def _procs_ps() -> list[tuple[int, int, float, str]]:
    """(pid, ppid, cpu percent, name) from ps, whose %CPU on macOS is a recent average."""
    rows = subprocess.run(["ps", "-Ao", "pid=,ppid=,pcpu=,comm="], capture_output=True,
                          text=True, check=True).stdout.splitlines()
    procs = []
    for line in rows:
        parts = line.split(None, 3)
        if len(parts) == 4:
            procs.append((int(parts[0]), int(parts[1]), float(parts[2]), parts[3]))
    return procs


def other_cpu() -> tuple[float, str]:
    """CPU use, in percent of one core, of processes outside this benchmark.

    Returns the total and the busiest such process. A benchmark run is only
    comparable when this is close to zero. Linux procps reports %CPU averaged over a
    process's whole life, so there it is measured from /proc instead. Not available
    on Windows, where it returns zero.
    """
    if WINDOWS:
        return 0.0, "not measured on Windows"
    me = os.getpid()
    procs = _procs_linux() if Path("/proc/self/stat").exists() else _procs_ps()
    parent = {pid: ppid for pid, ppid, _, _ in procs}

    def ours(pid: int) -> bool:
        while pid > 1:
            if pid == me:
                return True
            pid = parent.get(pid, 1)
        return False

    others = [(cpu, comm) for pid, _, cpu, comm in procs if not ours(pid)]
    total = sum(cpu for cpu, _ in others)
    busiest = max(others, default=(0.0, ""))
    return total, f"{busiest[1].rsplit('/', 1)[-1]} {busiest[0]:.0f}%"


def warm(path: Path) -> None:
    for _ in range(2):
        with path.open("rb") as f:
            while f.read(1 << 24):
                pass


def _run_group(cmd: list[str], timeout: float,
               env: dict | None = None) -> subprocess.CompletedProcess:
    """Run cmd in its own process group, and kill the whole group if interrupted.

    A reader can start processes of its own (Rscript starts R, pyreadstat workers),
    so killing only the direct child could leave one running into the next
    measurement.
    """
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             env=env,
                             start_new_session=not WINDOWS)
    try:
        out, err = popen.communicate(timeout=timeout)
    except BaseException:
        if WINDOWS:
            popen.kill()
        else:
            try:
                os.killpg(popen.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        popen.wait()
        raise
    return subprocess.CompletedProcess(cmd, popen.returncode, out, err)


def _stop(signum, frame):
    raise KeyboardInterrupt


def run_one(tool: str, path: Path, scratch: Path, checksum: bool = False) -> dict:
    group, build = TOOLS[tool]
    if tool == "sas7bdat-cli-csv":
        out = scratch / "ours.csv"
        cmd = [
            str(CLI),
            "convert",
            str(path),
            "--out",
            str(out),
            "--overwrite",
        ]
    elif tool == "readstat-cli-csv":
        out = scratch / "readstat.csv"
        out.unlink(missing_ok=True)
        cmd = [READSTAT, str(path), str(out)]
    else:
        out = None
        cmd = build(path)
    load = os.getloadavg()[0] if hasattr(os, "getloadavg") else None
    others, _ = other_cpu()
    env = dict(os.environ, BENCH_CHECKSUM="1" if checksum else "0")
    proc = _run_group([sys.executable, str(MEASURE), *cmd], timeout=1800, env=env)
    lines = proc.stdout.strip().splitlines()
    measured = json.loads(lines.pop()[len("MEASURE "):]) if lines and lines[-1].startswith(
        "MEASURE ") else {"wall_s": None, "peak_rss_bytes": None, "peak_footprint_bytes": None,
           "returncode": proc.returncode}
    wall = measured["wall_s"]
    peak = measured["peak_rss_bytes"]
    footprint = measured.get("peak_footprint_bytes")
    ok = proc.returncode == 0 and measured["returncode"] == 0
    row = {
        "tool": tool,
        "group": group,
        "wall_s": wall,
        "load1": load,
        "other_cpu": others,
        "peak_rss_mb": peak / 1e6 if peak is not None else None,
        "peak_footprint_mb": footprint / 1e6 if footprint is not None else None,
        "ok": ok,
        "read_s": None,
        "rows": None,
        "cols": None,
        "checksum": None,
    }
    if not ok:
        row["error"] = proc.stderr.strip().splitlines()[-1:] or ["?"]
        return row
    if out is None:
        result = json.loads(lines[-1])
        row.update(
            read_s=result["seconds"],
            rows=result["rows"],
            cols=result["cols"],
            checksum=result["checksum"],
        )
    else:
        row["read_s"] = wall
        with out.open("rb") as f:
            row["rows"] = sum(1 for _ in f) - 1
        out.unlink(missing_ok=True)
    return row


def compare(ref: dict, other: dict) -> list[str]:
    problems = []
    if other["rows"] != ref["rows"]:
        problems.append(f"rows {other['rows']} != {ref['rows']}")
    if ref["checksum"] is None or other["checksum"] is None:
        return problems
    if other["cols"] != ref["cols"]:
        problems.append(f"cols {other['cols']} != {ref['cols']}")
    compared = 0
    for name, theirs in other["checksum"].items():
        ours = ref["checksum"].get(name)
        if ours is None or ours[0] != theirs[0]:
            continue
        compared += 1
        if ours[0] == "num":
            tol = 1e-9 * max(abs(ours[1]), abs(theirs[1])) + 1e-6
            if abs(ours[1] - theirs[1]) > tol or ours[2] != theirs[2]:
                problems.append(f"{name}: {theirs[1:]} != {ours[1:]}")
        elif ours[1] != theirs[1]:
            problems.append(f"{name}: length {theirs[1]} != {ours[1]}")
    problems.append(f"({compared} columns compared)")
    return problems


def environment(files: list[str]) -> dict:
    def out(cmd):
        try:
            return subprocess.run(
                cmd, capture_output=True, text=True, check=False, cwd=ROOT
            ).stdout.strip()
        except OSError:
            return None

    def first_match(path: str, key: str) -> str | None:
        try:
            for line in Path(path).read_text().splitlines():
                if line.startswith(key):
                    return line.split(":", 1)[1].strip()
        except OSError:
            pass
        return None

    def cpu_name() -> str | None:
        if sys.platform == "darwin":
            return out(["sysctl", "-n", "machdep.cpu.brand_string"])
        return first_match("/proc/cpuinfo", "model name") or platform.processor() or None

    def memory_gb() -> float | None:
        try:
            return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
        except (ValueError, OSError, AttributeError):
            return None

    def power() -> str | None:
        if sys.platform == "darwin":
            lines = (out(["pmset", "-g", "batt"]) or "").splitlines()
            return lines[0] if lines else None
        supplies = sorted(Path("/sys/class/power_supply").glob("*/status"))
        return "; ".join(f"{p.parent.name} {p.read_text().strip()}" for p in supplies) or None

    r_versions = out(
        [
            RSCRIPT,
            "-e",
            'cat(R.version.string, "| haven", as.character(packageVersion("haven")),'
            ' "| fastsas", as.character(packageVersion("fastsas")))',
        ]
    )
    return {
        "date": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "commit": out(["git", "rev-parse", "HEAD"]),
        "dirty": bool(out(["git", "status", "--porcelain", "--untracked-files=no"])),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu": cpu_name(),
        "cores": os.cpu_count(),
        "memory_gb": memory_gb(),
        "power": power(),
        "rustc": out(["rustc", "--version"]),
        "python_packages": out([str(PYTHON), "-m", "pip", "freeze"])
        or out(["uv", "pip", "freeze", "--python", str(PYTHON)]),
        "r": r_versions,
        "readstat": out([READSTAT, "--version"]),
        "files": {
            k: {
                "name": FILES[k][0],
                "sha256": FILES[k][1],
                "bytes": (DATA / FILES[k][0]).stat().st_size,
            }
            for k in files
        },
    }


def summarise(rows: list[dict], files: list[str], tools: list[str]) -> str:
    lines = [
        "| File | Tool | Read, median (s) | IQR (s) | Process (s) | Peak footprint (MB) "
        "| Peak RSS (MB) | MB/s |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for f in files:
        size = (DATA / FILES[f][0]).stat().st_size / 1e6
        for t in tools:
            runs = [r for r in rows if r["file"] == f and r["tool"] == t and r["ok"]]
            if not runs:
                lines.append(f"| {f} | {t} | failed | | | | | |")
                continue
            reads = sorted(r["read_s"] for r in runs)
            q = statistics.quantiles(reads, n=4) if len(reads) > 1 else [reads[0]] * 3
            med = statistics.median(reads)
            # Process time and memory from the runs that did not compute a checksum.
            clean = [r for r in runs if not r.get("checksummed")] or runs
            wall = statistics.median(r["wall_s"] for r in clean)
            rss = [r["peak_rss_mb"] for r in clean if r["peak_rss_mb"] is not None]
            rss_cell = f"{statistics.median(rss):.0f}" if rss else "n/a"
            fps = [r["peak_footprint_mb"] for r in clean if r.get("peak_footprint_mb") is not None]
            fp_cell = f"{statistics.median(fps):.0f}" if fps else "n/a"
            lines.append(
                f"| {f} | {t} | {med:.3f} | {q[2] - q[0]:.3f} | {wall:.3f} | "
                f"{fp_cell} | {rss_cell} | {size / med:.0f} |"
            )
    return "\n".join(lines)


#: Environment fields that must not change between the sessions of one run.
STABLE = ("commit", "dirty", "machine", "cpu", "cores", "rustc", "python_packages", "r",
          "readstat")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def append_jsonl(path: Path, record: dict) -> None:
    with path.open("a") as fh:
        fh.write(json.dumps(record) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=10)
    ap.add_argument("--files", default=",".join(FILES))
    ap.add_argument("--tools", default=",".join(TOOLS))
    ap.add_argument("--out", type=Path)
    ap.add_argument("--resume", action="store_true",
                    help="continue the run in --out, with its first session's files, "
                         "tools and repetitions, skipping the runs already recorded")
    ap.add_argument("--allow-changed-env", action="store_true",
                    help="resume even if the commit, versions or machine have changed")
    ap.add_argument("--skip-sha", action="store_true")
    ap.add_argument("--wait-idle", type=float, default=15.0,
                    help="minutes to wait for other processes to settle before giving up")
    ap.add_argument("--max-other-cpu", type=float, default=50.0,
                    help="refuse to start when other processes use more CPU than this "
                         "(percent of one core)")
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, _stop)

    out_dir = args.out or ROOT / "benchmarks" / "results" / dt.date.today().isoformat()
    sessions_path = out_dir / "sessions.jsonl"
    runs_path = out_dir / "runs.jsonl"
    sessions = read_jsonl(sessions_path)
    if args.resume:
        if not sessions:
            sys.exit(f"nothing to resume in {out_dir}")
        first = sessions[0]["plan"]
        files, tools, reps = first["files"], first["tools"], first["reps"]
    else:
        if sessions:
            sys.exit(f"{out_dir} already holds a run; pass --resume, or choose another --out")
        files, tools, reps = args.files.split(","), args.tools.split(","), args.reps

    for f in files:
        path = DATA / FILES[f][0]
        if not path.exists():
            sys.exit(f"missing {path}; run benchmarks/fetch.py")
        if not args.skip_sha and sha256(path) != FILES[f][1]:
            sys.exit(f"checksum mismatch for {path}")

    build_rust()
    if "readstat-c" in tools:
        build_readstat_c()
    env = environment(files)
    if sessions and not args.allow_changed_env:
        before = sessions[0]["environment"]
        changed = [k for k in STABLE if before.get(k) != env.get(k)]
        if changed:
            sys.exit("the environment has changed since the first session ("
                     + ", ".join(changed) + "); pass --allow-changed-env to resume anyway")
    deadline = time.monotonic() + 60 * args.wait_idle
    calm = 0
    while calm < 3:
        total, busiest = other_cpu()
        calm = calm + 1 if total <= args.max_other_cpu else 0
        if calm < 3:
            if calm == 0 and time.monotonic() > deadline:
                sys.exit(f"other processes use {total:.0f}% CPU (busiest: {busiest}); "
                         "close them, or raise --max-other-cpu or --wait-idle")
            if calm == 0:
                print(f"waiting for the machine to settle: other processes use "
                      f"{total:.0f}% CPU (busiest: {busiest})", flush=True)
            time.sleep(15)
    out_dir.mkdir(parents=True, exist_ok=True)
    session = len(sessions) + 1
    append_jsonl(sessions_path, {"session": session,
                                 "plan": {"files": files, "tools": tools, "reps": reps},
                                 "environment": env})
    (out_dir / "environment.json").write_text(json.dumps(env, indent=2))

    done = {(r["file"], r["rep"], r["tool"]) for r in read_jsonl(runs_path)}
    remaining = sum(1 for f in files for rep in range(reps) for t in tools
                    if (f, rep, t) not in done)
    print(f"session {session}: {remaining} of {len(files) * reps * len(tools)} runs to do",
          flush=True)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            scratch = Path(tmp)
            for f in files:
                path = DATA / FILES[f][0]
                todo = [(rep, t) for rep in range(reps)
                        for t in tools[rep % len(tools):] + tools[:rep % len(tools)]
                        if (f, rep, t) not in done]
                if not todo:
                    continue
                warm(path)
                for rep, t in todo:
                    # Only the first repetition computes the checksum, so the other
                    # nine measure process time and peak memory without it.
                    row = run_one(t, path, scratch, checksum=rep == 0)
                    row["checksummed"] = rep == 0
                    row.update(file=f, rep=rep, session=session,
                               at=dt.datetime.now().astimezone().isoformat(timespec="seconds"))
                    append_jsonl(runs_path, row)
                    status = (f"{row['read_s']:.3f}s" if row["ok"]
                              else f"FAILED {row.get('error')}")
                    print(f"{f} rep {rep + 1}/{reps} {t}: {status}", flush=True)
    except KeyboardInterrupt:
        sys.exit(f"\nstopped; continue with: benchmarks/run.py --out {out_dir} --resume")

    rows = read_jsonl(runs_path)
    fields = ["file", "rep", "tool", "group", "ok", "read_s", "wall_s", "peak_footprint_mb", "peak_rss_mb",
              "rows", "cols", "load1", "other_cpu", "checksummed", "session", "at"]
    with (out_dir / "runs.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    report = ["# Correctness", "",
              "Each reader's first successful run, compared with the Rust core's "
              "(`sas7bdat-core`).", ""]
    for f in files:
        ref = next((r for r in rows if r["file"] == f and r["tool"] == "sas7bdat-core"
                    and r["ok"] and r.get("checksum") is not None), None)
        if ref is None:
            report.append(f"- {f}: no successful sas7bdat-core run to compare against")
            continue
        for t in tools:
            if t == "sas7bdat-core":
                continue
            other = next((r for r in rows if r["file"] == f and r["tool"] == t and r["ok"]
                          and (r.get("checksum") is not None or r["group"] == "csv")), None)
            if other is None:
                report.append(f"- {f} / {t}: no successful run")
                continue
            problems = compare(ref, other)
            mismatches = [x for x in problems if not x.startswith("(")]
            notes = " ".join(x for x in problems if x.startswith("("))
            verdict = "; ".join(mismatches) if mismatches else "match"
            report.append(f"- {f} / {t}: {verdict} {notes}".rstrip())
    (out_dir / "correctness.md").write_text("\n".join(report) + "\n")

    n_sessions = len({r["session"] for r in rows})
    header = (f"{reps} repetitions per tool and file, recorded in {n_sessions} "
              f"session{'s' if n_sessions > 1 else ''}; see sessions.jsonl. Read time is the "
              "median of all repetitions; process time and peak memory are from the "
              "repetitions that did not compute the correctness checksum.\n\n")
    complete = all(any(r["file"] == f and r["rep"] == rep and r["tool"] == t for r in rows)
                   for f in files for rep in range(reps) for t in tools)
    if not complete:
        header = "Incomplete: some runs are missing. " + header
    (out_dir / "summary.md").write_text(header + summarise(rows, files, tools) + "\n")
    print((out_dir / "summary.md").read_text())


if __name__ == "__main__":
    main()
