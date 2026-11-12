"""Run one command, then report its wall time and peak memory.

Usage: measure.py <command> [args...]

Passes the command's stdout and stderr through, then prints one last stdout line
`MEASURE {"wall_s": ..., "peak_rss_bytes": ..., "peak_footprint_bytes": ...,
"returncode": ...}`.

Two memory figures, because they differ for a reader that memory-maps its input:

- peak_rss_bytes is the largest resident set. It counts the pages of a mapped file
  that have been touched, which is file cache the system can drop at any time, so
  it overstates what such a reader holds.
- peak_footprint_bytes is the memory the process itself holds. On macOS it is the
  kernel's peak physical footprint, the figure the system uses for memory pressure,
  read from /usr/bin/time -l. On Linux it is the peak of anonymous and shared
  resident memory summed over the command's process tree, sampled every 10 ms from
  /proc, so a short spike between samples can be missed.

For a command that runs several worker processes, the macOS figures are those of
the largest single process. Both are null on other platforms.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    import resource
except ImportError:  # Windows
    resource = None

DARWIN = sys.platform == "darwin"
LINUX = sys.platform.startswith("linux")
REAL_LINE = re.compile(rb"^\s*\d+\.\d+ real\s")
TIME_LINE = re.compile(
    rb"^\s*(\d+)\s+(maximum resident set size|peak memory footprint)\s*$"
)


def _tree_anon_bytes(root: int) -> int:
    """RssAnon + RssShmem of root and all its descendants, from /proc."""
    children: dict[int, list[int]] = {}
    for stat in Path("/proc").glob("[0-9]*/stat"):
        try:
            raw = stat.read_text()
        except OSError:
            continue
        ppid = int(raw[raw.rindex(")") + 2 :].split()[1])
        children.setdefault(ppid, []).append(int(stat.parent.name))
    total, todo = 0, [root]
    while todo:
        pid = todo.pop()
        todo.extend(children.get(pid, []))
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith(("RssAnon:", "RssShmem:")):
                    total += int(line.split()[1]) * 1024
        except OSError:
            pass
    return total


def main() -> None:
    cmd = sys.argv[1:]
    if DARWIN:
        cmd = ["/usr/bin/time", "-l", *cmd]
    start = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    peak_anon = 0
    done: threading.Event | None = None
    sampler: threading.Thread | None = None
    if LINUX:
        done = threading.Event()

        def sample() -> None:
            nonlocal peak_anon
            while not done.is_set():
                peak_anon = max(peak_anon, _tree_anon_bytes(proc.pid))
                done.wait(0.01)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
    out, err = proc.communicate()
    wall = time.perf_counter() - start
    if done is not None and sampler is not None:
        done.set()
        sampler.join()

    rss = footprint = None
    if DARWIN:
        # time -l appends its report, starting with the "real ... user ... sys" line.
        lines = err.splitlines(keepends=True)
        cut = max((i for i, line in enumerate(lines) if REAL_LINE.match(line)), default=None)
        if cut is not None:
            for line in lines[cut:]:
                m = TIME_LINE.match(line)
                if m and m.group(2) == b"maximum resident set size":
                    rss = int(m.group(1))
                elif m:
                    footprint = int(m.group(1))
            err = b"".join(lines[:cut])
    elif resource is not None:
        rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        if LINUX:
            rss *= 1024  # kilobytes on Linux
            footprint = peak_anon

    sys.stdout.buffer.write(out)
    sys.stderr.buffer.write(err)
    print(
        "MEASURE "
        + json.dumps(
            {
                "wall_s": wall,
                "peak_rss_bytes": rss,
                "peak_footprint_bytes": footprint,
                "returncode": proc.returncode,
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
