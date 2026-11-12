"""Run one command, then report its wall time and peak memory.

Usage: measure.py <command> [args...]

Passes the command's stdout and stderr through, then prints one last stdout line
`MEASURE {"wall_s": ..., "peak_rss_bytes": ..., "returncode": ...}`. Peak memory
is the largest resident set of the command or of any descendant it waited for, from
getrusage(RUSAGE_CHILDREN); for a command that runs several worker processes it is
the largest single process, not their sum. It is null on platforms without the
`resource` module (Windows).
"""

from __future__ import annotations

import json
import subprocess
import sys
import time

try:
    import resource
except ImportError:  # Windows
    resource = None


def main() -> None:
    cmd = sys.argv[1:]
    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, check=False)
    wall = time.perf_counter() - start
    peak = None
    if resource is not None:
        peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        # Linux reports kilobytes, macOS and the BSDs bytes.
        if sys.platform.startswith("linux"):
            peak *= 1024
    sys.stdout.buffer.write(proc.stdout)
    sys.stderr.buffer.write(proc.stderr)
    print(
        "MEASURE "
        + json.dumps(
            {"wall_s": wall, "peak_rss_bytes": peak, "returncode": proc.returncode}
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
