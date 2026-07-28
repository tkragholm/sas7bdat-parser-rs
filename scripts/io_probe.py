#!/usr/bin/env python3
"""Probe the storage a SAS7BDAT conversion will run against.

Answers three questions before you spend hours on a large convert:

  1. Which drives are local and which are network, and where is there free space?
  2. How fast can this host read the file sequentially? (the floor for any tool)
  3. Do CONCURRENT reads go faster than one stream? (SMB usually says yes, which is
     what decides whether throwing cores at the problem can help)

Standard library only — copy it to the server and run it. Nothing is written or
modified; every operation is a read.

    python io_probe.py                          # just list the drives
    python io_probe.py \\\\server\\share\\big.sas7bdat
    python io_probe.py Z:\\data\\big.sas7bdat --sample-gb 5 --threads 1,4,8,16
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import threading
import time

MB = 1024 * 1024
GB = 1024 * MB

DRIVE_TYPES = {
    0: "unknown",
    1: "no root dir",
    2: "removable",
    3: "local disk",
    4: "NETWORK",
    5: "cd-rom",
    6: "ram disk",
}


def list_drives() -> None:
    """Print each drive with its type and free space."""
    print("=== drives ===")
    if os.name != "nt":
        usage = shutil.disk_usage(os.sep)
        print(f"  {os.sep}  free {usage.free / GB:,.1f} GB of {usage.total / GB:,.1f} GB")
        print("  (drive-type detection is Windows-only)")
        return

    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    buf = ctypes.create_unicode_buffer(1024)
    kernel32.GetLogicalDriveStringsW(len(buf), buf)
    roots = [r for r in buf[:].split("\0") if r]

    print(f"  {'drive':<8} {'type':<12} {'free GB':>12} {'total GB':>12}  UNC target")
    for root in roots:
        kind = DRIVE_TYPES.get(kernel32.GetDriveTypeW(root), "?")
        try:
            usage = shutil.disk_usage(root)
            free, total = f"{usage.free / GB:,.1f}", f"{usage.total / GB:,.1f}"
        except OSError:
            free = total = "n/a"

        unc = ""
        if kind == "NETWORK":
            size = wintypes.DWORD(1024)
            name = ctypes.create_unicode_buffer(1024)
            mpr = ctypes.WinDLL("mpr", use_last_error=True)
            if mpr.WNetGetConnectionW(root.rstrip("\\"), name, ctypes.byref(size)) == 0:
                unc = name.value
        print(f"  {root:<8} {kind:<12} {free:>12} {total:>12}  {unc}")


def sequential_read(path: str, sample_bytes: int, block: int = 8 * MB) -> float:
    """Read the first `sample_bytes` in one stream. Returns MB/s."""
    buf = bytearray(block)
    view = memoryview(buf)
    read_total = 0
    start = time.perf_counter()
    with open(path, "rb", buffering=0) as handle:
        while read_total < sample_bytes:
            got = handle.readinto(view)
            if not got:
                break
            read_total += got
    elapsed = time.perf_counter() - start
    return (read_total / MB) / elapsed if elapsed > 0 else 0.0


def parallel_read(path: str, sample_bytes: int, threads: int, block: int = 8 * MB) -> float:
    """Read `sample_bytes` split across `threads` readers at different offsets.

    Each thread opens its own handle and seeks to its own stripe, so this measures
    whether the storage rewards multiple outstanding requests — the property that
    decides whether parallel decode can help over a network share.
    """
    span = sample_bytes // threads
    counters = [0] * threads

    def worker(index: int) -> None:
        buf = bytearray(block)
        view = memoryview(buf)
        offset = index * span
        end = offset + span
        with open(path, "rb", buffering=0) as handle:
            handle.seek(offset)
            while offset < end:
                got = handle.readinto(view)
                if not got:
                    break
                offset += got
                counters[index] += got

    workers = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    start = time.perf_counter()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = time.perf_counter() - start
    return (sum(counters) / MB) / elapsed if elapsed > 0 else 0.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file", nargs="?", help="file to probe (the .sas7bdat you plan to convert)")
    parser.add_argument("--sample-gb", type=float, default=4.0, help="GB to read per measurement (default 4)")
    parser.add_argument("--threads", default="1,4,8,16", help="comma-separated reader counts (default 1,4,8,16)")
    args = parser.parse_args()

    list_drives()
    if not args.file:
        print("\n(pass a file path to measure read throughput)")
        return 0

    path = args.file
    if not os.path.isfile(path):
        print(f"\nnot a file: {path}", file=sys.stderr)
        return 1

    size = os.path.getsize(path)
    sample = min(int(args.sample_gb * GB), size)
    print(f"\n=== {path} ===")
    print(f"  size          {size / GB:,.2f} GB")
    print(f"  sampling      {sample / GB:,.2f} GB per measurement\n")

    base = sequential_read(path, sample)
    print(f"  {'readers':>8}  {'MB/s':>9}  {'vs 1 stream':>12}   projected full pass")
    print(f"  {1:>8}  {base:>9,.0f}  {'1.00x':>12}   {size / MB / base / 60:>6,.1f} min  (sequential)")

    for count in [int(t) for t in args.threads.split(",") if t.strip() and int(t) > 1]:
        rate = parallel_read(path, sample, count)
        speedup = rate / base if base else 0
        print(f"  {count:>8}  {rate:>9,.0f}  {speedup:>11,.2f}x   {size / MB / rate / 60:>6,.1f} min")

    print("\nHow to read this:")
    print("  * The 'projected full pass' column is the floor for ONE read of the file.")
    print("    A convert must read all of it, so nothing can beat that number.")
    print("  * If more readers scale well, concurrent I/O helps and parallel decode is")
    print("    worth adding. If it stays flat, the link is saturated and the only")
    print("    remaining lever is moving the data or reading it fewer times.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
