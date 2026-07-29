#!/usr/bin/env python3
"""A read-only mirror of a directory that reads like a slow network share.

Two costs are modelled, which between them are what a conversion actually feels:

  latency  charged when a handle reads somewhere other than where it left off. A
           sequential run pays it once and then streams; a seek pays it again. That is
           how an SMB client behaves, because its read-ahead pipelines a contiguous run
           but cannot predict a jump.
  rate     a token bucket shared by every handle, so concurrent readers compete for one
           link rather than each getting their own.

Concurrency then behaves the way the real share does without being told to: N readers
each amortise their own latency, so throughput climbs with N until the bucket is the
limit and stops climbing.

This is not SMB. It reproduces the performance envelope that the reader was tuned
against — round-trip cost, a bandwidth ceiling, and the shape of the concurrency curve —
not the protocol. Credit accounting and the redirector's own read splitting are absent,
so treat the absolute numbers as a model and the comparisons as the result.

  slowfs.py SRC MOUNTPOINT [latency_ms] [rate_mbytes_per_s]
"""

import errno
import os
import sys
import threading
import time

from fuse import FUSE, FuseOSError, Operations


class Bucket:
    """Shared rate limit. The wait is computed under the lock and slept outside it, so
    readers queue for the link instead of for the mutex."""

    def __init__(self, rate):
        self.rate = rate
        self.ready = time.monotonic()
        self.lock = threading.Lock()

    def take(self, size):
        if not self.rate:
            return
        with self.lock:
            now = time.monotonic()
            self.ready = max(self.ready, now) + size / self.rate
            wait = self.ready - now
        if wait > 0:
            time.sleep(wait)


class SlowFS(Operations):
    def __init__(self, root, latency, rate):
        self.root = os.path.realpath(root)
        self.latency = latency
        self.bucket = Bucket(rate)
        self.tips = {}
        self.tip_guard = threading.Lock()

    def _real(self, path):
        return os.path.join(self.root, path.lstrip("/"))

    def getattr(self, path, fh=None):
        try:
            st = os.lstat(self._real(path))
        except OSError as err:
            raise FuseOSError(err.errno) from err
        return {
            key: getattr(st, key)
            for key in (
                "st_atime",
                "st_ctime",
                "st_gid",
                "st_mode",
                "st_mtime",
                "st_nlink",
                "st_size",
                "st_uid",
            )
        }

    def readdir(self, path, fh):
        return [".", ".."] + os.listdir(self._real(path))

    def open(self, path, flags):
        if flags & (os.O_WRONLY | os.O_RDWR):
            raise FuseOSError(errno.EROFS)
        return os.open(self._real(path), os.O_RDONLY)

    def read(self, path, size, offset, fh):
        # One round trip per discontinuity. The kernel splits a large read into several
        # FUSE requests, and those are contiguous, so a streaming reader pays once.
        with self.tip_guard:
            seek = self.tips.get(fh) != offset
            self.tips[fh] = offset + size
        if seek and self.latency:
            time.sleep(self.latency)
        data = os.pread(fh, size, offset)
        self.bucket.take(len(data))
        return data

    def release(self, path, fh):
        with self.tip_guard:
            self.tips.pop(fh, None)
        return os.close(fh)

    def statfs(self, path):
        st = os.statvfs(self.root)
        return {
            key: getattr(st, key)
            for key in (
                "f_bavail",
                "f_bfree",
                "f_blocks",
                "f_bsize",
                "f_favail",
                "f_ffree",
                "f_files",
                "f_flag",
                "f_frsize",
                "f_namemax",
            )
        }


def main():
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    src, mount = sys.argv[1], sys.argv[2]
    latency = (float(sys.argv[3]) if len(sys.argv) > 3 else 0.0) / 1000.0
    rate = (float(sys.argv[4]) if len(sys.argv) > 4 else 0.0) * 1024 * 1024
    FUSE(
        SlowFS(src, latency, rate),
        mount,
        foreground=True,
        nothreads=False,
        allow_other=True,
        direct_io=True,
        max_read=1 << 20,
        ro=True,
    )


if __name__ == "__main__":
    main()
