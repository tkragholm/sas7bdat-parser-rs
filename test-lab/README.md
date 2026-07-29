# Network-storage lab

A container that makes a local directory read like a slow, distant share, so changes to the
reader can be measured without booking time on the real server.

```sh
test-lab/lab.sh --build                       # first run, or after changing the CLI

LATENCY=5 RATE=340 test-lab/lab.sh \
    sas7bdat convert /mnt/in/big.sas7bdat --out-dir /mnt/out --io-backend buffered

LATENCY=5 RATE=340 test-lab/lab.sh \
    python3 /usr/local/bin/probe.py /mnt/in/big.sas7bdat only=io

test-lab/lab.sh                               # a shell, for anything else
```

`fixtures/` is mounted read-only at `/mnt/in` and `target/lab-out/` writable at `/mnt/out`;
`IN=` and `OUT=` override them. `LATENCY` is milliseconds, `RATE` is MB/s, and `0` means
unlimited.

## What it models

`slowfs.py` is a read-only FUSE mirror charging two costs:

- **latency**, per discontinuity. A handle that keeps reading where it left off pays once and
  then streams; a seek pays again. That is how a client with read-ahead behaves — it pipelines
  a contiguous run but cannot predict a jump.
- **rate**, a token bucket shared by every handle, so concurrent readers compete for one link
  instead of each getting their own.

Nothing tells it how to respond to concurrency; that falls out. N readers each amortise their
own latency, so throughput climbs with N until the bucket is the limit and then stops. The
same curve the SMB sweep produced, from the same two causes.

Starting points from the measured share: `LATENCY=5 RATE=340`, which lands a single stream near
the 176 MB/s that was measured and four readers near 340.

## What it does not model

**It is not SMB.** The lab reproduces the performance envelope the reader was tuned against —
round-trip cost, a bandwidth ceiling, the shape of the concurrency curve — not the protocol.
Absent: SMB2 credit accounting, the Windows redirector splitting a large read into 1 MB
requests and pipelining them, multichannel, oplocks and leases. The Linux and Windows clients
differ anyway, so no container was ever going to settle those.

It also does **not** reproduce the memory-mapping pathology. Over FUSE a mapped file is served
by the page cache with kernel read-ahead, so `--io-backend mmap` measures about the same here
as `buffered` — on a real share it is far slower, which is why `Auto` declines to map one.
Nothing about that decision can be tested in this lab.

Use it for comparisons — *does this change help, and by how much?* Confirm absolute numbers on
the server with `scripts/probe.py`.

For a sense of what it should report, at `LATENCY=5 RATE=340`:

```text
readers   1 -> 122 MB/s   2 -> 230   4 -> 316   8 -> 334      (measured share: 176 / - / 323 / 280)
grf14_lea_blkgrp.sas7bdat, 37 MB:  default 156 ms   --parse-threads 1  2.6 s
```

The lab flattens where the real share falls off past four readers, so it will not find a
concurrency ceiling for you. It does show the cost of losing parallelism, plainly.

## Why FUSE and not a real SMB server

A Samba container with `tc netem` on the loopback would have been closer to the protocol, but
the OrbStack kernel ships no `cifs` client module, so nothing in the container can mount the
share it serves. FUSE is present and, for the questions this lab answers, more precise:
latency is charged exactly where intended instead of emerging from a queueing discipline, so
runs repeat.
