# Network-storage lab

A container that makes a local directory behave like a slow network share, for
comparing how changes to the reader perform on remote storage.

```sh
test-lab/lab.sh --build                       # first run, or after changing the CLI

LATENCY=5 RATE=340 test-lab/lab.sh \
    sas7bdat convert /mnt/in/big.sas7bdat --out-dir /mnt/out --io-backend buffered

LATENCY=5 RATE=340 test-lab/lab.sh \
    python3 /usr/local/bin/probe.py /mnt/in/big.sas7bdat only=io

test-lab/lab.sh                               # a shell
```

`fixtures/` is mounted read-only at `/mnt/in` and `target/lab-out/` at
`/mnt/out`; `IN=` and `OUT=` override them. `LATENCY` is in milliseconds,
`RATE` in MB/s, and `0` means unlimited.

## What it models

`slowfs.py` is a read-only FUSE mirror with two costs:

- **Latency** per discontinuity: a handle reading sequentially pays once, and
  every seek pays again, as a client with read-ahead does.
- **Rate**: one token bucket shared by all handles, so concurrent readers
  compete for one link.

With these two costs, throughput rises with the number of readers until the
bandwidth limit is reached. `LATENCY=5 RATE=340` is a reasonable starting
point for an SMB share on a fast LAN.

## What it does not model

- SMB itself: credit accounting, request splitting and pipelining in the
  Windows client, multichannel, oplocks and leases.
- The cost of memory-mapping a file on a share. Over FUSE a mapped file is
  served from the page cache with kernel read-ahead, so `--io-backend mmap`
  measures about the same as `buffered` here, while on a real share it is much
  slower. That is why `auto` does not map network paths, and it cannot be
  tested here.

Use the lab for relative comparisons. Confirm absolute numbers on real storage
with `scripts/probe.py`.

FUSE is used instead of a Samba container because the OrbStack kernel has no
`cifs` client module, and FUSE charges latency exactly where intended, so runs
repeat.
