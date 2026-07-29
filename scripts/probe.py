#!/usr/bin/env python3
"""probe.py [FILE] [k=v...]  keys: gb=4 t=1,2,4,8,16 b=1,4,8,16,32 rows=1000000 sink=PATH only=io|dec
Reads only, except sink=. No FILE lists drives. Run once per file: a rerun may hit the cache."""
import gc, os, shutil, sys, threading, time

M = 1 << 20
G = 1 << 30
A = dict(gb="4", t="1,2,4,8,16", b="1,4,8,16,32", rows="1000000", sink="", only="")
P = []
for a in sys.argv[1:]:
    k, _, v = a.partition("=")
    if v:
        A[k] = v
    else:
        P.append(a)
ints = lambda s: [int(x) for x in s.split(",") if x.strip()]
mins = lambda size, r: size / M / r / 60 if r else 0


def drives():
    print("=== drives ===")
    if os.name != "nt":
        u = shutil.disk_usage(os.sep)
        print(f"  {os.sep} free {u.free/G:,.1f} of {u.total/G:,.1f} GB (types are Windows-only)")
        return
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32")
    mpr = ctypes.WinDLL("mpr")
    T = {2: "removable", 3: "local", 4: "NETWORK", 5: "cdrom", 6: "ramdisk"}
    buf = ctypes.create_unicode_buffer(1024)
    k32.GetLogicalDriveStringsW(1024, buf)
    print(f"  {'drive':<7}{'type':<11}{'free GB':>11}{'total GB':>11}  UNC")
    for d in [x for x in buf[:].split("\0") if x]:
        t = T.get(k32.GetDriveTypeW(d), "?")
        try:
            u = shutil.disk_usage(d)
            f, tot = f"{u.free/G:,.1f}", f"{u.total/G:,.1f}"
        except OSError:
            f = tot = "n/a"
        unc = ""
        if t == "NETWORK":
            n = ctypes.create_unicode_buffer(1024)
            s = wintypes.DWORD(1024)
            if mpr.WNetGetConnectionW(d.rstrip("\\"), n, ctypes.byref(s)) == 0:
                unc = n.value
        print(f"  {d:<7}{t:<11}{f:>11}{tot:>11}  {unc}")


def read(path, off, nbytes, threads=1, block=8 * M):
    span = nbytes // threads
    got = [0] * threads

    def w(i):
        buf = memoryview(bytearray(block))
        o = off + i * span
        end = o + span
        with open(path, "rb", buffering=0) as h:
            h.seek(o)
            while o < end:
                n = h.readinto(buf)
                if not n:
                    break
                o += n
                got[i] += n

    ts = [threading.Thread(target=w, args=(i,)) for i in range(threads)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    e = time.perf_counter() - t0
    return (sum(got) / M) / e if e else 0


def io_probe(path):
    size = os.path.getsize(path)
    tc, bc = ints(A["t"]), ints(A["b"])
    # Each measurement gets a region nothing has read yet, or it times the page cache.
    smp = min(int(float(A["gb"]) * G), size // (len(tc) + len(bc)))
    if smp < 64 * M:
        print("  file too small to measure")
        return 0
    print(f"\n=== read {path} ===")
    print(f"  {size/G:,.2f} GB, {smp/G:,.2f} GB per measurement, each from fresh bytes")
    print(f"  {'readers':>8}{'MB/s':>9}{'x1':>7}   one pass")
    slot = base = best = bestc = 0
    for c in tc:
        r = read(path, slot * smp, smp, c)
        slot += 1
        base = base or r
        if r > best:
            best, bestc = r, c
        print(f"  {c:>8}{r:>9,.0f}{r/base:>6,.2f}x   {mins(size,r):>6,.1f} min")
    print(f"\n  block size at {bestc} readers")
    for b in bc:
        r = read(path, slot * smp, smp, bestc, b * M)
        slot += 1
        print(f"  {f'{b} MB':>8}{r:>9,.0f}{r/best:>6,.2f}x   {mins(size,r):>6,.1f} min")
    return best


def dec_probe(path):
    try:
        import polars as pl
        import sas7bdat_polars as sp
    except ImportError as e:
        print(f"\n(plugin not installed: {e})")
        return 0
    i = sp.sas_info(path)
    size = int(i.get("size_bytes") or os.path.getsize(path))
    tr = int(i["n_rows"])
    rl = int(i.get("row_length_bytes") or 0)
    rows = min(int(A["rows"]), tr)
    pb = rows * rl if rl else size * rows / max(tr, 1)
    print(f"\n=== decode {path} ===")
    print(f"  {tr:,} rows x {i['n_columns']:,} cols, row {rl:,} B, {i.get('encoding')}")
    print(f"  plugin {getattr(sp,'__version__','?')}, polars {pl.__version__}")
    print(f"  {pb/G:,.2f} GB of source per measurement")
    print(f"  {'threads':>8}{'sec':>8}{'MB/s':>9}{'x1':>7}   one pass")
    base = best = 0
    for c in ints(A["t"]):
        sp.set_scan_threads(c)
        gc.collect()
        t0 = time.perf_counter()
        f = sp.scan_sas(path, n_rows=rows).collect()
        e = time.perf_counter() - t0
        n = f.height
        del f
        r = (pb / M) / e if e else 0
        base = base or r
        best = max(best, r)
        flag = "" if n == rows else f"  ({n:,} rows)"
        print(f"  {c:>8}{e:>8,.1f}{r:>9,.0f}{r/base:>6,.2f}x   {mins(size,r):>6,.1f} min{flag}")
    if A["sink"]:
        sp.set_scan_threads(max(ints(A["t"])))
        gc.collect()
        t0 = time.perf_counter()
        sp.scan_sas(path, n_rows=rows).sink_parquet(A["sink"])
        e = time.perf_counter() - t0
        out = os.path.getsize(A["sink"]) if os.path.isfile(A["sink"]) else 0
        r = (pb / M) / e if e else 0
        print(f"\n  sink_parquet {e:,.1f}s, {r:,.0f} MB/s of source")
        if out:
            print(f"  {out/M:,.1f} MB out, {pb/out:,.1f}x smaller")
            print(f"  whole file: {mins(size,r):,.1f} min, ~{size/max(pb,1)*out/G:,.1f} GB parquet")
    sp.set_scan_threads(0)
    return best


drives()
if not P:
    print("\n(pass a file path to measure)")
    raise SystemExit(0)
path = P[0]
if not os.path.isfile(path):
    raise SystemExit(f"not a file: {path}")
io = dec = 0
if A["only"] != "dec":
    io = io_probe(path)
if A["only"] != "io":
    dec = dec_probe(path)
if io and dec:
    print(f"\n  read {io:,.0f} MB/s vs decode {dec:,.0f} MB/s: bound by {'I/O' if io < dec else 'CPU'}")
