# SAS7BDAT reader benchmarks

Comparing this project's three readers — the Rust core, the Polars plugin
(`sas7bdat_polars`), and the R binding (`readsas`) — against the established
tools: **readstat** (the C library + CLI behind everything else), **haven**
(R → tibble, wraps ReadStat), **pyreadstat** (Python → pandas, wraps ReadStat),
and **pandas** (`pd.read_sas`, an independent implementation).

## Setup

- **Machine:** Apple M3 Pro, 12 cores, 18 GB RAM, macOS 26.5.1
- **Versions:** this project @ `13332e8`; readstat 1.1.9; polars 1.40.1;
  pyreadstat 1.3.5; pandas 3.0.3; haven 2.5.5; R 4.6.0; Python 3.12
- **Method:** warm OS cache (one untimed warmup), then N timed iterations in
  one process (no interpreter-startup cost), reporting the **min** (most stable
  estimate). Each reader fully materializes the file into its in-memory table.
- **Files** (real public-use survey data, varied shape):

  | File | Size | Rows × Cols | Notes |
  |---|---:|---|---|
  | `AP_VOTECAST_2018_DATA` | 99 MB | 138,929 × 220 | AP VoteCast survey |
  | `NYYTS_2000_2020_PublicUse` | 193 MB | 121,730 × 514 | NY Youth Tobacco Survey |
  | `ahs2013n` | 2.15 GB | 70,044 × 4,041 | American Housing Survey (very wide) |

- **Threads:** the Rust core and the Polars plugin decode across all 12 cores.
  readstat / haven / pyreadstat / pandas are single-threaded (ReadStat is a
  single-threaded C library). `readsas` currently does **not** enable the core's
  parallelism (single-threaded decode + R marshalling) — see Findings.

All readers agreed on row counts on every file (correctness cross-check).

## Table A — in-memory read (file → table), throughput in MB/s (higher is better)

| Reader | Threads | AP (99 MB) | NYYTS (193 MB) | AHS (2.15 GB) |
|---|---:|---:|---:|---:|
| **rust-core** (ours) | 12 | **3436** | **3555** | **3232** |
| **sas7bdat-polars** (ours) | 12 | 2308 | 2377 | 1816 |
| rust-core, serial | 1 | 572 | 1047 | 1201 |
| **readsas** (ours, R) | 1 | 230 | 597 | 133 |
| pandas | 1 | 258 | 216 | 429 |
| pyreadstat | 1 | 90 | 128 | 103 |
| haven (R) | 1 | 47 | 60 | 32 |

Same data as wall-clock **min time (s)**, lower is better:

| Reader | AP | NYYTS | AHS |
|---|---:|---:|---:|
| rust-core (12t) | 0.029 | 0.054 | 0.67 |
| sas7bdat-polars (12t) | 0.043 | 0.081 | 1.19 |
| rust-core serial (1t) | 0.173 | 0.185 | 1.79 |
| readsas (1t) | 0.430 | 0.324 | 16.15 |
| pandas (1t) | 0.384 | 0.895 | 5.02 |
| pyreadstat (1t) | 1.098 | 1.516 | 20.95 |
| haven (1t) | 2.101 | 3.202 | 66.83 |

## Table B — CLI convert to CSV (file → CSV file), apples-to-apples

Both tools read the SAS file and write a CSV (so this includes CSV encoding,
unlike Table A). `hyperfine`, 3 runs after warmup. `sas7bdat-convert` decodes in
parallel; readstat is single-threaded — but note its *User* CPU time is also far
higher, so it does more total work, not just less parallel work.

| File | readstat | sas7bdat-convert (ours) | speedup |
|---|---:|---:|---:|
| AP_VOTECAST (99 MB) | 3.05 s | **0.61 s** | **5.0×** |
| NYYTS (193 MB) | 3.91 s | **1.03 s** | **3.8×** |

## Findings

- **The Rust core is the fastest reader** — ~3.2–3.6 GB/s, and even
  **single-threaded** (0.5–1.2 GB/s) it beats every reference tool. Against
  `pyreadstat` (same job, ReadStat C → pandas) the parallel core is **~28–38×**
  faster; against `haven` **~60–100×** (100× on the 2.15 GB file).
- **The Polars plugin** trails the raw core slightly (Arrow construction +
  Python boundary) but is still ~2.3 GB/s and **the fastest path that lands a
  usable DataFrame** in a host language.
- **`readsas` beats `haven`** — its direct R competitor — by **~5–9×** on the
  moderate files, despite being single-threaded, because the decode core is so
  much faster than ReadStat.
- **`readsas` regresses on the very wide table** (AHS, 4,041 cols → 16 s). The
  bottleneck is the R marshalling: every cell goes through an `Rfloat` box and a
  `Vec<Rfloat>` → REALSXP copy, ~283 M times. This is exactly what the deferred
  *direct-fill* optimization (write the REALSXP in place, skip the box) targets,
  plus enabling the core's parallelism for the decode. Two clear wins available.
- **CLI:** `sas7bdat-convert` is 3.8–5.0× faster than the `readstat` CLI at the
  same read-and-write-CSV job.

## Reproduce

```sh
# Rust core (release)
cargo build --release -p sas7bdat --example bench_read
./target/release/examples/bench_read <file.sas7bdat> 5 parallel   # or: serial

# Python (polars plugin / pyreadstat / pandas) — needs a venv with the plugin built
python benchmarks/bench_py.py <file.sas7bdat> 5 polars,pyreadstat,pandas

# R (readsas / haven)
Rscript benchmarks/bench_r.R <file.sas7bdat> 5 readsas,haven

# CLI convert-to-CSV
hyperfine --warmup 1 --prepare 'rm -f /tmp/a.csv /tmp/b.csv' \
  "readstat <file> /tmp/a.csv" \
  "./target/release/sas7bdat-convert <file> --sink csv --out /tmp/b.csv --overwrite"
```
