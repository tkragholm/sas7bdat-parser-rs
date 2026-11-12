# SAS7BDAT reader benchmarks

Comparing this project's three readers (the Rust core, the Polars plugin
`sas7bdat_polars`, and the R binding `fastsas`) against the established
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

- **Threads:** the Rust core, the Polars plugin, and `fastsas` decode across all
  12 cores. readstat / haven / pyreadstat / pandas are single-threaded (ReadStat
  is a single-threaded C library).

All readers agreed on row counts on every file (correctness cross-check).

## Table A: in-memory read (file → table), throughput in MB/s (higher is better)

| Reader | Threads | AP (99 MB) | NYYTS (193 MB) | AHS (2.15 GB) |
|---|---:|---:|---:|---:|
| **rust-core** (ours) | 12 | **3436** | **3555** | **3232** |
| **sas7bdat-polars** (ours) | 12 | 2308 | 2377 | 1816 |
| **fastsas** (ours, R) | 12 | 739 | 1443 | 482 |
| rust-core, serial | 1 | 572 | 1047 | 1201 |
| pandas | 1 | 258 | 216 | 429 |
| pyreadstat | 1 | 90 | 128 | 103 |
| haven (R) | 1 | 47 | 60 | 32 |

Same data as wall-clock **min time (s)**, lower is better:

| Reader | AP | NYYTS | AHS |
|---|---:|---:|---:|
| rust-core (12t) | 0.029 | 0.054 | 0.67 |
| sas7bdat-polars (12t) | 0.043 | 0.081 | 1.19 |
| fastsas (12t) | 0.134 | 0.134 | 4.47 |
| rust-core serial (1t) | 0.173 | 0.185 | 1.79 |
| pandas (1t) | 0.384 | 0.895 | 5.02 |
| pyreadstat (1t) | 1.098 | 1.516 | 20.95 |
| haven (1t) | 2.101 | 3.202 | 66.83 |

## Table B: convert to CSV with the command-line tools

Both tools read the SAS file and write a CSV (so this includes CSV encoding,
unlike Table A). `hyperfine`, 3 runs after warmup. `sas7bdat-convert` decodes in
parallel; readstat is single-threaded, but its user CPU time is also far
higher, so it does more total work, not just less parallel work.

| File | readstat | sas7bdat-convert (ours) | speedup |
|---|---:|---:|---:|
| AP_VOTECAST (99 MB) | 3.05 s | **0.61 s** | **5.0×** |
| NYYTS (193 MB) | 3.91 s | **1.03 s** | **3.8×** |

## Table C: categorical / factor encoding of string columns (opt-in)

For low-cardinality character columns, the bindings can dictionary-encode
(`read_sas(categorical=TRUE)` → `factor`; `scan_sas(categorical=True)` →
`Categorical`). Measured on the 2.15 GB AHS file (2,574 character columns):

| | read | memory | group-by (4 cols) |
|---|---|---|---|
| **R `factor`** vs `character` | **2.6× faster** | **~32% smaller** | ~11× faster |
| **Polars `Categorical`** vs `Utf8` | +0.57 s slower | larger | ~11× faster |

In R the factor is faster on all three measures. In Polars it only speeds up
later operations, because Polars' own `String` type is already compact.

## Findings

- The Rust core reads at about 3.2 to 3.6 GB/s on 12 threads and 0.5 to 1.2 GB/s
  on one. Against pyreadstat, which does the same job through ReadStat, the
  parallel core is 28 to 38 times faster; against haven, 60 to 100 times.
- The Polars plugin reaches about 2.3 GB/s. The gap to the core is the cost of
  building Arrow arrays and crossing into Python.
- `fastsas` is 15 to 24 times faster than haven and faster than pandas on every
  file. It allocates each R vector at the known row count and fills it in place,
  interning each distinct string once per column.
- Converting to CSV, `sas7bdat-convert` is 3.8 to 5.0 times faster than the
  `readstat` command-line tool.

## Reproduce

```sh
# Rust core (release)
cargo build --release -p sas7bdat --example bench_read
./target/release/examples/bench_read <file.sas7bdat> 5 parallel   # or: serial

# Python (polars plugin / pyreadstat / pandas); needs a venv with the plugin built
python benchmarks/bench_py.py <file.sas7bdat> 5 polars,pyreadstat,pandas

# R (fastsas / haven)
Rscript benchmarks/bench_r.R <file.sas7bdat> 5 fastsas,haven

# CLI convert-to-CSV
hyperfine --warmup 1 --prepare 'rm -f /tmp/a.csv /tmp/b.csv' \
  "readstat <file> /tmp/a.csv" \
  "./target/release/sas7bdat-convert <file> --sink csv --out /tmp/b.csv --overwrite"
```
