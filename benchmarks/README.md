# SAS7BDAT reader benchmarks

This project's readers (the Rust core, the Polars plugin `sas7bdat_polars`, the
R package `fastsas`, and the `sas7bdat` command-line converter) against the
other open readers: the ReadStat C library, polars-readstat, pyreadstat,
pandas and haven, and the `readstat` command-line program for CSV output.

## Method

`run.py` runs every measurement as one fresh process that reads one file once
into the reader's own in-memory table, with the reader's defaults. The read is
timed inside the process, so interpreter start-up and imports are left out; the
whole process's wall time and peak memory are recorded too. The file is read
before every measurement, so the page cache is warm even after a reader that
needed most of the memory, and the readers are interleaved in an order that
rotates between repetitions, so drift on the machine spreads over all of them.
Before each measurement it waits until other processes use less than half a
core. On Linux it also measures what other user-space processes used during
the measurement, and repeats a measurement during which they used more. Each
median is reported with a 95% bootstrap interval.

After the timed runs, each reader's result is compared with the Rust core's:
row and column counts, and per column the sum and missing count of a numeric
column or the total length of a string column after trailing spaces are
removed. Date and time columns are left out of the comparison.

Peak memory is recorded twice (see `measure.py`). Peak resident set counts the
pages of a memory-mapped file, which this project's readers use and the others
do not, so it overstates their memory. Peak physical footprint is the memory
the process itself holds, and is the fairer comparison: on macOS the kernel's
own figure, on Linux the anonymous and shared memory of the process tree,
sampled every 10 ms.

## Files

`fetch.py` downloads the files into `fixtures/benchmark/` and checks their
SHA-256 against `run.py`. All are US federal public-use files.

| Name | File | Size | Rows × columns |
|---|---|---:|---|
| `ahs2013n` | American Housing Survey 2013, national | 2.15 GB | 70,044 × 4,041 |
| `ahs2013_household` | American Housing Survey 2013, household | 613 MB | 70,044 × 2,167 |
| `meps2021_h233` | Medical Expenditure Panel Survey 2021, full-year (HC-233) | 338 MB | 28,336 × 1,488 |
| `ahs2013_person` | American Housing Survey 2013, person | 26 MB | 148,342 × 82 |
| `ahs2013_topical` | American Housing Survey 2013, topical | 14 MB | 84,355 × 114 |

`ahs2013_topical` is the only compressed file of the five. It has no known
public download, so `fetch.py` cannot fetch it, and it is left out of the
results below.

## Results

Run of 16 November 2026 at commit `440d8d5`: AMD Ryzen 5 3600, 6 cores and 12
threads, 16 GB, Linux 7.2; ReadStat 1.1.9, polars-readstat 0.25.0, pyreadstat
1.3.7, pandas 3.1.1, Polars 2.1.0, haven 2.5.5, R 4.6.1, Rust 1.99.1. Ten
repetitions per reader and file, none of them disturbed by other processes.
All readers agreed with the Rust core on every file. The run's records,
including a 95% interval for every median, are in `results/2026-11-16/`.
ReadStat was built from its 1.1.9 release, whose version string still says
1.1.8; the string was corrected so the record names the release. An earlier
run on an Apple M3 Pro, on 13 November, is in `results/2026-11-13/`.

This project's readers and polars-readstat use their default thread counts.
ReadStat, pyreadstat, pandas and haven are single-threaded, and "pyreadstat,
12 processes" is `read_file_multiprocessing` with one process per thread.

Read time, median in seconds (lower is better):

| Reader | AHS national | AHS household | MEPS | AHS person |
|---|---:|---:|---:|---:|
| `sas7bdat` core | **1.174** | **0.501** | **0.122** | **0.035** |
| `sas7bdat` core, one thread | 4.549 | 2.040 | 0.287 | 0.187 |
| Polars plugin | 1.612 | 0.658 | 0.149 | 0.061 |
| polars-readstat | 1.760 | 0.874 | 0.244 | 0.064 |
| `fastsas` (R) | 4.710 | 2.357 | 0.610 | 0.583 |
| ReadStat C library | 32.486 | 12.374 | 1.626 | 0.686 |
| pandas | 22.417 | 10.185 | 0.382 | 1.368 |
| pyreadstat | 30.150 | 15.135 | 4.519 | 1.272 |
| pyreadstat, 12 processes | 14.482 | 6.338 | 1.227 | 0.589 |
| haven (R) | 53.239 | 25.663 | 4.979 | 2.705 |

Converting to CSV with the command-line tools, median in seconds:

| Tool | AHS national | AHS household | MEPS | AHS person |
|---|---:|---:|---:|---:|
| `sas7bdat` | **4.617** | **2.736** | **0.876** | **0.260** |
| `readstat` | 45.940 | 34.696 | 14.886 | 1.427 |

Peak physical footprint, MB:

| Reader | AHS national | AHS household | MEPS | AHS person |
|---|---:|---:|---:|---:|
| `sas7bdat` core | 3350 | 1642 | 405 | 100 |
| Polars plugin | 4618 | 2277 | 505 | 246 |
| polars-readstat | 4450 | 2185 | 625 | 330 |
| `fastsas` (R) | 2516 | 1512 | 555 | 181 |
| ReadStat C library | 9312 | 3961 | 360 | 483 |
| pandas | 3898 | 1899 | 402 | 295 |
| pyreadstat | 3218 | 1960 | 684 | 183 |
| haven (R) | 2360 | 1306 | 429 | 188 |
| `sas7bdat` to CSV | 935 | 1233 | 759 | 184 |

On one thread the core is 4 to 7 times faster than the ReadStat C library. The
Polars plugin is 1.05 to 1.6 times faster than polars-readstat, which by
default does not keep the file's row order. `fastsas` is 5 to 11 times faster
than haven, and on the national file peaks at 2.5 GB against 2.4 GB for haven.
pyreadstat's 12 processes cut its time by 52 to 73% on the three larger files.
Converting to CSV, `sas7bdat` is 5 to 17 times faster than `readstat`.

## Reproduce

```sh
python3 benchmarks/fetch.py
python3 -m venv benchmarks/.venv
benchmarks/.venv/bin/pip install polars polars-readstat pyreadstat pandas sas7bdat-polars
Rscript -e 'install.packages(c("haven", "fastsas"), repos = c("https://tkragholm.r-universe.dev", "https://cloud.r-project.org"))'
python3 benchmarks/run.py            # results in benchmarks/results/<date>/
```

`run.py` builds the Rust core's benchmark program and the CLI with cargo, and
compiles `readstat_read.c` against the installed ReadStat library. It needs
`Rscript` and `readstat` on PATH, or the environment variables `RSCRIPT`,
`READSTAT` and `READSTAT_PREFIX`. A full run takes about 50 minutes. Stopped
part way, it prints the command that continues it.
