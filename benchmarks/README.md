# SAS7BDAT reader benchmarks

This project's readers (the Rust core, the Polars plugin `sas7bdat_polars`, the
R package `fastsas`, and the `sas7bdat` command-line converter) against the
other open readers: the ReadStat C library, polars-readstat, pyreadstat,
pandas and haven, and the `readstat` command-line program for CSV output.

## Method

`run.py` runs every measurement as one fresh process that reads one file once
into the reader's own in-memory table, with the reader's defaults. The read is
timed inside the process, so interpreter start-up and imports are left out; the
whole process's wall time and peak memory are recorded too. Each file is read
twice before its runs so the page cache is warm, and the readers are
interleaved in an order that rotates between repetitions, so drift on the
machine spreads over all of them. Before starting, it waits until other
processes use less than half a core.

After the timed runs, each reader's result is compared with the Rust core's:
row and column counts, and per column the sum and missing count of a numeric
column or the total length of a string column after trailing spaces are
removed. Date and time columns are left out of the comparison.

Peak memory is recorded twice (see `measure.py`). Peak resident set counts the
pages of a memory-mapped file, which this project's readers use and the others
do not, so it overstates their memory. Peak physical footprint is the memory
the process itself holds, and is the fairer comparison on macOS.

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

Run of 13 November 2026 at commit `cad3e94`: Apple M3 Pro, 12 cores, 18 GB,
macOS 26.6; ReadStat 1.1.9, polars-readstat 0.25.0, pyreadstat 1.3.7, pandas
3.1.1, Polars 2.1.0, haven 2.5.5, R 4.6.2. Ten repetitions per reader and
file. All readers agreed with the Rust core on every file. The run's records
are in `results/2026-11-13/`.

This project's readers and polars-readstat use their default thread counts.
ReadStat, pyreadstat, pandas and haven are single-threaded, and "pyreadstat,
12 processes" is `read_file_multiprocessing` with one process per core.

Read time, median in seconds (lower is better):

| Reader | AHS national | AHS household | MEPS | AHS person |
|---|---:|---:|---:|---:|
| `sas7bdat` core | **0.474** | **0.233** | **0.058** | **0.013** |
| `sas7bdat` core, one thread | 1.527 | 0.706 | 0.085 | 0.077 |
| Polars plugin | 0.646 | 0.272 | 0.071 | 0.020 |
| polars-readstat | 0.822 | 0.381 | 0.113 | 0.027 |
| `fastsas` (R) | 2.742 | 1.342 | 0.266 | 0.310 |
| ReadStat C library | 5.074 | 2.328 | 0.545 | 0.229 |
| pandas | 13.123 | 5.366 | 0.209 | 0.725 |
| pyreadstat | 15.701 | 6.900 | 1.663 | 0.550 |
| pyreadstat, 12 processes | 9.172 | 3.341 | 0.956 | 0.587 |
| haven (R) | 64.686 | 28.566 | 2.976 | 3.406 |

Converting to CSV with the command-line tools, median in seconds:

| Tool | AHS national | AHS household | MEPS | AHS person |
|---|---:|---:|---:|---:|
| `sas7bdat` | **1.462** | **0.941** | **0.309** | **0.122** |
| `readstat` | 35.182 | 25.039 | 7.594 | 1.194 |

Peak physical footprint, MB:

| Reader | AHS national | AHS household | MEPS | AHS person |
|---|---:|---:|---:|---:|
| `sas7bdat` core | 4294 | 2480 | 860 | 145 |
| Polars plugin | 4072 | 2074 | 731 | 287 |
| polars-readstat | 4402 | 2138 | 531 | 260 |
| `fastsas` (R) | 3735 | 3331 | 1139 | 294 |
| ReadStat C library | 6470 | 2866 | 356 | 327 |
| pandas | 4008 | 1947 | 411 | 315 |
| pyreadstat | 3293 | 2004 | 746 | 207 |
| haven (R) | 2443 | 1358 | 452 | 206 |
| `sas7bdat` to CSV | 1376 | 1878 | 658 | 204 |

On one thread the core is 3 to 6 times faster than the ReadStat C library. The
Polars plugin is 1.3 to 1.6 times faster than polars-readstat, which by
default does not keep the file's row order. `fastsas` is 11 to 24 times faster
than haven, and uses more memory: on the national file 3.7 GB against 2.4 GB.

During this run other processes, mostly Spotlight indexing, used about one of
the twelve cores. The other projects' readers, at the same versions, were
within 3% of their times in a run on 12 November with an idle machine.

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
