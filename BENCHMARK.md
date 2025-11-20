❯ cargo run --release --features hotpath --bin sas7bd -- convert --columnar --out /tmp/bench-ahs-col.parquet ahs2013n.sas7bdat
Compiling sas7bdat-parser-rs v0.1.0 (/Users/tobiaskragholm/dev/sas7bdat-parser-rs)
Finished `release` profile [optimized] target(s) in 21.23s
Running `target/release/sas7bd convert --columnar --out /tmp/bench-ahs-col.parquet ahs2013n.sas7bdat`
ahs2013n.sas7bdat -> /tmp/bench-ahs-col.parquet
[hotpath] timing - Execution duration of functions.
sas7bd: 8.67s
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| Function | Calls | Avg | P95 | Total | % Total |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| sas7bd | 1 | 8.67 s | 8.67 s | 8.67 s | 100.00% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| parquet::write_columnar_batch | 2 | 3.74 s | 6.58 s | 7.47 s | 86.15% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| stream_columnar::utf8_staged | 5148 | 374.75 µs | 1.82 ms | 1.93 s | 22.24% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| rows::next_columnar_batch_contiguous | 3 | 159.63 ms | 448.79 ms | 478.90 ms | 5.52% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| rows::fetch_next_page | 17512 | 17.58 µs | 33.73 µs | 307.92 ms | 3.55% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| parquet::finish | 1 | 101.49 ms | 101.52 ms | 101.49 ms | 1.17% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| parquet::begin | 1 | 1.45 ms | 1.45 ms | 1.45 ms | 0.01% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| parquet::new | 4041 | 117 ns | 83 ns | 475.00 µs | 0.00% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| parquet::into_inner | 1 | 0 ns | 1 ns | 0 ns | 0.00% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| rows::row_iterator | 1 | 173.54 µs | 173.57 µs | 173.54 µs | 0.00% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+
| rows::new | 3 | 82.40 µs | 172.67 µs | 247.21 µs | 0.00% |
+--------------------------------------+-------+-----------+-----------+-----------+---------+

…] is 📦 v0.1.0 via 🐍 v3.14.0 via 📐 v4.5.2 via 🦀 v1.93.0-nightly took 30s
❯ cargo run --release --bin sas7bd -- convert --columnar --out /tmp/bench-ahs-col.parquet ahs2013n.sas7bdat
Compiling sas7bdat-parser-rs v0.1.0 (/Users/tobiaskragholm/dev/sas7bdat-parser-rs)
Finished `release` profile [optimized] target(s) in 17.62s
Running `target/release/sas7bd convert --columnar --out /tmp/bench-ahs-col.parquet ahs2013n.sas7bdat`
ahs2013n.sas7bdat -> /tmp/bench-ahs-col.parquet

…] is 📦 v0.1.0 via 🐍 v3.14.0 via 📐 v4.5.2 via 🦀 v1.93.0-nightly took 26s
❯ hyperfine --warmup 1 --runs 3 --prepare 'rm -f /tmp/bench-ahs-col.parquet' './target/release/sas7bd convert --columnar --out /tmp/bench-ahs-col.parquet ahs2013n.sas7bdat'
Benchmark 1: ./target/release/sas7bd convert --columnar --out /tmp/bench-ahs-col.parquet ahs2013n.sas7bdat
Time (mean ± σ): 8.888 s ± 0.093 s [User: 7.372 s, System: 1.337 s]
Range (min … max): 8.794 s … 8.980 s 3 runs

## Hotpath profiling workflow

- Build with `--features hotpath` and point results at a directory, e.g.:
  - `cargo run --release --features hotpath --bin sas7bd -- --hotpath-out target/hotpath --hotpath-save json,csv --hotpath-tag ahs13n convert --columnar --out /tmp/bench-ahs-col.parquet ahs2013n.sas7bdat`
- When `--hotpath-out` (or `HOTPATH_OUT`) is set, profiling emits timestamped files per run:
  - `hotpath-sas7bd-YYYYMMDD-HHMMSS[-tag]-json.json`
  - `hotpath-sas7bd-YYYYMMDD-HHMMSS[-tag]-csv.csv`
- CLI/env knobs (all optional, hotpath feature only):
  - `--hotpath-save` / `HOTPATH_SAVE` (comma separated) to pick formats (`json`, `json-pretty`, `csv`; defaults to `json,csv` when an output dir is set)
  - `--hotpath-stdout` / `HOTPATH_STDOUT` to change terminal output (`table`/`json`/`json-pretty`/`csv`; defaults to `table`)
  - `--hotpath-tag` / `HOTPATH_TAG` to stamp filenames per dataset/change
- `BENCH_HOTPATH=1 benchmarks/run_rust.sh <file>` now saves JSON + CSV under `target/hotpath` (tagged with the input file name) while still printing the table to stdout.
