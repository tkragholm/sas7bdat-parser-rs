## Benchmark Harnesses

This directory contains helper programs for comparing different `sas7bdat`
readers.

> **Note:** some harnesses depend on vendored reference implementations that are
> committed as a Git submodule. After cloning the repository, run
> `git submodule update --init --recursive` to populate `benchmarks/lib/c/`
> (ReadStat) before building the C benchmark.

### C# (`SasBenchmarks`)

The `SasBenchmarks` project wraps the vendored `Sas7Bdat.Core` sources under
`benchmarks/lib/csharp` and streams a dataset while timing the read.

Build prerequisites:

* .NET SDK 9.0 (or later)

Usage:

```bash
benchmarks/runners/run_csharp.sh tests/data_AHS2013/omov.sas7bdat
```

`run_csharp.sh` restores any framework dependencies, builds the local library if
needed, and then runs the benchmark. The program reports total rows, column
count, and elapsed time in milliseconds.

### Additional Benchmarks

Place other benchmark harnesses (Rust, ReadStat CLI wrappers, etc.) in this
directory alongside `SasBenchmarks` for easy comparison.

### Build All Harnesses

Use the build helper to compile every benchmark harness in one go:

```bash
benchmarks/suites/build_all.py
```

Pass a fixture to drive build-only scripts and `--allow-fail` to continue on
missing toolchains:

```bash
benchmarks/suites/build_all.py --fixture fixtures/raw_data/pandas/airline.sas7bdat --allow-fail
```

### Rust CLI (`sas7 convert`)

Use the primary CLI to exercise the same pipelines exposed to end users. The
`convert` subcommand writes Parquet/CSV/TSV files. Parquet always uses the
streaming columnar decoder by default:

```bash
cargo run --release --bin sas7 -- convert tests/data_AHS2013/omov.sas7bdat \
  --out /tmp/out.parquet
```

The `benchmarks/runners/run_rust.sh` helper wraps `sas7 convert`, rebuilds incrementally,
and writes to a temporary output:

```bash
benchmarks/runners/run_rust.sh tests/data_AHS2013/omov.sas7bdat
```

### Rust Bench Harness (`sas7bdat-rustbench`)

The Rust bench harness reads every row with no output side effects, mirroring
the C/C++/C# benchmarks:

```bash
benchmarks/runners/run_rust_bench.sh tests/data_AHS2013/omov.sas7bdat
```

### ReadStat Library (C)

`run_readstat.sh` compiles the vendored ReadStat sources under
`benchmarks/lib/c/` alongside `benchmarks/readstat/readstat_bench.c`, producing a self-contained
binary in `benchmarks/.build/`. No system-wide `libreadstat` installation is
required. The resulting benchmark streams every value in the file:

```bash
benchmarks/runners/run_readstat.sh tests/data_AHS2013/omov.sas7bdat
```

If you want the ReadStat CLI from the submodule for correctness tests:

```bash
benchmarks/runners/run_readstat_cli.sh --build-only
export SAS7BDAT_READSTAT_BIN="$(benchmarks/runners/run_readstat_cli.sh --print-path)"
```

### C++ (`cppsas7bdat`)

The C++ benchmark builds the `cppsas7bdat` reader from the sources in
`benchmarks/lib/cpp/` and measures streaming throughput using a lightweight sink
that touches every cell.

Build prerequisites:

* CMake 3.16+
* A C++20 compiler (e.g. `g++` 10+ or `clang++` 12+)
* Development packages for `fmt`, `spdlog`, and Boost date\_time (Debian/Ubuntu:
  `libfmt-dev`, `libspdlog-dev`, `libboost-date-time-dev`)

If CMake cannot locate Boost, set one of `Boost_DIR`, `BOOST_ROOT`, or
`CMAKE_PREFIX_PATH` to the directory containing `BoostConfig.cmake` (or the
Boost install root).

Usage:

```bash
benchmarks/runners/run_cpp.sh tests/data_AHS2013/omov.sas7bdat
```

The script configures a local build directory under `benchmarks/.build/`,
rebuilds when sources change, and runs the resulting `cpp_bench` executable.

### Hyperfine Setup

After building the necessary binaries once (Rust `cargo run --release`,
`dotnet build` inside `SasBenchmarks`, C `run_readstat.sh`, C++
`run_cpp.sh`), you can compare all readers with
[`hyperfine`](https://github.com/sharkdp/hyperfine). Example:

```bash
hyperfine \
  'benchmarks/runners/run_rust_bench.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/runners/run_csharp.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/runners/run_readstat.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/runners/run_cpp.sh tests/data_AHS2013/omov.sas7bdat'
```

Replace the input path with the dataset you want to benchmark. Each command
should emit summary statistics (rows, elapsed ms) in addition to Hyperfine’s
timing output.

The `run_hyperfine.sh` helper executes the same sequence for a single dataset
and accepts additional Hyperfine arguments:

```bash
benchmarks/runners/run_hyperfine.sh tests/data_AHS2013/omov.sas7bdat --warmup 1
```

### Suite Runner

The suite runner executes multiple parsers over all fixtures, records timings,
and emits an optional JSON report:

```bash
benchmarks/suites/bench_suite.py --output benchmarks/report.json
```

Use `--pattern` to filter fixtures and `--parsers` to select a subset:

```bash
benchmarks/suites/bench_suite.py --pattern ahs2013 --parsers rust readstat cpp csharp
```
