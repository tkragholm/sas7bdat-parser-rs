## Benchmark Harnesses

This directory contains helper programs for comparing different `sas7bdat`
readers.

### C# (`SasBenchmarks`)

The `SasBenchmarks` project wraps the vendored `Sas7Bdat.Core` sources under
`benchmarks/lib/csharp` and streams a dataset while timing the read.

Build prerequisites:

* .NET SDK 9.0 (or later)

Usage:

```bash
benchmarks/run_csharp.sh tests/data_AHS2013/omov.sas7bdat
```

`run_csharp.sh` restores any framework dependencies, builds the local library if
needed, and then runs the benchmark. The program reports total rows, column
count, and elapsed time in milliseconds.

### Additional Benchmarks

Place other benchmark harnesses (Rust, ReadStat CLI wrappers, etc.) in this
directory alongside `SasBenchmarks` for easy comparison.

### Rust Library (`examples/benchmark.rs`)

The repository now includes an example program that iterates over all rows using
the core Rust crate:

```bash
cargo run --release --example benchmark -- tests/data_AHS2013/omov.sas7bdat
```

For convenience, use:

```bash
benchmarks/run_rust.sh tests/data_AHS2013/omov.sas7bdat
```

### ReadStat Library (C)

`run_readstat.sh` compiles the vendored ReadStat sources under
`benchmarks/lib/c/` alongside `readstat_bench.c`, producing a self-contained
binary in `benchmarks/.build/`. No system-wide `libreadstat` installation is
required. The resulting benchmark streams every value in the file:

```bash
benchmarks/run_readstat.sh tests/data_AHS2013/omov.sas7bdat
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

Usage:

```bash
benchmarks/run_cpp.sh tests/data_AHS2013/omov.sas7bdat
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
  'benchmarks/run_rust.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/run_csharp.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/run_readstat.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/run_cpp.sh tests/data_AHS2013/omov.sas7bdat'
```

Replace the input path with the dataset you want to benchmark. Each command
should emit summary statistics (rows, elapsed ms) in addition to Hyperfine’s
timing output.

The `run_hyperfine.sh` helper executes the same sequence for a single dataset
and accepts additional Hyperfine arguments:

```bash
benchmarks/run_hyperfine.sh tests/data_AHS2013/omov.sas7bdat --warmup 1
```
