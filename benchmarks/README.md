## Benchmark Harnesses

This directory contains helper programs for comparing different `sas7bdat`
readers.

### C# (`SasBenchmarks`)

The `SasBenchmarks` project wraps the [`Sas7Bdat`](https://www.nuget.org/packages/Sas7Bdat)
library and streams a dataset while timing the read.

Build prerequisites:

* .NET SDK 9.0 (or later)
* `Sas7Bdat` NuGet package. With restricted network access, copy the package
  `.nupkg` and any dependencies into `benchmarks/nuget-packages/`. The supplied
  `NuGet.Config` points the restore process at this local feed.

Usage:

```bash
benchmarks/run_csharp.sh tests/data_AHS2013/omov.sas7bdat
```

`run_csharp.sh` restores packages using the local feed defined in
`NuGet.Config`, primes a self-contained cache under `benchmarks/.nuget/`, builds
the harness when necessary, and then runs the benchmark. The program reports
total rows, column count, and elapsed time in milliseconds.

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

`run_readstat.sh` compiles the bundled ReadStat sources under `read-stat-src/`
alongside `readstat_bench.c`, producing a self-contained binary in
`benchmarks/.build/`. No system-wide `libreadstat` installation is required.
The resulting benchmark streams every value in the file:

```bash
benchmarks/run_readstat.sh tests/data_AHS2013/omov.sas7bdat
```

### Hyperfine Setup

After building the necessary binaries once (Rust `cargo run --release`,
`dotnet build` inside `SasBenchmarks`), you can compare all three readers with
[`hyperfine`](https://github.com/sharkdp/hyperfine). Example:

```bash
hyperfine \
  'benchmarks/run_rust.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/run_csharp.sh tests/data_AHS2013/omov.sas7bdat' \
  'benchmarks/run_readstat.sh tests/data_AHS2013/omov.sas7bdat'
```

Replace the input path with the dataset you want to benchmark. Each command
should emit summary statistics (rows, elapsed ms) in addition to Hyperfine’s
timing output.

The `run_hyperfine.sh` helper executes the same sequence for a single dataset
and accepts additional Hyperfine arguments:

```bash
benchmarks/run_hyperfine.sh tests/data_AHS2013/omov.sas7bdat --warmup 1
```
