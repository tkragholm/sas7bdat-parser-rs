# sas7bdat

A fast, SIMD-accelerated SAS7BDAT file parser for Rust with optional [Apache Arrow](https://arrow.apache.org/) batch output.

[![Crates.io](https://img.shields.io/crates/v/sas7bdat.svg)](https://crates.io/crates/sas7bdat)
[![docs.rs](https://docs.rs/sas7bdat/badge.svg)](https://docs.rs/sas7bdat)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`sas7bdat` decodes SAS7BDAT datasets — the binary on-disk format produced by SAS —
and streams them into modern Rust data pipelines. It was originally built for heavy,
secure processing of large national registers on Statistics Denmark's servers, and is
designed to bring a legacy, closed-source format into open-source workflows.

Most of the ecosystem reads SAS files through the venerable C library
[ReadStat](https://github.com/WizardMac/ReadStat) (used by R's `haven` and Python's
`pyreadstat`). Implementing the reader in Rust preserves that performance while making
the code easier to contribute to and far simpler to redistribute — cross-compiled
binaries and Python wheels instead of a C build toolchain.

## Features

- Zero-copy memory-mapped I/O via `memmap2`
- SIMD-accelerated string decoding (`std::simd` + `simdutf8`)
- Parallel page scanning with `rayon`
- RLE and CHAR compression support
- Optional Arrow batch output (feature-gated)
- Optional string-column dictionary encoding (`dictionary` feature): an HLL
  cardinality gate + byte-direct/`lasso2` interner that powers `Categorical`
  (Polars) and `factor` (R) output — see [`benchmarks/categorical-encoding.md`](../../benchmarks/categorical-encoding.md)
- Column projection to skip unwanted columns
- Row range selection
- `WINDOWS-1252` and UTF-8 encoding support
- Companion `.sas7bcat` catalog support for hydrating value labels

## Requirements

This crate uses portable SIMD (`#![feature(portable_simd)]`) and therefore requires a
**nightly** Rust toolchain. A pinned toolchain is provided in `rust-toolchain.toml`.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
sas7bdat = "0.3"

# With Arrow output support:
sas7bdat = { version = "0.3", features = ["arrow"] }
```

### Open a dataset and inspect metadata

```rust
use sas7bdat::{Dataset, Result};

fn main() -> Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    println!("{} rows × {} columns", ds.metadata().row_count, ds.columns().len());
    Ok(())
}
```

### Stream decoded rows

`scan()` returns a `ScanBuilder`; `visit_rows` calls your closure with a borrowed
`RowView` per row without allocating row-by-row. Return `ControlFlow::Break(())` to
stop early.

```rust
use sas7bdat::{Dataset, Result};
use std::ops::ControlFlow;

fn main() -> Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    ds.scan().visit_rows(|row| {
        println!("{row:?}");
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}
```

Prefer owned values? `let rows = ds.collect_rows()?;` returns a `Vec<OwnedRow>`.

### Arrow batch output

Requires the `arrow` feature.

```rust
use sas7bdat::{Dataset, Result};
use std::ops::ControlFlow;

fn main() -> Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    ds.scan().visit_arrow_batches(|batch| {
        println!("{} rows", batch.num_rows());
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}
```

Or collect them all at once with `ds.scan().collect_arrow_batches()?`.

### Column projection

Build a `Projection` from the dataset and attach it to a scan to decode only the
columns you need:

```rust
use sas7bdat::{Dataset, Result};
use std::ops::ControlFlow;

fn main() -> Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    let projection = ds.projection().column("col_a").column("col_b").build()?;
    ds.scan().with_projection(&projection).visit_rows(|row| {
        println!("{row:?}");
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}
```

The shortcut `ds.rows_with_projection(&["col_a", "col_b"], |row| { .. })?` does the same
in one call.

## Validation

Correctness is checked with golden-parity tests that compare decoded output row-by-row
against reference CSV snapshots, plus a broad fixture smoke suite spanning compression
modes, encodings, and date/time types. Test fixtures are drawn in part from the
[pandas](https://github.com/pandas-dev/pandas) SAS test corpus (BSD-3-Clause).

## Related work

This parser is an independent Rust implementation. The following projects were used as
references and prior art for the (reverse-engineered, undocumented) SAS7BDAT format:

- **ReadStat (C)** — battle-tested reference library behind `haven` and `pyreadstat` ([WizardMac/ReadStat](https://github.com/WizardMac/ReadStat)).
- **cppsas7bdat (C++)** — C++ reader used for comparison ([olivia76/cpp-sas7bdat](https://github.com/olivia76/cpp-sas7bdat)).
- **Sas7Bdat.Core (C#)** — .NET reader ([richokelly/Sas7Bdat](https://github.com/richokelly/Sas7Bdat)).
- **pandas (Python)** — pandas' built-in SAS reader, independent of ReadStat ([pandas-dev/pandas](https://github.com/pandas-dev/pandas/blob/main/pandas/io/sas/sas7bdat.py)).
- **Parso (Java)** — EPAM's independent Java reader ([epam/parso](https://github.com/epam/parso)).
- **"SAS7BDAT Database Binary Format"** — Matt Shotwell's foundational reverse-engineering
  of the format ([vignette PDF](https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf),
  [BioStatMatt/sas7bdat](https://github.com/BioStatMatt/sas7bdat)).

No code from these projects is included; outputs are validated against ReadStat,
`pyreadstat`, and `haven` used as external oracles.

## Performance

Throughput on a representative production corpus (mix of compressed and uncompressed files):

- Uncompressed: ~9.7M rows/s
- Compressed (RLE/CHAR): ~3.3M rows/s

## License

MIT — see [LICENSE](LICENSE).

SAS® and SAS7BDAT are trademarks of SAS Institute Inc. This project is an independent
open-source effort and is not affiliated with, sponsored, or endorsed by SAS Institute
Inc.; the names are used solely to describe file-format compatibility.
