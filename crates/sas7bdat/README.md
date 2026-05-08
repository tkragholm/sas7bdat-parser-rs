# sas7bdat

A fast, SIMD-accelerated SAS7BDAT file parser for Rust with optional [Apache Arrow](https://arrow.apache.org/) batch output.

[![Crates.io](https://img.shields.io/crates/v/sas7bdat.svg)](https://crates.io/crates/sas7bdat)
[![docs.rs](https://docs.rs/sas7bdat/badge.svg)](https://docs.rs/sas7bdat)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Features

- Zero-copy memory-mapped I/O via `memmap2`
- SIMD-accelerated string decoding (`simdutf8`)
- Parallel page scanning with `rayon`
- RLE and CHAR compression support
- Optional Arrow batch output (feature-gated)
- Column projection to skip unwanted columns
- Row range selection
- `WINDOWS-1252` and UTF-8 encoding support

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
sas7bdat = "0.3"

# With Arrow output support:
sas7bdat = { version = "0.3", features = ["arrow"] }
```

### Basic row-by-row scan

```rust
use sas7bdat::{Dataset, OpenOptions};

fn main() -> sas7bdat::Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    println!("{} rows × {} columns", ds.metadata().row_count, ds.metadata().columns.len());

    let mut scan = ds.scan()?;
    while let Some(row) = scan.next_row()? {
        for cell in row {
            print!("{cell:?}  ");
        }
        println!();
    }
    Ok(())
}
```

### Arrow batch output

```rust
use sas7bdat::{Dataset, BatchHint};

fn main() -> sas7bdat::Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    let mut scan = ds.scan_columnar(BatchHint::rows(4096))?;
    while let Some(batch) = scan.next_batch()? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

### Column projection

```rust
use sas7bdat::{Dataset, Projection};

let ds = Dataset::open("data.sas7bdat")?;
let projection = Projection::by_name(&["col_a", "col_b"])?;
let mut scan = ds.scan_with_options(Default::default(), projection)?;
```

## Workspace

This repository is a Cargo workspace:

| Crate | Description |
|---|---|
| `sas7bdat` (`crates/sas7bdat/`) | Core parser library (published to crates.io) |
| `sas7bdat-cli` (`crates/sas7bdat-cli/`) | CLI tools: `sas7bdat-convert`, `sas7bdat-inspect`, and profiling binaries |
| `sas7bdat-polars` (`crates/polars_plugin/`) | Polars IO plugin (Python wheel via maturin) |

## CLI tools

The `sas7bdat-cli` workspace member provides several binaries for working with SAS files:

```sh
# Convert to Parquet
cargo run -p sas7bdat-cli --bin sas7bdat-convert -- data.sas7bdat --sink parquet --out out.parquet

# Inspect metadata
cargo run -p sas7bdat-cli --bin sas7bdat-inspect -- data.sas7bdat --json

# Profile a directory of SAS files
cargo run -p sas7bdat-cli --bin sas7bdat-corpus-profile -- /path/to/sas/files --format csv --out profile.csv
```

## Building

Requires Rust nightly (see `rust-toolchain.toml`).

```sh
cargo build --release
```

To build with Arrow support:

```sh
cargo build --release -p sas7bdat --features arrow
```

## Testing

```sh
# Core library tests
cargo nextest run --release -p sas7bdat

# Or with standard test runner
cargo test -p sas7bdat
```

With [`just`](https://github.com/casey/just):

```sh
just test-core
just test
```

## Performance

Throughput on a representative production corpus (mix of compressed and uncompressed files):

- Uncompressed: ~9.7M rows/s
- Compressed (RLE/CHAR): ~3.3M rows/s

## License

MIT — see [LICENSE](LICENSE).
