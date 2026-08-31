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
- Column projection to skip unwanted columns
- Row range selection
- `WINDOWS-1252` and UTF-8 encoding support
- Companion `.sas7bcat` catalog support for hydrating value labels
- Deleted rows are recognised and excluded, in both the uncompressed and the compressed
  representation (see [Related work](#related-work): released ReadStat does not do this)
- Every scan reports which decode pipeline it took, and can be asked before it runs

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
sas7bdat = "0.9"

# With Arrow output support:
sas7bdat = { version = "0.9", features = ["arrow"] }
```

### Stream decoded rows

`scan()` returns a `ScanBuilder`; `visit_rows` calls your closure with a borrowed
`RowView` per row without allocating row-by-row.

```rust
use sas7bdat::{Dataset, Result};
use std::ops::ControlFlow;

fn main() -> Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    println!("{} rows × {} columns", ds.metadata().row_count, ds.columns().len());

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

### Column projection

```rust
use sas7bdat::Dataset;

let ds = Dataset::open("data.sas7bdat")?;
let projection = ds.projection().column("col_a").column("col_b").build()?;
ds.scan().with_projection(&projection).visit_rows(|row| {
    println!("{row:?}");
    Ok(std::ops::ControlFlow::Continue(()))
})?;
```

## Workspace

This repository is a Cargo workspace:

| Crate | Description |
|---|---|
| `sas7bdat` (`crates/sas7bdat/`) | Core parser library (published to crates.io) |
| `sas7bdat-cli` (`crates/sas7bdat-cli/`) | The `sas7bdat` CLI (`convert`/`info`/`head`/`completions`), plus compatibility and profiling binaries |
| `sas7bdat-convert` (`crates/sas7bdat-convert/`) | Conversion driver behind the CLI: Parquet, CSV and TSV sinks (published to crates.io) |
| `sas7bdat-polars` (`crates/polars-plugin/`) | Polars IO plugin (Python wheel via maturin) |
| `fastsas` (`crates/r-plugin/`) | R binding: `read_sas()` into a data frame, via extendr |
| `fastsasconvert` (`crates/r-convert-plugin/`) | R binding for the conversion driver |

The two R packages sit outside the Cargo workspace and depend on `sas7bdat` **by version**
rather than by path, so a tarball built with `R CMD build` resolves it from crates.io.
`crates/*/src/.cargo/config.toml` patches that back to this checkout for local work, and
`scripts/check-versions.sh` guards the two from drifting apart.

## CLI tools

The `sas7bdat-cli` member provides one user-facing tool, `sas7bdat`. Install it as a
prebuilt binary from PyPI (a plain `py3-none-<platform>` wheel — no Python version
floor, no dependencies, Python is only the delivery mechanism):

```sh
pip install sas7bdat-cli          # or: uv tool install sas7bdat-cli
sas7bdat info data.sas7bdat
```

`pip install "sas7bdat-polars[cli]"` installs the Polars plugin together with this CLI.

From a clone, the same tool runs through cargo with subcommands:

```sh
# Show metadata, columns, and a small sample
cargo run -p sas7bdat-cli --bin sas7bdat -- info data.sas7bdat

# Preview the first rows as a table
cargo run -p sas7bdat-cli --bin sas7bdat -- head data.sas7bdat -n 20

# Convert (output name + format inferred — this writes data.parquet, Zstd-compressed)
cargo run -p sas7bdat-cli --bin sas7bdat -- convert data.sas7bdat

# Pick a different Parquet codec (zstd | lz4 | snappy | none)
cargo run -p sas7bdat-cli --bin sas7bdat -- convert data.sas7bdat --compression lz4

# Convert to CSV (format inferred from the .csv extension)
cargo run -p sas7bdat-cli --bin sas7bdat -- convert data.sas7bdat --out out.csv

# Generate a shell completion script
cargo run -p sas7bdat-cli --bin sas7bdat -- completions zsh
```

The standalone `sas7bdat-convert` and `sas7bdat-inspect` binaries remain as backward-compatible
aliases. Every user-facing binary is in the crate's `default` features (`cli`, `aliases`,
`dir-mapper`), so a plain `cargo build`/`cargo install` gets them all; the gates exist only so
each Python wheel can select a single binary (maturin packages *every* binary cargo produced —
it has no `--bin` filter). Developer/profiling tools (`sas7bdat-corpus-profile`, ...) build only
with `--features dev-tools`:

```sh
cargo run -p sas7bdat-cli --features dev-tools --bin sas7bdat-corpus-profile -- /path/to/sas/files --format csv --out profile.csv
```

## Polars plugin

`sas7bdat-polars` (`crates/polars-plugin/`) is a [Polars](https://pola.rs/) IO plugin built
as a PyO3 extension module. It is **not** published to crates.io — it ships as a Python
wheel via [maturin](https://github.com/PyO3/maturin) (package name `sas7bdat-polars`):

```sh
cd crates/polars-plugin
maturin develop --release        # build + install into the active venv
# or
maturin build --release          # produce a wheel under target/wheels/
```

> Note: there are three `pyproject.toml` files in this repo, each building a different
> distribution from the same workspace — `crates/polars-plugin/` builds this Polars
> extension, `crates/sas7bdat-cli/` builds the `sas7bdat-cli` binary wheel, and the root
> one builds the `sas7bdat-dir-mapper` binary wheel. Because maturin packages every binary
> the cargo build produced, the last two pick their single binary with
> `no-default-features` plus one feature (see `crates/sas7bdat-cli/Cargo.toml`).

## Building

Builds on stable. The pinned toolchain is in `rust-toolchain.toml`, at an exact version
rather than floating on `stable`, so a new release's clippy lints turn CI red when someone
chooses to bump it rather than on the Thursday Rust ships. Only the opt-in `nightly-simd`
feature, which swaps the default `fearless_simd` backend for `std::simd`, needs a nightly
toolchain.

```sh
cargo build --release
```

A bare `cargo build` only builds the core library and CLI (`default-members`). The
`sas7bdat-polars` plugin links against libpython, so it is excluded from the default build;
build it explicitly with `cargo build -p sas7bdat-polars` or via maturin (see above).

To build the core crate with Arrow support:

```sh
cargo build --release -p sas7bdat --features arrow
```

## Testing

```sh
# Core library tests
cargo nextest run --release -p sas7bdat

# Or with the standard test runner
cargo test -p sas7bdat
```

With [`just`](https://github.com/casey/just):

```sh
just doctor        # what is missing on this machine, and the command that fixes each
just test-core     # the Rust core and the CLI
just test          # the above, plus the Polars plugin (Rust and Python)
just test-all      # the above, plus both R suites
```

`test-all` is separate from `test` because `just test-r` installs `fastsas` and
`fastsasconvert` into your user R library, which is the only way to run their suites.
A command that reads like it only runs tests should not change the machine.

**Start with `just doctor`.** The suites depend on things a fresh clone does not have:
a Python venv the plugin tests build into, the binary fixture corpus, and the two R
packages installed into your R library. `doctor` names each gap and the recipe that
closes it, rather than letting you find out through a maturin error that points
somewhere else. `just setup-python` and `just test-r` create what they need.

A missing corpus file makes a suite *smaller*, never silently green: the Polars tests
skip with a banner naming the file and the count.

Run `just install-hooks` once. It installs a `commit-msg` check that rejects a subject
[git-cliff](https://git-cliff.org) would drop from the changelog, which is otherwise
silent: `cliff.toml` sets `filter_unconventional = true`, and a commit that does not
match a `commit_parsers` entry simply never appears in `CHANGELOG.md`.

Correctness is checked with golden-parity tests that compare decoded output row-by-row
against reference CSV snapshots, plus a broad fixture smoke suite spanning compression
modes, encodings, and date/time types. Test fixtures are drawn in part from the
[pandas](https://github.com/pandas-dev/pandas) SAS test corpus (BSD-3-Clause); the large
binary corpus is kept out of git (see `fixtures/README.md`).

## Releasing

```sh
just release sas7bdat 0.10.0                      # print the plan; changes nothing
just release sas7bdat 0.10.0 --execute            # the local work, and push main
just release sas7bdat 0.10.0 --execute --publish  # also push tags, which uploads
```

A release here is never one crate and never one step. `sas7bdat-convert` depends on the
core by version as well as by path, so bumping the core across a `0.x` boundary forces a
breaking release of the convert crate too; both R bindings' requirements move with them;
and `inst/AUTHORS`, which CI gates on, cannot be regenerated until the crates are
actually on crates.io, because `vendor-r-package.sh` strips the local `[patch]` exactly
as `R CMD build` does.

`just release` works all of that out from the manifests, prints the ordered plan with
what is already done, and names what each remaining step waits for. It is resumable:
every step decides for itself whether it has happened, so recovering from an
interruption is just running it again.

## Performance

Throughput on a representative production corpus (mix of compressed and uncompressed files):

- Uncompressed: ~9.7M rows/s
- Compressed (RLE/CHAR): ~3.3M rows/s

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

**Where this reader deliberately disagrees with them:** deleted rows. SAS tombstones a
deleted row rather than removing it, and ReadStat 1.1.9 — which is what `haven` and
`pyreadstat` ship — returns those rows as data. This reader excludes them, so its row
counts are lower than theirs on affected files. ReadStat built after
[#366](https://github.com/WizardMac/ReadStat/pull/366) agrees with this reader; that is the
build used as the oracle here. A handful of small test fixtures
originate from the pyreadstat and Parso test corpora (Apache-2.0) — see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## License

MIT — see [LICENSE](LICENSE). Third-party test-fixture attribution is listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

SAS® and SAS7BDAT are trademarks of SAS Institute Inc. This project is an independent
open-source effort and is not affiliated with, sponsored, or endorsed by SAS Institute
Inc.; the names are used solely to describe file-format compatibility.
