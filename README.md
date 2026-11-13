# sas7bdat

A reader for SAS7BDAT files, the binary dataset format written by SAS, in Rust,
with a Polars plugin for Python, an R package, and a command-line tool.

[![Crates.io](https://img.shields.io/crates/v/sas7bdat.svg)](https://crates.io/crates/sas7bdat)
[![docs.rs](https://docs.rs/sas7bdat/badge.svg)](https://docs.rs/sas7bdat)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The reader memory-maps the file, decodes data pages in parallel, and handles
strings with vectorised routines. On a 12-core laptop it reads a 2.15 GB survey
file in 0.67 s, against 21 s for pyreadstat and 67 s for haven; see
[benchmarks](benchmarks/README.md). It is used to read SAS register extracts on
Statistics Denmark's research servers.

## Why

National statistics and health-data agencies deliver research data as SAS7BDAT
files. Researchers who work in R or Python rather than SAS read those files
through a few open readers, most often ReadStat by way of haven and pyreadstat,
which decode on one thread and return values one at a time. This reader is for
that step when the files are large or many: it decodes pages in parallel, hands
back Arrow and Polars columns directly, and has the same core behind its R,
Python and command-line interfaces. [DESIGN.md](DESIGN.md) explains how it is
built and why.

It reads SAS7BDAT files and their `.sas7bcat` catalogs. It does not write files,
and it does not read SAS transport (XPORT), Stata or SPSS files; ReadStat and
pyreadstat cover those.

## Features

- RLE (`COMPRESS=CHAR`) and RDC (`COMPRESS=BINARY`) compression
- 32- and 64-bit files of either endianness, and tables with thousands of columns
- Text encodings known to `encoding_rs`; unrecognised labels are read as UTF-8
- Column projection and row ranges
- Rows streamed one at a time, or as Arrow record batches (`arrow` feature)
- Value labels from companion `.sas7bcat` catalogs
- Rows that SAS has marked as deleted are excluded

## Trying it

Any SAS7BDAT file will do. pandas' test suite has a small one (BSD-3-Clause):

```sh
curl -LO https://raw.githubusercontent.com/pandas-dev/pandas/v2.2.3/pandas/tests/io/sas/data/airline.sas7bdat
```

```sh
sas7bdat info airline.sas7bdat                                    # pip install sas7bdat-cli
python -c 'import sas7bdat_polars as sp; print(sp.read_sas("airline.sas7bdat"))'
```

The R packages ship example files, used in their help pages:

```r
fastsas::read_sas7bdat(system.file("extdata", "people.sas7bdat", package = "fastsas"))
```

In a clone of the repository, `cargo run -p sas7bdat --example read_file`
prints the columns and rows of the same file from Rust.

## Rust

```toml
[dependencies]
sas7bdat = { version = "0.10", features = ["arrow"] }
```

```rust
use sas7bdat::{Dataset, Result};
use std::ops::ControlFlow;

fn main() -> Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    let projection = ds.projection().column("age").column("sex").build()?;
    ds.scan().with_projection(&projection).visit_arrow_batches(|batch| {
        println!("{} rows", batch.num_rows());
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}
```

`ds.collect_rows()` returns owned rows instead; `ds.scan().visit_rows(...)`
streams borrowed ones.

## Python

```sh
pip install sas7bdat-polars
```

```python
import sas7bdat_polars as sp

df = sp.read_sas("data.sas7bdat", columns=["age", "sex"])
lf = sp.scan_sas("data.sas7bdat")          # lazy, with projection pushdown
```

See [crates/polars-plugin](crates/polars-plugin/README.md).

## R

The `fastsas` package returns haven-compatible tibbles. It is not on CRAN.
[R-universe](https://tkragholm.r-universe.dev) builds it as a binary package for
Windows, macOS and Linux, so no Rust toolchain is needed:

```r
install.packages(
  "fastsas",
  repos = c("https://tkragholm.r-universe.dev", "https://cloud.r-project.org")
)
```

```r
df <- fastsas::read_sas7bdat("data.sas7bdat")
```

See [crates/r-plugin](crates/r-plugin/README.md).

## Command line

```sh
pip install sas7bdat-cli
sas7bdat info data.sas7bdat               # columns, types, labels, a sample
sas7bdat head data.sas7bdat -n 20
sas7bdat convert data.sas7bdat            # writes data.parquet
sas7bdat convert data.sas7bdat --out data.csv
```

## Repository layout

| Crate | What it is |
|---|---|
| `crates/sas7bdat` | The reader (crates.io: `sas7bdat`) |
| `crates/sas7bdat-convert` | Parquet, CSV and TSV writers behind the CLI (crates.io) |
| `crates/sas7bdat-cli` | The `sas7bdat` command (PyPI: `sas7bdat-cli`) |
| `crates/polars-plugin` | Polars plugin (PyPI: `sas7bdat-polars`) |
| `crates/r-plugin` | R package `fastsas` |
| `crates/r-convert-plugin` | R package `fastsasconvert`, for conversion |

Building, testing, and releasing are described in
[CONTRIBUTING.md](CONTRIBUTING.md), and how the reader is built and why in
[DESIGN.md](DESIGN.md).

## Related work

The SAS7BDAT format is undocumented. This reader builds on the public
reverse-engineering work and on other open readers:

- Matt Shotwell's [SAS7BDAT Database Binary Format](https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf),
  the first public description of the format.
- [ReadStat](https://github.com/WizardMac/ReadStat) (C), the library behind
  haven and pyreadstat. The first prototype of this reader started from a
  c2rust translation of ReadStat's SAS reader; see
  [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
- [pandas](https://github.com/pandas-dev/pandas/blob/main/pandas/io/sas/sas7bdat.py)
  (Python), [Parso](https://github.com/epam/parso) (Java),
  [cppsas7bdat](https://github.com/olivia76/cpp-sas7bdat) (C++) and
  [Sas7Bdat.Core](https://github.com/richokelly/Sas7Bdat) (C#), other
  independent readers.
- [polars-readstat](https://github.com/jrothbaum/polars_readstat), another
  Polars plugin for SAS, Stata and SPSS files.

Decoded output is compared cell by cell with ReadStat built from its main
branch. On a local corpus of 329 public SAS files, 325 match on every cell.
ReadStat rejects three of the others, and one is left out because the two
readers map ISO-8859-1 differently.

The one deliberate difference is deleted rows. SAS marks a deleted row instead
of removing it. This reader excludes those rows, as ReadStat does since
[#366](https://github.com/WizardMac/ReadStat/pull/366). The ReadStat 1.1.9
release, which haven and pyreadstat ship, returns them, so those packages
report more rows on such files.

## License

MIT, see [LICENSE](LICENSE). Third-party test-fixture attribution is listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

SAS® and SAS7BDAT are trademarks of SAS Institute Inc. This project is an independent
open-source effort and is not affiliated with, sponsored, or endorsed by SAS Institute
Inc.; the names are used solely to describe file-format compatibility.
