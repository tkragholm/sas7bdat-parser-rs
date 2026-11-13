# sas7bdat

A reader for SAS7BDAT files, the binary dataset format written by SAS. It
decodes pages in parallel, handles strings with vectorised routines, and can
hand the result to Apache Arrow.

[![Crates.io](https://img.shields.io/crates/v/sas7bdat.svg)](https://crates.io/crates/sas7bdat)
[![docs.rs](https://docs.rs/sas7bdat/badge.svg)](https://docs.rs/sas7bdat)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/tkragholm/sas7bdat-parser-rs/blob/main/LICENSE)

```toml
[dependencies]
sas7bdat = "0.10"
# or, with Arrow record batches:
sas7bdat = { version = "0.10", features = ["arrow"] }
```

```rust
use sas7bdat::{Dataset, Result};
use std::ops::ControlFlow;

fn main() -> Result<()> {
    let ds = Dataset::open("data.sas7bdat")?;
    println!("{} rows, {} columns", ds.metadata().row_count, ds.columns().len());

    ds.scan().visit_rows(|row| {
        println!("{row:?}");
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}
```

It reads RLE and RDC compressed files, 32- and 64-bit files of either
endianness, the text encodings known to `encoding_rs`, value labels from
`.sas7bcat` catalogs, and excludes rows that SAS has marked as deleted. It
builds on stable Rust.

The same reader is available as a Polars plugin for Python
(`pip install sas7bdat-polars`), as an R package, and as a command-line tool
(`pip install sas7bdat-cli`). See the
[repository](https://github.com/tkragholm/sas7bdat-parser-rs) for those,
for benchmarks, and for how correctness is checked.

MIT licensed. SAS and SAS7BDAT are trademarks of SAS Institute Inc.; this
project is not affiliated with or endorsed by SAS Institute Inc.
