# sas7bdat

`sas7bdat` is a Rust library for decoding SAS7BDAT datasets with a focus on reproducible research workflows. It exposes a safe API for inspecting metadata, streaming rows, and writing Parquet output so that legacy SAS exports can participate in modern data engineering pipelines. The project is Rust-first (library + CLI) with Python (PyO3) and R (extendr) bindings under active development. It was originally built for heavy, secure processing on Statistics Denmark’s servers over large national registers.

This project aims to bridge a legacy, closed-source data format into modern, open-source workflows. Today many stacks lean on the venerable C-based ReadStat (e.g., haven, pyreadstat); implementing the reader in Rust should make contributions more approachable and redistribution (cross-compilation, shipping wheels/binaries) simpler while preserving performance.

## Related work

- **ReadStat (C)** — battle-tested reference library used by haven and pyreadstat ([WizardMac/ReadStat](https://github.com/WizardMac/ReadStat)).
- **cppsas7bdat (C++)** — C++ reader used for comparison ([olivia76/cpp-sas7bdat](https://github.com/olivia76/cpp-sas7bdat)).
- **Sas7Bdat.Core (C#)** — .NET reader ([richokelly/Sas7Bdat](https://github.com/richokelly/Sas7Bdat)).
- **pandas (Python)** — pandas’ built-in SAS reader (Python implementation, independent of ReadStat) ([pandas-dev/pandas](https://github.com/pandas-dev/pandas/blob/main/pandas/io/sas/sas7bdat.py)).
- **Reverse-engineered SAS7BDAT docs** — historical compatibility study and binary format notes ([BioStatMatt/sas7bdat](https://github.com/BioStatMatt/sas7bdat)).

The crate powers a test suite that cross-checks parsed output against community fixtures and other statistical packages (pandas, PyReadStat, Haven). It also ships an example that downloads the U.S. Census American Housing Survey (AHS) public-use file, converts it to Parquet, and demonstrates end-to-end integration.

## Features

- Zero-copy metadata decoding, including column projections and row pagination.
- Configurable Parquet writer with row-group sizing heuristics.
- Support for companion catalog files to hydrate value labels.
- Comprehensive fixtures spanning multiple SAS encodings and compression modes.
- Datatest-based regression suite that compares results with external toolchains.

## Getting started

Add the library to an existing Cargo project:

```bash
cargo add sas7bdat
```

Or build the repository directly:

```bash
git clone https://github.com/tkragholm/sas7bdat-parser-rs.git
cd sas7bdat-parser-rs
git submodule update --init --recursive
cargo build
```

### Repository layout

- Core Rust crate: `crates/sas7bdat/`
- Python bindings (PyO3/maturin): `python/`
- R bindings (extendr): `R/`

### CLI usage

This repo also ships a small CLI to batch‑convert SAS7BDAT files to Parquet/CSV/TSV using streaming sinks. It supports directory recursion, simple projection, and pagination.

```
cargo run --bin sas7 -- convert path/to/dir --sink parquet --jobs 4
cargo run --bin sas7 -- convert file.sas7bdat --sink csv --out file.csv --columns COL1,COL2 --skip 100 --max-rows 1000
cargo run --bin sas7 -- inspect file.sas7bdat --json
```

Options include `--out-dir`, `--out`, `--sink {parquet|csv|tsv}`, CSV/TSV `--headers/--no-headers` and `--delimiter`, projection via `--columns` or `--column-indices`, pagination with `--skip` and `--max-rows`, and Parquet tuning flags `--parquet-row-group-size` and `--parquet-target-bytes`.

### Converting the AHS dataset

The repository includes an example that downloads the 2013 AHS public-use file ZIP archive, extracts the embedded `.sas7bdat`, and writes `ahs2013n.parquet` to the working directory:

```bash
cargo run --example sas_to_parquet            # default output ahs2013n.parquet
cargo run --example sas_to_parquet -- data/ahs.parquet
```

The example requires network access to `https://www2.census.gov/` during the download step.
If the download is slow or blocked, point at a local or alternate ZIP:

```bash
curl -L -o /tmp/ahs2013.zip "https://www2.census.gov/programs-surveys/ahs/2013/AHS%202013%20National%20PUF%20v2.0%20Flat%20SAS.zip"
AHS_ZIP_PATH=/tmp/ahs2013.zip cargo run --example sas_to_parquet

# or use a mirror
AHS_ZIP_URL=https://your.mirror/AHS2013.zip cargo run --example sas_to_parquet
```

### Using the library

```rust
use std::fs::File;
use sas7bdat::SasReader;

fn main() -> sas7bdat::Result<()> {
    let mut sas = SasReader::open("dataset.sas7bdat")?;
    let metadata = sas.metadata().clone();
    println!("Columns: {}", metadata.variables.len());

    let mut rows = sas.rows()?;
    while let Some(row) = rows.try_next()? {
        // Inspect row values here
        println!("first column = {:?}", row[0]);
    }

    Ok(())
}
```

See the examples in `crates/sas7bdat/examples/` for more complete pipelines, including Parquet export.

## Testing

Run the unit and integration test suites:

```bash
cargo test
```

Snapshot fixtures rely on datasets under `fixtures/raw_data/`. Large archives are ignored by `.gitignore` but are required for the full regression suite.


## License

Licensed under the [MIT License](LICENSE).

## Contributing

Issues and pull requests are welcome. Please open an issue before proposing substantial architectural changes so we can coordinate design and testing expectations.
