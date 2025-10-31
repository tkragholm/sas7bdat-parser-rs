# sas7bdat-parser-rs

`sas7bdat-parser-rs` is a Rust library for decoding SAS7BDAT datasets with a focus on reproducible research workflows. It exposes a safe API for inspecting metadata, streaming rows, and writing Parquet output so that legacy SAS exports can participate in modern data engineering pipelines.

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
cargo add sas7bdat-parser-rs
```

Or build the repository directly:

```bash
git clone https://github.com/tkragholm/sas7bdat-parser-rs.git
cd sas7bdat-parser-rs
cargo build
```

### CLI usage

This repo also ships a small CLI to batch‑convert SAS7BDAT files to Parquet/CSV/TSV using streaming sinks. It supports directory recursion, simple projection, and pagination.

```
cargo run --bin sas7bd -- convert path/to/dir --sink parquet --jobs 4
cargo run --bin sas7bd -- convert file.sas7bdat --sink csv --out file.csv --columns COL1,COL2 --skip 100 --max-rows 1000
cargo run --bin sas7bd -- inspect file.sas7bdat --json
```

Options include `--out-dir`, `--out`, `--sink {parquet|csv|tsv}`, CSV/TSV `--headers/--no-headers` and `--delimiter`, projection via `--columns` or `--column-indices`, pagination with `--skip` and `--max-rows`, and Parquet tuning flags `--parquet-row-group-size` and `--parquet-target-bytes`.

### Converting the AHS dataset

The repository includes an example that downloads the 2013 AHS public-use file ZIP archive, extracts the embedded `.sas7bdat`, and writes `ahs2013n.parquet` to the working directory:

```bash
cargo run --example sas_to_parquet            # default output ahs2013n.parquet
cargo run --example sas_to_parquet -- data/ahs.parquet
```

The example requires network access to `https://www2.census.gov/` during the download step.

### Using the library

```rust
use std::fs::File;
use sas7bdat_parser_rs::SasFile;

fn main() -> sas7bdat_parser_rs::Result<()> {
    let mut sas = SasFile::open("dataset.sas7bdat")?;
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

See the examples in `examples/` for more complete pipelines, including Parquet export.

## Testing

Run the unit and integration test suites:

```bash
cargo test
```

Snapshot fixtures rely on datasets under `fixtures/raw_data/`. Large archives are ignored by `.gitignore` but are required for the full regression suite.

## Citation

If you use `sas7bdat-parser-rs` in academic work, please cite the JOSS paper once published. A `CITATION.cff` file will be added alongside the paper metadata.

## License

Licensed under the [MIT License](LICENSE).

## Contributing

Issues and pull requests are welcome. Please open an issue before proposing substantial architectural changes so we can coordinate design and testing expectations.
