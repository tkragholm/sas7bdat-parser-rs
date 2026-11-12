# sas7bdat-cli

A command-line tool for inspecting SAS7BDAT files, the binary format written by
SAS, and converting them to Parquet, CSV or TSV. It is a single Rust binary built on
the [`sas7bdat`](https://crates.io/crates/sas7bdat) reader, and needs no SAS
installation or C library.

## Installation

```sh
pip install sas7bdat-cli
# or, as an isolated tool:
uv tool install sas7bdat-cli
```

The wheel ships a single executable, `sas7bdat`. It is a plain binary wheel
(`py3-none-<platform>`), so it works with any Python version and has no
dependencies; pip is only the way it is delivered.

Rust users can install the same tool from source instead:

```sh
cargo install --git https://github.com/tkragholm/sas7bdat-parser-rs sas7bdat-cli
```

## Usage

```sh
sas7bdat info data.sas7bdat        # columns, types, labels, and a sample
sas7bdat head data.sas7bdat        # first rows as a table
sas7bdat convert data.sas7bdat     # -> data.parquet (Zstd)
sas7bdat convert data.sas7bdat --out out.csv        # format inferred from the extension
sas7bdat convert ./folder --out-dir out             # whole tree, mirrored
sas7bdat completions zsh           # shell completion script
```

Run `sas7bdat <command> --help` for the full options of a command.

## Related packages

- [`sas7bdat-polars`](https://pypi.org/project/sas7bdat-polars/): a Polars plugin
  built on the same reader, with lazy scans and projection pushdown.
  `pip install "sas7bdat-polars[cli]"` installs it together with this CLI.
- [`sas7bdat`](https://crates.io/crates/sas7bdat): the Rust crate.

## License

MIT. See the [repository](https://github.com/tkragholm/sas7bdat-parser-rs) for
third-party notices and the SAS trademark note.
