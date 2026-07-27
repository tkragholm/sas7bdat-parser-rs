# sas7bdat-cli

A fast command-line tool for reading, inspecting, and converting SAS7BDAT files —
the binary on-disk format produced by SAS. It is a self-contained Rust binary built on
the SIMD-accelerated [`sas7bdat`](https://crates.io/crates/sas7bdat) parser: no SAS
installation, no ReadStat, no C toolchain.

## Installation

```sh
pip install sas7bdat-cli
# or, without polluting an environment:
uv tool install sas7bdat-cli
```

The wheel ships a single executable, `sas7bdat`. It is a plain binary wheel
(`py3-none-<platform>`), so it has no Python version floor and pulls in no
dependencies — Python is only the delivery mechanism.

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

- **[`sas7bdat-polars`](https://pypi.org/project/sas7bdat-polars/)** — a Polars IO plugin
  backed by the same parser, with lazy scans plus projection and predicate pushdown.
  `pip install "sas7bdat-polars[cli]"` installs it together with this CLI.
- **[`sas7bdat`](https://crates.io/crates/sas7bdat)** — the underlying Rust crate.

## License

MIT. See the [repository](https://github.com/tkragholm/sas7bdat-parser-rs) for
third-party notices and the SAS trademark note.
