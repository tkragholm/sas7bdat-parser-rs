# sas7bdat-polars

A [Polars](https://pola.rs/) IO plugin for reading SAS7BDAT files, backed by the
SIMD-accelerated [`sas7bdat`](https://crates.io/crates/sas7bdat) Rust parser. It registers
a native IO source via `polars.io.plugins.register_io_source`, so scans are lazy and
support projection and predicate pushdown straight into the reader.

## Installation

```sh
pip install sas7bdat-polars
```

### Version constraints

This wheel is tightly coupled to its build environment:

- **Polars is pinned to `1.40.*`.** The extension shares the Polars Rust ABI (via
  `polars-ffi`) with the in-process `polars` package, so the installed `polars` must match
  the version the wheel was built against. A mismatch is undefined behavior, not a graceful
  error.
- **Built against the CPython stable ABI** (`abi3`, minimum 3.12), so a single `cp312-abi3`
  wheel runs on CPython 3.12 and newer.

## Usage

```python
import polars as pl
import sas7bdat_polars as sp

# Lazy scan — returns a LazyFrame; filters/projections push down into the reader.
lf = sp.scan_sas("data.sas7bdat")
df = lf.filter(pl.col("age") > 30).select("name", "age").collect()

# Hydrate value labels from a companion catalog.
lf = sp.scan_sas("data.sas7bdat", catalog_path="formats.sas7bcat")

# Inspect the Arrow schema without reading rows.
schema = sp.schema_for_file("data.sas7bdat")
```

## License

MIT — see the [repository](https://github.com/tkragholm/sas7bdat-parser-rs) for details.
