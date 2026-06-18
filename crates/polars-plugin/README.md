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

# Return character columns as Categorical (low-cardinality category codes).
lf = sp.scan_sas("survey.sas7bdat", categorical=True)

# SAS stores every numeric column as a float. Declare integer-coded columns
# (registry/category codes) explicitly to get Int64 out instead of Float64:
lf = sp.scan_sas(
    "bef2020.sas7bdat",
    schema_overrides={"KOEN": pl.Int64, "SOCIO13": pl.Int64, "HFAUDD": pl.Int64},
)
```

`categorical=True` casts every character column to `Categorical` in the lazy plan
(via Polars' own cast — equivalent to
`sp.scan_sas(path).with_columns(pl.col(pl.String).cast(pl.Categorical))`). The
benefit is **downstream**: group-by / join / sort on these columns run on `u32`
codes and are ~10–15× faster. It is *not* a read or memory win — Polars' `String`
is already compact, so casting adds a little to the read (~0.6s on a 2.5k-string-
column file) and uses more memory; only enable it when you'll group/join on the
string columns. (Contrast with the R binding's `categorical=TRUE`, where `factor`
*is* a read-speed and memory win.)

`schema_overrides` is applied at schema time, so the lazy schema and the collected
frame always agree, and the same override map yields the same dtypes for every file
of a register. Override names that don't exist in a given file are ignored, so a
register-wide map can be passed wholesale. If a file contains a value that violates
an Int64 override (non-integral or out of range), the scan **fails with an error
naming the column, row, and value** — it never silently falls back to Float64.
Supported override dtypes: `Int64`, `Float64`, `Date`, `Datetime`, `Time`, `String`,
`Binary` (numeric columns can only be re-typed to numeric/temporal dtypes, character
columns to `String`/`Binary`). Feature-detect with
`sp.PLUGIN_CONTRACT_VERSION >= "sas7bdat_polars.v2"`.

## License

MIT — see the [repository](https://github.com/tkragholm/sas7bdat-parser-rs) for details.
