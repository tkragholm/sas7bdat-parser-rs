# sas7bdat-polars

A [Polars](https://pola.rs/) IO plugin for reading SAS7BDAT files, backed by the
SIMD-accelerated [`sas7bdat`](https://crates.io/crates/sas7bdat) Rust parser. It registers
a native IO source via `polars.io.plugins.register_io_source`, so scans are lazy and
support projection and predicate pushdown straight into the reader.

## Installation

```sh
pip install sas7bdat-polars
```

To also get the standalone `sas7bdat` command (convert / info / head), install the extra:

```sh
pip install "sas7bdat-polars[cli]"
```

That pulls in [`sas7bdat-cli`](https://pypi.org/project/sas7bdat-cli/), a separate binary
wheel built from the same parser. It is kept separate on purpose: it carries no polars pin
and no Python floor, so CLI-only users do not inherit this package's constraints.

### Version constraints

This wheel is tightly coupled to its build environment:

- **Polars is pinned to `1.41.*`.** The extension shares the Polars Rust ABI (via
  `polars-ffi`) with the in-process `polars` package, so the installed `polars` must match
  the version the wheel was built against. A mismatch is undefined behavior, not a graceful
  error.
- **Built against the CPython stable ABI** (`abi3`, minimum 3.12), so a single `cp312-abi3`
  wheel runs on CPython 3.12 and newer.

## Usage

```python
import polars as pl
import sas7bdat_polars as sp

# Eager read — the ergonomic default. ALWAYS pass `columns`: SAS7BDAT is wide and
# row-oriented, so projecting the columns you need is the biggest speed-up.
df = sp.read_sas("data.sas7bdat", columns=["name", "age"])
df = sp.read_sas("data.sas7bdat", columns=["age"], n_rows=1_000_000)   # bound I/O
df = sp.read_sas("data.sas7bdat", columns=["age"], predicate=pl.col("age") > 30)

# Lazy scan — returns a LazyFrame; filters/projections push down into the reader.
lf = sp.scan_sas("data.sas7bdat", columns=["name", "age"])
df = lf.filter(pl.col("age") > 30).collect()

# Header-only metadata (row/column count, encoding, size) without decoding the body.
info = sp.sas_info("data.sas7bdat")   # {'n_rows': ..., 'n_columns': ..., 'encoding': ...}

# Hydrate value labels from a companion catalog.
lf = sp.scan_sas("data.sas7bdat", catalog_path="formats.sas7bcat")

# Inspect the Arrow schema without reading rows.
schema = sp.schema_for_file("data.sas7bdat")
```

## Where this differs from `pyreadstat`

**Deleted rows are excluded.** SAS tombstones a deleted row rather than removing it:
the row stays on the page and stays counted by the header. This plugin recognises the
mark, in both the uncompressed and the compressed representation, and drops those rows.
The ReadStat 1.1.9 that `pyreadstat` ships does not, so on a file with deletions this
plugin returns fewer rows, and the difference is `pyreadstat`'s. ReadStat built after
[#366](https://github.com/WizardMac/ReadStat/pull/366) agrees with this plugin.

## Performance & threading

Benchmarked on a 2.1 GB / 4041-column file (warm cache): a full `.collect()` takes
~1.8 s (decodes every column) while `read_sas(columns=[one])` takes ~0.04 s. The rules:

- **Always project** (`read_sas(columns=...)` / `scan_sas(columns=...)`). Reading one
  column instead of all is ~50× on wide files and the biggest lever by far.
- **Bound huge reads** with `n_rows=` when you only need a peek — the reader's row
  limit stops after the first pages, cutting I/O.
- **Let the reader parallelise.** It runs its own SIMD page decode across all cores;
  tune with `set_scan_threads(n)` (or `SAS7BDAT_SCAN_THREADS`). Do **not** throttle
  Polars' own pool (`POLARS_MAX_THREADS`) — it does not control the decoder and only
  starves the pipeline. (The library warns if it detects this mistake.)
- **Streaming works** (`.collect(engine="streaming")`): the reader is `Send + Sync`.

```python
sp.set_scan_threads(8)   # cap decode threads; set_scan_threads(0) resets to all cores
sp.scan_threads()        # -> effective count

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
