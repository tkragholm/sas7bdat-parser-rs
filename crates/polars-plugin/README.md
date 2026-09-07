# sas7bdat-polars

SAS7BDAT files as Arrow streams, with a [Polars](https://pola.rs/) API on top, backed by
the SIMD-accelerated [`sas7bdat`](https://crates.io/crates/sas7bdat) Rust parser.

The compiled extension does not link polars. It hands every decoded batch to Python as an
Arrow C stream behind the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
(`__arrow_c_stream__`), which polars, pyarrow, pandas and duckdb all import through Arrow's
C Data Interface with no copy of the primitive buffers. The polars functions below are a
thin Python layer on polars' public constructors.

## Installation

```sh
pip install sas7bdat-polars
```

To also get the standalone `sas7bdat` command (convert / info / head), install the extra:

```sh
pip install "sas7bdat-polars[cli]"
```

That pulls in [`sas7bdat-cli`](https://pypi.org/project/sas7bdat-cli/), a separate binary
wheel built from the same parser. It is kept separate on purpose: it carries no polars
requirement and no Python floor.

### Version constraints

- **Polars is a floor, not a pin**: `>=1.41` for this release, the oldest version the
  `compat` CI job installs the wheel against; the same job runs the newest polars on
  PyPI. Nothing in the wheel depends on polars' internals, so a new polars release
  needs no new wheel. The package checks the floor at import and raises an `ImportError`
  naming both versions below it; `SAS7BDAT_POLARS_SKIP_VERSION_CHECK=1` overrides that.
- **Built against the CPython stable ABI** (`abi3`, minimum 3.12), so a single `cp312-abi3`
  wheel runs on CPython 3.12 and newer.

## Usage

```python
import polars as pl
import sas7bdat_polars as sp

# Eager read. ALWAYS pass `columns`: SAS7BDAT is wide and row-oriented, so projecting
# the columns you need is the biggest speed-up.
df = sp.read_sas("data.sas7bdat", columns=["name", "age"])
df = sp.read_sas("data.sas7bdat", columns=["age"], n_rows=1_000_000)   # bound I/O
df = sp.read_sas("data.sas7bdat", columns=["age"], predicate=pl.col("age") > 30)

# Lazy scan. Projection and a row limit reach the decoder; a filter is applied to
# each batch by polars as it arrives.
lf = sp.scan_sas("data.sas7bdat", columns=["name", "age"])
df = lf.filter(pl.col("age") > 30).collect()

# Header-only metadata (row/column count, encoding, size) without decoding the body.
info = sp.sas_info("data.sas7bdat")   # {'n_rows': ..., 'n_columns': ..., 'encoding': ...}

# Value labels from a companion format catalog.
lf = sp.scan_sas("data.sas7bdat", catalog_path="formats.sas7bcat")

# The schema without reading rows.
schema = sp.schema_for_file("data.sas7bdat")
```

### Any Arrow consumer

A `SasDataset` and every stream it hands out expose `__arrow_c_stream__`, so the same
file reads without polars:

```python
ds = sp.SasDataset("data.sas7bdat", schema_overrides={"ID": pl.Int64})
table = pyarrow.table(ds)                                  # pyarrow
frame = pl.DataFrame(ds.stream(columns=["ID"], n_rows=10)) # polars, projected
for batch in ds.batch_reader(["ID"], None, None, 65_536):  # one pl.DataFrame per batch
    ...
```

## Where this differs from `pyreadstat`

**Deleted rows are excluded.** SAS tombstones a deleted row rather than removing it:
the row stays on the page and stays counted by the header. This reader recognises the
mark, in both the uncompressed and the compressed representation, and drops those rows.
The ReadStat 1.1.9 that `pyreadstat` ships does not, so on a file with deletions this
reader returns fewer rows, and the difference is `pyreadstat`'s. ReadStat built after
[#366](https://github.com/WizardMac/ReadStat/pull/366) agrees with this reader.

## Performance & threading

The rules:

- **Always project** (`read_sas(columns=...)` / `scan_sas(columns=...)`). Reading one
  column instead of all is ~50× on wide files and the biggest lever by far.
- **Bound huge reads** with `n_rows=` when you only need a peek: the reader's row
  limit stops after the first pages, cutting I/O.
- **Let the reader parallelise.** It runs its own SIMD page decode across all cores;
  tune with `set_scan_threads(n)` (or `SAS7BDAT_SCAN_THREADS`). Do **not** throttle
  Polars' own pool (`POLARS_MAX_THREADS`): it does not control the decoder and only
  starves the pipeline. (The library warns if it detects this mistake.)
- **Streaming works** (`.collect(engine="streaming")`): each scan decodes on its own
  thread and a consumer that lets go of a stream early stops it.

```python
sp.set_scan_threads(8)   # cap decode threads; set_scan_threads(0) resets to all cores
sp.scan_threads()        # -> effective count

# Read through a bounded buffer instead of mapping the file. A mapped file counts
# every page it touches against the process's resident set while the dataset
# lives: on a 128 MB file that was 405 MB peak against 301 MB buffered, and on a
# multi-gigabyte register file it is the difference between a working set the
# size of the file and one the size of the result. The price is slower peeks
# (`head`) and single-column reads; full reads are within noise. `SAS7BDAT_IO_BACKEND`
# sets it for every dataset that does not name one.
df = sp.read_sas("bef2020.sas7bdat", columns=["PNR"], io_backend="buffered")

# Hand named string columns over dictionary-encoded, which polars reads as
# Categorical: a 32-bit code per row and each distinct value once per batch.
lf = sp.scan_sas("lpr_diag.sas7bdat", categorical=["C_DIAGTYPE", "C_PATTYPE"])
df = sp.read_sas("survey.sas7bdat", categorical=True)   # every string column

# SAS stores every numeric column as a float. Declare integer-coded columns
# (registry/category codes) explicitly to get Int64 out instead of Float64:
lf = sp.scan_sas(
    "bef2020.sas7bdat",
    schema_overrides={"KOEN": pl.Int64, "SOCIO13": pl.Int64, "HFAUDD": pl.Int64},
)
```

`categorical` is done by the reader, not by a cast afterwards: the column crosses as
an Arrow dictionary array, polars imports the codes and the values as they are and
unifies the batches' dictionaries on its side. A polars `String` holds sixteen bytes
per value plus the bytes of any value over twelve; a `Categorical` holds four per row
plus each distinct value once. Right for codes and labels, wrong for identifiers,
where the dictionary is as large as the data and the codes come on top. Value labels
from a catalog can be categorical too. Group-by, join and sort on these columns then
run on the codes.

`schema_overrides` is applied at schema time, so the lazy schema and the collected
frame always agree, and the same override map yields the same dtypes for every file
of a register. Override names that don't exist in a given file are ignored, so a
register-wide map can be passed wholesale. If a file contains a value that violates
an Int64 override (non-integral or out of range), the scan **fails with an error
naming the column, row, and value**; it never silently falls back to Float64.
Supported override dtypes: `Int64`, `Float64`, `Date`, `Datetime`, `Time`, `String`,
`Binary`, as polars dtypes or as those names (numeric columns can only be re-typed
to numeric/temporal dtypes, character columns to `String`/`Binary`). Feature-detect
with `sp.PLUGIN_CONTRACT_VERSION >= "sas7bdat_polars.v3"`.

## License

MIT. See the [repository](https://github.com/tkragholm/sas7bdat-parser-rs) for details.
