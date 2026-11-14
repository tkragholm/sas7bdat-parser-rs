# sas7bdat-polars

Read SAS7BDAT files into [Polars](https://pola.rs/), using the
[`sas7bdat`](https://crates.io/crates/sas7bdat) Rust reader. Batches reach
Python as Arrow C streams, so pyarrow, pandas and DuckDB can read them too.

```sh
pip install sas7bdat-polars           # needs Python 3.12+ and polars 1.41.1+
pip install "sas7bdat-polars[cli]"    # also the `sas7bdat` command-line tool
```

Wheels are built for Linux (x86-64 and ARM64), Windows (x86-64) and macOS (Apple
Silicon). On x86-64 they need a CPU with AVX2: Intel Haswell (2013) or AMD
Excavator (2015) and later.

## Usage

```python
import polars as pl
import sas7bdat_polars as sp

# Eager read. Name the columns you need: SAS7BDAT stores rows, so projection
# is the largest saving on wide files.
df = sp.read_sas("data.sas7bdat", columns=["id", "age"])
df = sp.read_sas("data.sas7bdat", columns=["age"], n_rows=1_000_000)
df = sp.read_sas("data.sas7bdat", row_offset=1_000_000, n_rows=1_000_000)  # rows 1M to 2M

# Lazy scan. Projection and a row limit reach the decoder; filters run per batch.
lf = sp.scan_sas("data.sas7bdat", columns=["id", "age"])
df = lf.filter(pl.col("age") > 30).collect()

# Metadata and schema without decoding rows: counts, encoding, table name, and
# each column's label and SAS format.
info = sp.sas_info("data.sas7bdat")
columns = sp.column_info("data.sas7bdat")   # name, label, format, type, width
schema = sp.schema_for_file("data.sas7bdat")

# Value labels from a companion catalog: labelled columns come back as labels,
# or as their codes with apply_labels=False; value_labels gives the mapping.
lf = sp.scan_sas("data.sas7bdat", catalog_path="formats.sas7bcat")
codes = sp.SasDataset("data.sas7bdat", "formats.sas7bcat", apply_labels=False).read()
labels = sp.value_labels("data.sas7bdat", "formats.sas7bcat")  # {column: {code: label}}

# A header that names the wrong encoding, and failing on bytes that do not decode.
df = sp.read_sas("data.sas7bdat", encoding="utf-8", strict=True)

# Several files with the same columns, by list or pattern, with each row's file.
df = sp.read_sas("lmdb_*.sas7bdat", include_file_paths="source")

# SAS special missing values (.A to .Z, ._): a Q1_missing column with each tag.
df = sp.read_sas("survey.sas7bdat", missing_tags=["Q1", "Q2"])

# Dates, datetimes and times as SAS's numbers (days or seconds from 1960-01-01).
df = sp.read_sas("data.sas7bdat", temporal_as_numbers=True)

# SAS stores every number as a float. Declare integer-coded columns to get Int64.
lf = sp.scan_sas("data.sas7bdat", schema_overrides={"region": pl.Int64})

# Return string columns as Categorical, for codes and labels.
lf = sp.scan_sas("data.sas7bdat", categorical=["diagnosis"])
```

Without polars:

```python
import pyarrow as pa

table = pa.table(sp.SasDataset("data.sas7bdat"))
```

## Behaviour worth knowing

- **Deleted rows are excluded.** SAS marks a deleted row instead of removing
  it. This reader drops those rows. The ReadStat 1.1.9 release that pyreadstat
  ships returns them, so on such files pyreadstat reports more rows.
- **Schema overrides fail loudly.** A value that does not fit an `Int64`
  override, such as 2.5, stops the scan with an error naming the column, row
  and value. Override names missing from a file are ignored, so one map can be
  used for a whole set of files. Supported types: `Int64`, `Float64`, `Date`,
  `Datetime`, `Time`, `String`, `Binary`.
- **Categorical** columns are dictionary-encoded by the reader. They suit codes
  and labels, not identifiers, where the dictionary is as large as the data.
- **Threads.** The reader decodes on all cores. Set a limit with
  `sp.set_scan_threads(n)` or `SAS7BDAT_SCAN_THREADS`. `POLARS_MAX_THREADS`
  does not affect the decoder.
- **Memory.** By default a local file is memory-mapped. On files much larger
  than the result, `io_backend="buffered"` keeps memory bounded, at some cost
  to small reads. `SAS7BDAT_IO_BACKEND` sets the default.

## Licence

MIT. See the [repository](https://github.com/tkragholm/sas7bdat-parser-rs).
