# sas7bdat-polars

Read SAS7BDAT files into [Polars](https://pola.rs/), using the
[`sas7bdat`](https://crates.io/crates/sas7bdat) Rust reader. Batches reach
Python as Arrow C streams, so pyarrow, pandas and DuckDB can read them too.

```sh
pip install sas7bdat-polars           # needs Python 3.12+ and polars 1.41+
pip install "sas7bdat-polars[cli]"    # also the `sas7bdat` command-line tool
```

## Usage

```python
import polars as pl
import sas7bdat_polars as sp

# Eager read. Name the columns you need: SAS7BDAT stores rows, so projection
# is the largest saving on wide files.
df = sp.read_sas("data.sas7bdat", columns=["id", "age"])
df = sp.read_sas("data.sas7bdat", columns=["age"], n_rows=1_000_000)

# Lazy scan. Projection and a row limit reach the decoder; filters run per batch.
lf = sp.scan_sas("data.sas7bdat", columns=["id", "age"])
df = lf.filter(pl.col("age") > 30).collect()

# Metadata and schema without decoding rows.
info = sp.sas_info("data.sas7bdat")
schema = sp.schema_for_file("data.sas7bdat")

# Value labels from a companion catalog.
lf = sp.scan_sas("data.sas7bdat", catalog_path="formats.sas7bcat")

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
