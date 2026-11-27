# fastsasconvert 0.4.1

## Documentation

- The README's links point to the GitHub repository, so they resolve from the installed package.

Built with sas7bdat 0.10.4, sas7bdat-convert 0.5.0.

# fastsasconvert 0.4.0

## New features

- `convert_sas()` gains `columns`, made by the new `sas_columns()`, to set the written names by a case rule and renames, add constant columns such as the input's path, and set the Parquet types of strings, timestamps and times.
- `convert_sas()` gains `output_files`, one destination per input in input order, and `labels`, which stores the SAS labels and formats in the Parquet file's metadata.

## Changes

- SAS times are written as Parquet `TIME` by default, which `arrow::read_parquet()` reads as `hms` and DuckDB as `TIME`. A file holding a time outside one day fails, and its `error` names the column and the fix: `sas_columns(times = "duration")` writes durations as earlier versions did.

## Bug fixes

- `R CMD check --as-cran` passes without WARNINGs. The Rust build tree is removed once the package is linked, and the bundled C code is built for the same macOS version as the Rust code.
- The tests use at most two threads, as CRAN asks.

Built with sas7bdat 0.10.4 and sas7bdat-convert 0.5.0.

# fastsasconvert 0.3.2

## Bug fixes

- Builds for Windows on ARM64. The link failed on a flag its linker does not accept.

Built with sas7bdat 0.10.4 and sas7bdat-convert 0.4.2.

# fastsasconvert 0.3.1

No change in behaviour. Built with sas7bdat 0.10.3 and sas7bdat-convert 0.4.2.

# fastsasconvert 0.3.0

## New features

- `convert_sas()` gains `encoding`, to read the text in another encoding than the one a file declares, and `strict`, to fail a file with a cell that does not decode.

## Performance

- CSV and TSV are written from batches decoded in parallel.
- Converting wide files needs less memory. Parquet from the 4,041-column AHS national file peaked at 2.1 GB, down from 3.7 GB, and CSV at 846 MB, down from 1.4 GB.
- Finding the missing values in numeric columns is about 7 times faster on x86-64 CPUs with AVX2 but not AVX-512.

## Bug fixes

- Error messages name the file.

Built with sas7bdat 0.10.2 and sas7bdat-convert 0.4.2.
