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
