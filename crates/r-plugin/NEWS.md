# fastsas 0.3.4

## Bug fixes

- The tests pass on Windows. A test file named `test-nul.R` gave the snapshot path `_snaps/nul.md`, and `NUL` is a reserved file name there.

## Documentation

- The README's links point to docs.rs and the GitHub repository, so they resolve from the installed package.

Built with sas7bdat 0.10.4.

# fastsas 0.3.3

## Bug fixes

- `R CMD check --as-cran` passes without the WARNING about `abort`. The Rust build tree, whose standard library references `abort`, is removed once the package is linked.
- The tests use at most two threads, as CRAN asks.

Built with sas7bdat 0.10.4.

# fastsas 0.3.2

## Bug fixes

- Builds for WebAssembly (webR) and for Windows on ARM64. The WebAssembly build failed on a dependency that compiles only on 64-bit targets, and the Windows ARM64 link on a flag its linker does not accept.

Built with sas7bdat 0.10.4.

# fastsas 0.3.1

## Performance

- `read_sas7bdat()` uses at most four decode threads unless `threads` is set. More did not read faster, because the R vectors are filled on one thread, and each further thread held a batch in memory. The AHS household file peaks at 2.0 GB, down from 2.6 to 2.8 GB; haven peaks at 1.9 GB.

Built with sas7bdat 0.10.3.

# fastsas 0.3.0

## New features

- `read_sas7bdat()` gains `col_select`, `skip` and `n_max`, to read some columns or rows of a file.
- `read_sas7bdat()` reads several files at once when `path` has more than one element, and `id` names a column holding each row's file.
- `encoding` reads a file's text in another encoding than the one it declares, and `strict = TRUE` fails on the first cell that does not decode.
- `temporal_as_numbers = TRUE` returns date, datetime and time columns as the numbers SAS stores, without conversion.
- `sas_info()` gives a file's metadata without reading its rows.
- Columns with a SAS format carry haven's `format.sas` attribute.
- Errors are classed conditions (`fastsas_error_io`, `fastsas_error_format`, `fastsas_error_decode`, `fastsas_error_unsupported`, `fastsas_error_argument`), so a caller can handle one kind with `tryCatch()`. A catalog found beside the file that cannot be read gives a warning, where it was ignored silently; the file is still read, without labels.

## Bug fixes

- Rows SAS marked as deleted are left out of the result.
- Text with a NUL byte inside it is cut at the NUL instead of raising an error.
- Error messages name the file.

## Performance

- Wide files need less memory: the 2,167-column AHS household file peaked at 2.7 GB, down from 3.3 GB.
- Finding the missing values in numeric columns is about 7 times faster on x86-64 CPUs with AVX2 but not AVX-512.

Built with sas7bdat 0.10.2.
