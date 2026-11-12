# fastsas

Read SAS7BDAT files into R, using the
[`sas7bdat`](https://crates.io/crates/sas7bdat) Rust reader through
[extendr](https://extendr.rs/). Results match haven's column types.

```r
library(fastsas)
df <- read_sas7bdat("data.sas7bdat")
df <- read_sas7bdat("data.sas7bdat", catalog = "formats.sas7bcat")
df <- read_sas7bdat("data.sas7bdat", categorical = TRUE)
```

`read_sas()` is an alias, but haven also exports `read_sas()`, so the package
attached last wins. Use `read_sas7bdat()` or `fastsas::read_sas()`.

## Installation

The package is not on CRAN yet. It installs from a clone of the repository
and needs R and a Rust toolchain (`cargo`, `rustc` 1.89 or later):

```sh
R CMD INSTALL --no-staged-install crates/r-plugin
```

## What you get

A tibble with these column types:

| SAS column | R type |
|---|---|
| numeric | `double` |
| character | `character` (UTF-8) |
| date format | `Date` |
| datetime format | `POSIXct` (UTC), with fractional seconds |
| time format | `hms` |

- Missing values become `NA`. Special missing values (`.A` to `.Z`, `._`)
  become `haven::tagged_na()`, on numeric and temporal columns.
- Variable labels are set as each column's `label` attribute, as haven does.
- With a `.sas7bcat` catalog, passed as `catalog` or placed next to the data
  file with the same name, labelled columns are `haven_labelled` vectors.
- `categorical = TRUE` returns unlabelled character columns as `factor`. On a
  file with 2,574 character columns this read 2.6 times faster and used about
  a third less memory; see [benchmarks](../../benchmarks/README.md). Columns
  with many distinct values stay `character`.

## Where results differ from haven

- **Deleted rows.** SAS marks a deleted row instead of removing it. fastsas
  drops those rows. The ReadStat 1.1.9 release that haven uses returns them.
- **Western encodings.** Files declaring ISO-8859-1 are decoded as
  Windows-1252. The two differ only on bytes 0x80 to 0x9F, which SAS files
  written on Windows use for curly quotes, dashes and the euro sign.
- **Integer overrides.** Columns read as `Int64` are returned as `double`.

## Options

- `io_backend`: `"auto"` (the default) memory-maps local files and reads
  network shares sequentially. Only Windows can tell the two apart, so on other
  systems pass `io_backend = "buffered"` for files on a share.
- `threads`: the number of decode threads (default: all cores).

## Development

`src/rust/Cargo.toml` depends on `sas7bdat` by version, so a package built
with `R CMD build` takes the core from crates.io. Inside the repository,
`src/.cargo/config.toml` points it at the working tree instead; cargo only
finds that file when run from `src/` or below, which the Makevars does.
`.Rbuildignore` keeps the redirect out of the tarball.

```sh
R CMD build crates/r-plugin
R CMD check --no-manual fastsas_*.tar.gz
```
