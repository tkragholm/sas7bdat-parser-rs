# readsas

Fast SAS7BDAT reader for R, backed by the Rust `sas7bdat` core in this
workspace and exposed via [extendr](https://extendr.github.io/).

```r
library(readsas)
df <- read_sas("path/to/file.sas7bdat")
```

Returns a tibble with `haven`-compatible column types:

| SAS column            | R type                         |
|-----------------------|--------------------------------|
| numeric               | `double`                       |
| character             | `character` (UTF-8)            |
| date format           | `Date`                         |
| datetime format       | `POSIXct` (UTC)               |
| time format           | `hms`                          |

SAS missing values become `NA`. The SAS epoch (1960-01-01) is rebased to the R
epoch (1970-01-01) during decode.

## Build / install

Requires R, and a Rust toolchain (`cargo`, `rustc`).

```sh
R CMD INSTALL --no-staged-install crates/r-plugin
```

The Rust crate (`src/rust`) depends on the workspace `sas7bdat` crate by relative
path. The Makevars invokes `cargo build` to produce `libreadsas.a`, which is
linked into `readsas.so`.

## Status: v1

This is the first working binding. It consumes the core's existing
`OwnedColumnBuffer` columns and marshals them into R vectors on the main thread:
one memcpy per numeric/temporal column, UTF-8 interning for strings. See
`../../docs/r-bindings/design-direct-fill.md` for the design and the deferred
optimizations (numeric direct-fill, dictionary-driven string interning,
tagged-NA / `labelled` haven-parity).

### Known limitations / follow-ups

- **Format classification is the core's.** Columns the core classifies as
  `Float` (e.g. some datetime/time SAS formats like `DTDATE`, `HHMM`) arrive as
  `double`, not `POSIXct`/`hms`. This is shared with the Polars plugin and is a
  core `LogicalType` matter, not a binding bug.
- **No variable/value labels yet.** `haven_labelled` columns and column labels
  are not wired through (planned: pass core `LabelSet` metadata out as a side
  list and reclass in the R wrapper).
- **Int64 columns** (from explicit schema overrides) are coerced to `double`
  for haven-parity. A `bit64::integer64` opt-in is a follow-up.
- **Distribution.** The relative path dependency on `sas7bdat` works for in-repo
  local installs; a distributable package vendors or version-pins the core.
