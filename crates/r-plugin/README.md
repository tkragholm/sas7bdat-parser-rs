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

SAS missing values become `NA`. SAS **special missings** (`.A`–`.Z`, `._`) on
numeric columns become `haven::tagged_na()` values, bit-exact with `haven`. The
SAS epoch (1960-01-01) is rebased to the R epoch (1970-01-01) during decode.

```r
df <- read_sas("survey.sas7bdat")
haven::na_tag(df$income)   # e.g. "b" where SAS stored .B ("refused")
```

SAS variable labels are attached as each column's `label` attribute. Value-label
formats are supported via a `.sas7bcat` catalog: pass `catalog = "..."` (or drop
a same-stem `.sas7bcat` next to the data file) and labelled columns are returned
as `haven_labelled` vectors.

```r
df <- read_sas("data.sas7bdat", catalog = "formats.sas7bcat")
attr(df$SEX, "labels")   # c(Male = 1, Female = 2)
```

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
one memcpy per numeric/temporal column, UTF-8 interning for strings. Variable
labels and value-label catalogs are wired through (see above). See
`../../docs/r-bindings/design-direct-fill.md` for the design and the deferred
optimizations (numeric direct-fill, dictionary-driven string interning,
tagged-NA haven-parity).

Column R types are driven by the SAS **logical type**, not the core's
`OwnedColumnBuffer` variant. This matters for temporal columns: the core emits a
typed `Date`/`DateTime`/`Time` buffer for whole-unit values but falls back to a
raw `F64` buffer when a column carries fractional seconds (its integer-only
`SasDateTime` can't hold them). The binding still types such columns as
`POSIXct`/`hms` (which represent fractional seconds), matching `haven` — so a
`DATETIME` column with sub-second values is `POSIXct`, not `double`.

(The Polars plugin already does the equivalent: it coerces an F64 fallback
buffer to the declared temporal dtype from its schema. This binding now matches
that behavior.)

Value labels keyed on a special missing (e.g. a format that labels `.A` as
`"Refused"`) are carried through as a `tagged_na`-valued entry in the column's
`labels` vector, matching `haven`.

### Known limitations / follow-ups

- **Int64 columns** (from explicit schema overrides) are coerced to `double`
  for haven-parity. A `bit64::integer64` opt-in is a follow-up.
- **Distribution.** The relative path dependency on `sas7bdat` works for in-repo
  local installs; a distributable package vendors or version-pins the core.
