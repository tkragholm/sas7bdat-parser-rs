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

## Performance

The core decodes batches across all cores; `read_sas` pre-allocates each R
vector at the known row count and fills it in place on the main thread:

- **Numeric / temporal:** one copy from the decoded batch into the REALSXP
  (column-major, so each vector is written sequentially), with the SAS→R epoch
  shift and tagged-NA folded into the write — no intermediate `Rfloat` buffer.
- **Character:** a per-column dictionary interns each distinct value once and
  fills cells with raw `SET_STRING_ELT` (UTF-8), avoiding per-cell `mkChar` and
  extendr `set_elt` overhead — important for wide files with millions of string
  cells.

See `benchmarks/` for numbers (≈15–24× faster than `haven`) and
`../../docs/r-bindings/design-direct-fill.md` for the design.

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
