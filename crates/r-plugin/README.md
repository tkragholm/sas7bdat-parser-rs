# fastsas

Fast SAS7BDAT reader for R, backed by the Rust `sas7bdat` core in this
workspace and exposed via [extendr](https://extendr.github.io/).

```r
library(fastsas)
df <- read_sas7bdat("path/to/file.sas7bdat")
```

`read_sas()` is an exported alias, but `haven` exports a `read_sas()` too, so
whichever package is attached second wins and `library(fastsas); library(haven)`
silently changes which one you get. Prefer `read_sas7bdat()`, or qualify the call
as `fastsas::read_sas()`.

Returns a tibble with `haven`-compatible column types:

| SAS column            | R type                         |
|-----------------------|--------------------------------|
| numeric               | `double`                       |
| character             | `character` (UTF-8)            |
| date format           | `Date`                         |
| datetime format       | `POSIXct` (UTC)               |
| time format           | `hms`                          |

SAS missing values become `NA`. SAS **special missings** (`.A`–`.Z`, `._`) become
`haven::tagged_na()` values, bit-exact with `haven`, on numeric *and* temporal
columns. SAS writes the missing-value indicator in two different spellings and
both are decoded; see `MISSING_TAG` in `src/rust/src/lib.rs`. The SAS epoch
(1960-01-01) is rebased to the R epoch (1970-01-01) during decode.

```r
df <- read_sas7bdat("survey.sas7bdat")
haven::na_tag(df$income)   # e.g. "b" where SAS stored .B ("refused")
```

Files declaring a single-byte Western encoding are decoded as **Windows-1252**,
not strict ISO-8859-1. The two differ only on bytes `0x80`–`0x9F`, which
ISO-8859-1 leaves as control characters and real SAS-on-Windows files use for
curly quotes, dashes and the euro sign. `haven` decodes those as controls, so the
two packages differ on exactly those bytes.

SAS variable labels are attached as each column's `label` attribute, matching
`haven` byte for byte: trailing ASCII padding is dropped (SAS writes a label at
its declared width), while a leading space — or a trailing non-breaking space — is
content and is kept. Value-label
formats are supported via a `.sas7bcat` catalog: pass `catalog = "..."` (or drop
a same-stem `.sas7bcat` next to the data file) and labelled columns are returned
as `haven_labelled` vectors.

### Network drives (`io_backend`)

`io_backend = "auto"` (the default) memory-maps local files and reads network
shares sequentially. It can only tell the two apart on Windows, where a UNC path
is remote by construction and a mapped drive is resolved through the OS;
everywhere else every path looks local and is memory-mapped.

That matters because mapping a file on a share turns each access into a network
round-trip with no readahead. Override it when `auto` cannot tell:

```r
df <- read_sas7bdat("//server/share/data.sas7bdat", io_backend = "buffered")
```

The sequential path itself is tuned for SMB — 4 MB reads, at most four in flight
regardless of how many decode threads are running.

`threads` bounds decode concurrency (default: every logical core). Lowering it
leaves the machine room for other work and bounds the memory held by in-flight
batches; it does not change read concurrency, which the reader caps separately.

### Categorical columns (`categorical = TRUE`)

SAS character columns are usually low-cardinality category codes. Pass
`categorical = TRUE` to return plain (non-value-labelled) character columns as
`factor` instead of `character`. On a string-heavy file (the 2.15 GB AHS file:
2,574 character columns) this is **~3× faster to read, ~32% less memory, and
~11× faster** for downstream `table()`/grouping/joins — because the factor's
integer codes replace per-cell CHARSXP interning. It uses an HLL cardinality
gate, so genuinely high-cardinality columns stay `character`.

```r
df <- read_sas7bdat("survey.sas7bdat", categorical = TRUE)
```

```r
df <- read_sas7bdat("data.sas7bdat", catalog = "formats.sas7bcat")
attr(df$SEX, "labels")   # c(Male = 1, Female = 2)
```

## Build / install

Requires R, and a Rust toolchain (`cargo`, `rustc`).

```sh
R CMD INSTALL --no-staged-install crates/r-plugin
```

The Makevars invokes `cargo build` to produce `libfastsas.a`, which is linked into
`fastsas.so`.

`src/rust/Cargo.toml` names `sas7bdat` **by version**, so a package copied out of
the repository — which is what `R CMD build` does — can resolve it from crates.io.
Inside the repository that is the wrong crate to compile against, because
development runs ahead of the last release, so `src/.cargo/config.toml` redirects
it back to the working tree:

```toml
[patch.crates-io]
sas7bdat = { path = "../../sas7bdat" }
```

`.Rbuildignore` strips `^src/\.cargo$`, so the redirect never reaches a tarball.

Cargo discovers that config by walking up from the **working directory**, not from
`--manifest-path`. Anything invoking cargo for this crate therefore has to run from
`src/` or below — the Makevars does, and so does the CI lint step:

```sh
cd crates/r-plugin/src && cargo clippy --locked --manifest-path rust/Cargo.toml
```

Until the version named in `Cargo.toml` is actually on crates.io, a tarball build
stops at dependency resolution:

```
error: failed to select a version for the requirement `sas7bdat = "^0.7"`
candidate versions found which didn't match: 0.4.0, 0.3.0, 0.2.0, ...
```

The checks that do not compile anything — documentation, `NAMESPACE`,
`DESCRIPTION`, Rd syntax — run regardless, and CI gates on them:

```sh
R CMD build crates/r-plugin
R CMD check --no-manual --no-install fastsas_*.tar.gz   # must report "Status: OK"
```

Once the core is published, drop `--no-install` and the full check compiles too.

## Performance

The core decodes batches across all cores; `read_sas7bdat` pre-allocates each R
vector at the known row count and fills it in place on the main thread as batches
arrive, dropping each one after it is written — so peak memory is the finished R
object plus the scan's bounded in-flight window, not the whole decoded file on
top of both:

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
- **Distribution.** The dependency is wired for it (see "Build / install"), but a
  tarball is not installable until the core version it names is on crates.io. The
  newest published core is 0.4.0; this binding needs 0.7.0, both for APIs the
  0.5/0.6 work introduced and for behaviour still unreleased — the exact SAS format
  tables, the label trim rule, and opting out of temporal decoding to keep
  special-missing tags. Publishing the core is the only remaining step; nothing
  here needs to change when it happens.
- **Special missings on typed temporal buffers.** Tags survive because this
  binding asks the core for undecoded `F64` temporal columns. A caller that
  enables the core's temporal decoding gets `SasDate`/`SasDateTime` values, which
  have already discarded the NaN payload the tag lives in.
