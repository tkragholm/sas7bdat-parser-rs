# Third-party notices

This project's own code is MIT-licensed (see [LICENSE](LICENSE)). This file
documents third-party material that is redistributed in this repository —
currently only small binary test fixtures — and credits the projects this
parser was validated against.

No third-party source code is vendored in this repository. The parser is an
independent implementation; see the "Related work" section of the README.

## Redistributed test fixtures

### From pyreadstat (Apache-2.0)

The following files in `crates/r-plugin/inst/extdata/` are copied from the
test corpus of [pyreadstat](https://github.com/Roche/pyreadstat)
(copyright Hoffmann-La Roche and pyreadstat contributors), licensed under the
Apache License 2.0 — see [LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt):

- `missing_test.sas7bdat` (from `test_data/missing_data/`)
- `missing_formats.sas7bcat` (from `test_data/missing_data/`)
- `test_data_win.sas7bdat` (from `test_data/sas_catalog/`)
- `test_formats_win.sas7bcat` (from `test_data/sas_catalog/`)

The files are unmodified.

### From Parso (Apache-2.0)

- `crates/r-plugin/inst/extdata/dtdate.sas7bdat` is
  `src/test/resources/dates/sas/date_format_dtdate.sas7bdat` from
  [Parso](https://github.com/epam/parso) (copyright EPAM Systems), licensed
  under the Apache License 2.0 — see
  [LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt). Unmodified.

### `people.sas7bdat`

- `crates/r-plugin/inst/extdata/people.sas7bdat` is a tiny synthetic teaching
  dataset (5 rows, two columns: `ID`, `GENDER`) that has circulated in public
  SAS7BDAT sample-file collections since at least 2014; the earliest known
  source is Louisiana State University statistics course material
  (EXST7087, Summer 2006). It contains no real-world or creative data.
- `crates/sas7bdat/tests/fixtures/people_nonascii.sas7bdat` is a single-byte
  patched copy of the above (see `crates/sas7bdat/tests/fixtures/README.md`).

### Untracked local corpora

The large fixture corpus under `fixtures/` is intentionally **not** tracked in
git and is not redistributed with this repository. Parts of it originate from
the [pandas](https://github.com/pandas-dev/pandas) SAS test corpus
(BSD-3-Clause) and other public sample-file collections; see
`fixtures/README.md`.

## Correctness references (no code copied)

Correctness was validated by comparing output against independent
implementations, used strictly as external oracles (via their command-line
tools and public APIs):

- [ReadStat](https://github.com/WizardMac/ReadStat) by Evan Miller (MIT)
- [pyreadstat](https://github.com/Roche/pyreadstat) (Apache-2.0)
- [haven](https://haven.tidyverse.org/) (MIT)

Understanding of the SAS7BDAT binary layout draws on the public
reverse-engineering literature, in particular Matt Shotwell's
["SAS7BDAT Database Binary Format"](https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf)
and the projects listed under "Related work" in the README.

## Trademark

SAS and all other SAS Institute Inc. product or service names are registered
trademarks or trademarks of SAS Institute Inc. in the USA and other countries.
This project is an independent open-source effort and is not affiliated with,
sponsored, or endorsed by SAS Institute Inc. The terms "SAS" and "sas7bdat"
are used solely to describe file-format compatibility.
