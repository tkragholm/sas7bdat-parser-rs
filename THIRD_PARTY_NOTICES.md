# Third-party notices

This project's own code is MIT-licensed (see [LICENSE](LICENSE)). This file
lists third-party material in this repository and the work the reader builds
on.

## Code derived from ReadStat (MIT)

The first prototype of this reader, written in October 2025, began as a
translation of [ReadStat](https://github.com/WizardMac/ReadStat)'s SAS reader
from C to Rust with [c2rust](https://github.com/immunant/c2rust), and was then
rewritten as Rust within two weeks. The present code base is a second rewrite,
begun in March 2026. It replaced this repository's earlier history, which is
why the commit history starts on 29 March 2026 while crates.io holds releases
from January 2026. In the present code base, the catalog reader
(`crates/sas7bdat/src/catalog.rs`), file probe (`probe.rs`) and encoding table
(`encoding.rs`) descend from the first rewrite. None of the translated code
remains, but because those parts derive from ReadStat, ReadStat's licence and
copyright notice (copyright Evan Miller) are reproduced in
[LICENSES/MIT-ReadStat.txt](LICENSES/MIT-ReadStat.txt).

The code was written with AI coding assistants: OpenAI Codex, Claude Code and
Gemini CLI. The author directed the work and reviewed and tested what they
produced.

## Redistributed test files

### From pandas (BSD-3-Clause)

The four CSV files in `crates/sas7bdat/tests/csv_golden/` (`0x40controlbyte.csv`,
`airline.csv`, `datetime.csv`, `many_columns.csv`) are the expected outputs
from the SAS tests of [pandas](https://github.com/pandas-dev/pandas)
(`pandas/tests/io/sas/data/`), copyright AQR Capital Management, Lambda
Foundry, the PyData Development Team and pandas contributors, licensed under
the BSD 3-Clause License, see
[LICENSES/BSD-3-Clause-pandas.txt](LICENSES/BSD-3-Clause-pandas.txt).
Unmodified.

The following files in `crates/sas7bdat/tests/fixtures/corpus/pandas/` are
copied from the same directory of pandas, at commit `b650df099160`, under the
same licence. Unmodified.

- `0x40controlbyte.sas7bdat`
- `airline.sas7bdat`
- `datetime.sas7bdat`
- `many_columns.sas7bdat`
- `test10.sas7bdat`
- `test13.sas7bdat`
- `test14.sas7bdat`
- `test15.sas7bdat`
- `test16.sas7bdat`
- `test3.sas7bdat`
- `zero_rows.sas7bdat`

### From pyreadstat (Apache-2.0)

The following files in `crates/r-plugin/inst/extdata/` are copied from the
test corpus of [pyreadstat](https://github.com/Roche/pyreadstat)
(copyright Hoffmann-La Roche and pyreadstat contributors), licensed under the
Apache License 2.0, see [LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt):

- `missing_test.sas7bdat` (from `test_data/missing_data/`)
- `missing_formats.sas7bcat` (from `test_data/missing_data/`)
- `test_data_win.sas7bdat` (from `test_data/sas_catalog/`)
- `test_formats_win.sas7bcat` (from `test_data/sas_catalog/`)

Unmodified.

### From Parso (Apache-2.0)

`crates/r-plugin/inst/extdata/dtdate.sas7bdat` is
`src/test/resources/dates/sas/date_format_dtdate.sas7bdat` from
[Parso](https://github.com/epam/parso) (copyright EPAM Systems), licensed
under the Apache License 2.0, see
[LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt). Unmodified.

The following files in `crates/sas7bdat/tests/fixtures/corpus/parso/` are
copied from Parso's `src/test/resources/`, at commit `3c514e66264f`, under the
same licence. Unmodified. Parso has no NOTICE file.

- `54-cookie.sas7bdat` (from `bugs/54-cookie.sas7bdat`)
- `all_rand_normal.sas7bdat` (from `sas7bdat/all_rand_normal.sas7bdat`)
- `all_rand_normal_with_deleted.sas7bdat` (from `sas7bdat/all_rand_normal_with_deleted.sas7bdat`)
- `all_rand_normal_with_deleted2.sas7bdat` (from `sas7bdat/all_rand_normal_with_deleted2.sas7bdat`)
- `charset_utf8.sas7bdat` (from `sas7bdat/charset_utf8.sas7bdat`)
- `chinese_column_works.sas7bdat` (from `sas7bdat/chinese_column_works.sas7bdat`)
- `comp_deleted.sas7bdat` (from `sas7bdat/comp_deleted.sas7bdat`)
- `data_page_with_deleted.sas7bdat` (from `sas7bdat/data_page_with_deleted.sas7bdat`)
- `date_formats.sas7bdat` (from `sas7bdat/date_formats.sas7bdat`)
- `mix_and_missing.sas7bdat` (from `sas7bdat/mix_and_missing.sas7bdat`)
- `time_formats.sas7bdat` (from `sas7bdat/time_formats.sas7bdat`)

### `people.sas7bdat`

- `crates/r-plugin/inst/extdata/people.sas7bdat` is a small synthetic teaching
  dataset (5 rows, columns `ID` and `GENDER`) that has circulated in public
  SAS7BDAT sample-file collections since at least 2014; the earliest known
  source is Louisiana State University statistics course material (EXST7087,
  Summer 2006). It contains no real-world data.
- `crates/sas7bdat/tests/fixtures/people_nonascii.sas7bdat` is a copy of it
  with one byte changed (see `crates/sas7bdat/tests/fixtures/README.md`).

### Untracked local corpus

The fixture corpus under `fixtures/` is not tracked in git and is not
redistributed, apart from the files listed above under pandas and Parso. Parts
of it come from the pandas SAS test corpus (BSD-3-Clause) and other public
sample-file collections; see `fixtures/README.md`.

## References and correctness oracles

The format knowledge draws on Matt Shotwell's
["SAS7BDAT Database Binary Format"](https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf)
and on the readers listed under "Related work" in the README. Decoded output
is compared with the command-line tools and public APIs of:

- [ReadStat](https://github.com/WizardMac/ReadStat) by Evan Miller (MIT)
- [pyreadstat](https://github.com/Roche/pyreadstat) (Apache-2.0)
- [haven](https://haven.tidyverse.org/) (MIT)

## Trademark

SAS and all other SAS Institute Inc. product or service names are registered
trademarks or trademarks of SAS Institute Inc. in the USA and other countries.
This project is not affiliated with, sponsored, or endorsed by SAS Institute
Inc. The terms "SAS" and "sas7bdat" are used only to describe file-format
compatibility.
