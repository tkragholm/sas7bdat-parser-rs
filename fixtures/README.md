# SAS Test Fixtures

This directory vendors a subset of the SAS7BDAT files distributed with
[`pandas`](https://github.com/pandas-dev/pandas) under the BSD-3-Clause license (`pandas/LICENSE`). The fixtures provide coverage for variations the Rust reader must support: 32/64-bit headers, multiple compression modes, valuelabels, date/time fields, and corner cases such as zero-row datasets.

Each `.sas7bdat` file in `raw_data/pandas/` is copied verbatim from
`pandas/pandas/tests/io/sas/data/`. Use them for integration tests and for comparing parser output against pandas’ reference behavior. The `raw_data` tree also houses additional corpora under `csharp/`, `other/`, `principlesofeco/`, and `readstat/` for bespoke edge cases. Large archives are ignored by `.gitignore` but are required for the full regression suite.

## Additional fixtures

The `other/` directory contains extra SAS datasets that exercise edge cases not covered by the pandas corpus.

The integration suite automatically walks every `.sas7bdat` file in `fixtures/raw_data/` using
the `datatest-stable` harness in `crates/sas7bdat/tests/fixtures_snapshot_*.rs`. Rust snapshots
are compared against external parsers at runtime: ReadStat (CLI), the C++ parser, and the C#
parser are invoked directly; pandas/pyreadstat/haven snapshots are generated into temp
directories via `scripts/snapshots/`. Reports are written to `target/sas7bdat-reports` when the
corresponding `SAS7BDAT_VERIFY_*` flag is set.

Large AHS 2019 datasets are not checked into git. Download the ZIPs listed in
`ahs-links.txt`, extract them, and place the `.sas7bdat` files under:

- `fixtures/raw_data/ahs2019_metro/`
- `fixtures/raw_data/ahs2019_national/`

The intentionally broken fixtures `pandas/corrupt.sas7bdat` and
`pandas/zero_variables.sas7bdat` remain excluded from the suite because neither pyreadstat nor
the Rust parser can decode them reliably.
