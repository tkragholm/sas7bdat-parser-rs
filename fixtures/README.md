# SAS Test Fixtures

This directory vendors a subset of the SAS7BDAT files distributed with
[`pandas`](https://github.com/pandas-dev/pandas) under the BSD-3-Clause license
(`pandas/LICENSE`). The fixtures provide coverage for variations the Rust
reader must support: 32/64-bit headers, multiple compression modes, value
labels, date/time fields, and corner cases such as zero-row datasets.

Each `.sas7bdat` file in `raw_data/pandas/` is copied verbatim from
`pandas/pandas/tests/io/sas/data/`. Use them for integration tests and for
comparing parser output against pandas’ reference behavior. The `raw_data`
tree also houses additional corpora under `csharp/`, `other/`, `principlesofeco/`,
and `readstat/` for bespoke edge cases.

## Additional fixtures

The `other/` directory contains extra SAS datasets that exercise edge cases not
covered by the pandas corpus.

The integration suite automatically walks every `.sas7bdat` file in
`fixtures/raw_data/` using the `datatest-stable` harness defined in
`tests/fixtures_snapshot.rs`. Parser results are captured with `insta`
snapshots under `tests/snapshots/`. When fixtures change or new datasets are
added:

1. Regenerate snapshots with `cargo insta test` (or `INSTA_UPDATE=always cargo test`).
2. Review and accept updates via `cargo insta review`.

The intentionally broken fixtures `pandas/corrupt.sas7bdat` and
`pandas/zero_variables.sas7bdat` remain excluded from the snapshots because
neither pyreadstat nor the Rust parser can decode them reliably.
