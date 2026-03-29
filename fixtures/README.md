# SAS Test Fixtures

This directory contains fixture datasets used for parser correctness and
benchmarking.

The large binary fixture corpus is intentionally kept out of git. Only this
README stays tracked. Populate the directory locally when running broad parser
validation or benchmarks.

## Source and coverage

- `raw_data/pandas/`: copied from pandas SAS test corpus (BSD-3-Clause)
- Additional corpora: `csharp/`, `other/`, `principlesofeco/`, `readstat/`
- Coverage includes compression variants, date/time fields, value labels, and
  malformed edge cases

## How fixtures are used

- Rust integration snapshots under `crates/sas7bdat/tests/` use these fixtures.
- `tests/fixture_smoke.rs` in this crate auto-discovers local `.sas7bdat` files and runs open/raw/typed/batch smoke coverage, plus stricter assertions for selected representative fixtures when they are present.
  Current pinned regressions include both uncompressed and compressed real files such as `charset_utf8.sas7bdat`, `54-cookie.sas7bdat`, `54-class.sas7bdat`, `test2.sas7bdat`, and `max_sas_date.sas7bdat`.
  The fixture suite also has a dedicated compressed-corpus smoke pass so compressed datasets are exercised as a first-class runtime path.
- `benches/scan_hotpaths.rs` uses a smaller set of local fixtures for repeatable Criterion runs and now includes both the larger compressed `raw_data/ahs2013/topical.sas7bdat` case and the larger uncompressed `raw_data/ahs2013/homimp.sas7bdat` case so the suite is not biased toward tiny files.
- Optional Rust-side external comparisons currently target ReadStat/C++/C#.
- Python (`python/tests/`) and R (`R/tests/`) host pandas/pyreadstat/haven
  comparison checks.

## Large datasets

Large AHS 2019 datasets are not committed. Download the ZIPs listed in
`ahs-links.txt`, extract, and place `.sas7bdat` files under:

- `fixtures/raw_data/ahs2019_metro/`
- `fixtures/raw_data/ahs2019_national/`

## Known excluded broken fixtures

These remain intentionally excluded from parity suites:

- `raw_data/pandas/corrupt.sas7bdat`
- `raw_data/pandas/zero_variables.sas7bdat`
