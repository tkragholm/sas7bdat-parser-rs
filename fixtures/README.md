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
- `src/bin/fixture_catalog.rs` builds a local JSON catalog of the available fixture corpus, including the AHS datasets plus the `csharp`, `other`, `pandas`, `principlesofeco`, and `readstat` subcorpora. The catalog records metadata, sampled content features, and derived tags such as `compressed`, `string-heavy`, `benchmark-standard`, or `benchmark-macro`.
- `src/bin/fixture_profile.rs` runs one fixture in a selected scan mode (`raw_rows`, `typed_rows`, `typed_batches`, etc.) and emits structured timing plus parser stats. This is intended to be wrapped by established OS tools for memory and CPU inspection.
- The root `justfile` is the main entrypoint for this workflow:
  - `just catalog` generates `fixtures/fixture_catalog.local.json`
  - `just profile ...` runs a structured profile
  - `just profile-rss ...` wraps the run with `/usr/bin/time -l` for peak RSS
  - `just profile-sample ...` wraps the run with macOS `sample`
  - `just profile-leaks ...` wraps the run with macOS `leaks`
- Optional Rust-side external comparisons currently target ReadStat/C++/C#.
- Python (`python/tests/`) and R (`R/tests/`) host pandas/pyreadstat/haven
  comparison checks.

## Benchmark workflow

The fixture workflow is intended to make benchmark selection explicit instead of
implicitly relying on a few hand-picked files.

Recommended procedure:

1. Refresh the local fixture catalog:
   - `just catalog`
2. Inspect or diff the catalog if needed:
   - `just catalog-stdout`
3. Run a structured single-fixture profile:
   - `just profile fixtures/raw_data/ahs2013/topical.sas7bdat typed_batches mixed 1 128`
4. Add peak RSS:
   - `just profile-rss fixtures/raw_data/ahs2013/topical.sas7bdat typed_batches mixed 1 128`
5. Use tag-driven Criterion runs for fixture families:
   - `just bench-standard`
   - `just bench-compressed`
   - `just bench-string-heavy`
   - `just bench-numeric-heavy`
   - `just bench-macro`

The common tuning knobs are recipe arguments, not environment variables:

- projection
- max fixture count
- sample rows
- repeat
- row limit
- batch size

The one remaining environment-based escape hatch is `CRITERION_ARGS`, because
Criterion accepts a free-form set of pass-through flags and it is pragmatic to
leave that as shell-style argument passthrough.

Examples:

- `just bench-standard full 2`
- `just bench-compressed full 3`
- `just bench-string-heavy strings 2`
- `CRITERION_ARGS='--sample-size 10 --warm-up-time 0.1 --measurement-time 0.1' just bench-standard full 1`

The intended interpretation of the fixture tags is:

- `correctness-only`: useful for parser validation and narrow regressions, not representative throughput work
- `benchmark-standard`: normal benchmark candidates
- `benchmark-macro`: very large or ultra-wide datasets that should not run in every quick benchmark pass
- `compressed` / `uncompressed`: compression-family selection
- `string-heavy` / `numeric-heavy` / `mixed`: content-family selection

## Large datasets

Large AHS 2019 datasets are not committed. Download the ZIPs listed in
`ahs-links.txt`, extract, and place `.sas7bdat` files under:

- `fixtures/raw_data/ahs2019_metro/`
- `fixtures/raw_data/ahs2019_national/`

## Known excluded broken fixtures

These remain intentionally excluded from parity suites:

- `raw_data/pandas/corrupt.sas7bdat`
- `raw_data/pandas/zero_variables.sas7bdat`
