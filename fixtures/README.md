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
- `src/bin/corpus_profile.rs` is the server-oriented version of the catalog workflow. It walks arbitrary input roots, emits one JSON report for the whole corpus, and adds aggregate summaries such as compression counts, encoding counts, tag counts, and top files by size, rows, columns, and string columns.
- `src/bin/fixture_profile.rs` runs one fixture in a selected scan mode (`raw_rows`, `typed_rows`, `typed_batches`, etc.) and emits structured timing plus parser stats. This is intended to be wrapped by established OS tools for memory and CPU inspection.
- `src/bin/fixture_string_profile.rs` samples string columns for one fixture and reports width buckets plus the densest and emptiest string columns. This is useful when deciding whether a large string workload is dominated by dense identifiers, low-cardinality categoricals, or mostly-empty fixed-width columns.
- In practice, `fixture_profile` is now the preferred tool for large file-backed workload decisions, backend comparisons, RSS checks, and one-off fixture studies. Criterion remains the primary tool for curated, repeatable hot-path suites over the pinned benchmark set.
- The root `justfile` is the main entrypoint for this workflow:
  - `just catalog` generates `fixtures/fixture_catalog.local.json`
  - `just profile ...` runs a structured profile
  - `just string-profile ...` reports width buckets and dense/empty string columns for one fixture
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

Use the tools differently:

- Use `corpus_profile` when the question is “what does this whole SAS7BDAT corpus look like?”
- Use `just profile ...` when the question is fixture-specific, backend-specific, or memory-related.
- Use Criterion benches when the question is whether a code change improved a pinned hot-path family in a repeatable way.
- Prefer `fixture_profile` for very large file-backed datasets like `ahs2013n.sas7bdat`, especially when comparing `mmap-preferred` versus `buffered-only`.
- Prefer Criterion for curated regression families such as projected `topical`, compressed matrix runs, and backend matrix runs.

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

- `cargo run --release --bin corpus_profile -- /data/sas7bdat --sample-rows 512 --out corpus-profile.json`
- `just bench-standard full 2`
- `just bench-compressed full 3`
- `just bench-string-heavy strings 2`
- `CRITERION_ARGS='--sample-size 10 --warm-up-time 0.1 --measurement-time 0.1' just bench-standard full 1`

## Wheel Packaging

The repository now includes a minimal [`/Users/tobiaskragholm/dev/sas7bdat-simd/pyproject.toml`](/Users/tobiaskragholm/dev/sas7bdat-simd/pyproject.toml) configured for `maturin` `bin` bindings.

That means a command like:

- `uvx maturin build --release --target x86_64-pc-windows-msvc --verbose`

builds a wheel that installs the Rust binaries as Python package scripts. The new `corpus_profile` binary is intended to be the main server-facing entrypoint for large real corpora.

The intended interpretation of the fixture tags is:

- `correctness-only`: useful for parser validation and narrow regressions, not representative throughput work
- `benchmark-standard`: normal benchmark candidates
- `benchmark-macro`: very large or ultra-wide datasets that should not run in every quick benchmark pass
- `compressed` / `uncompressed`: compression-family selection
- `string-heavy` / `numeric-heavy` / `mixed`: content-family selection

## Benchmark decision policy

Criterion runs in this repository are now treated as two different tools:

1. Fast screening runs.
2. Decision-grade confirmation runs.

Fast screening runs are for iteration only. They are useful when:

- rejecting obvious losers quickly
- checking that a change did not catastrophically break a benchmark family
- narrowing down where to spend effort next

Typical fast screening settings are short, for example:

- `--sample-size 10`
- `--warm-up-time 0.1`
- `--measurement-time 0.1`

These runs are not considered strong enough on their own for commit or revert
decisions when the measured effect is small or medium.

Decision-grade confirmation runs are required before concluding that a change is
worth keeping or backing out when the effect is not overwhelmingly large.

Typical confirmation settings are longer, for example:

- `--sample-size 20`
- `--warm-up-time 0.5`
- `--measurement-time 1`

Current policy:

- do not commit or revert based only on a fast screening run when the measured effect is roughly under `10%`
- rerun the relevant benchmark family with the longer confirmation profile before deciding
- confirm against the dedicated family benchmark and at least one guardrail benchmark
- when a decision depends on a specific large file-backed workload or I/O backend, confirm it with `fixture_profile` as well instead of relying on in-memory Criterion runs alone

For example:

- string-heavy work must be checked against `fixture_topical_projection_strings`
- mixed-path work must be checked against `fixture_topical_projection`
- numeric guardrails should still be checked against `fixture_topical_projection_numeric`
- large uncompressed file-backed tradeoffs should be checked against `ahs2013n.sas7bdat` with `fixture_profile`

The practical interpretation is:

- short runs are for steering
- long runs are for decisions
- `fixture_profile` is for large targeted workload truth

## Large datasets

Large AHS 2019 datasets are not committed. Download the ZIPs listed in
`ahs-links.txt`, extract, and place `.sas7bdat` files under:

- `fixtures/raw_data/ahs2019_metro/`
- `fixtures/raw_data/ahs2019_national/`

## Known excluded broken fixtures

These remain intentionally excluded from parity suites:

- `raw_data/pandas/corrupt.sas7bdat`
- `raw_data/pandas/zero_variables.sas7bdat`
