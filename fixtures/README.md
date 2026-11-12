# Test fixtures

The fixture corpus is not tracked in git; only this README is. Populate the
directory locally to run the corpus tests, the oracle comparison and the
benchmarks. Tests that need a missing file skip with a message.

## Contents

- `raw_data/pandas/`: the pandas SAS test files (BSD-3-Clause).
- `raw_data/csharp/`, `raw_data/other/`, `raw_data/principlesofeco/`,
  `raw_data/readstat/`, `raw_data/readstat_issues/`: test files from other
  readers and public sample collections.
- `raw_data/ahs2013/`: files from the US Census American Housing Survey 2013.
- `ahs2013n.sas7bdat` (2.15 GB, AHS 2013 national file): used by most Polars
  plugin tests and by the benchmarks. It is public Census data and can be
  downloaded again. No other file has its `DEGREE` and `LMED` columns.

`raw_data/pandas/corrupt.sas7bdat` and `zero_variables.sas7bdat` are malformed
on purpose and are left out of the parity tests.

## What uses the corpus

- `crates/sas7bdat/tests/`: smoke tests over every file found, pinned
  regressions on selected files, parity between parallel and serial decoding,
  and golden tests against the pandas CSVs in `tests/csv_golden/`.
- `cargo run -p sas7bdat --release --example verify_readstat`: compares every
  cell with the `readstat` command-line tool on `PATH`. Use a ReadStat build
  from its main branch; the 1.1.9 release returns deleted rows.
- `crates/sas7bdat/benches/scan_hotpaths.rs`: Criterion benchmarks over a fixed
  set of files.
- Profiling tools in `sas7bdat-cli`, built with `--features dev-tools`:
  `sas7bdat-corpus-catalog` and `sas7bdat-corpus-profile` describe a whole
  corpus; `sas7bdat-fixture-profile` and `sas7bdat-fixture-string-profile`
  time and describe one file.

## Benchmark workflow

```sh
just catalog                 # write fixtures/fixture_catalog.local.json
just profile <file> typed_batches mixed 1 128
just profile-rss <file> typed_batches mixed 1 128    # adds peak RSS (macOS)
just bench-standard          # Criterion, by catalog tag
just bench-compressed
just bench-string-heavy
```

Catalog tags pick the files for each family: `benchmark-standard`,
`benchmark-macro` (very large or very wide), `compressed`, `string-heavy`,
`numeric-heavy`, `mixed`, and `correctness-only`. Extra Criterion flags go in
`CRITERION_ARGS`.

## Benchmark decision policy

Short Criterion runs (`--sample-size 10 --warm-up-time 0.1
--measurement-time 0.1`) are for steering. Before keeping or reverting a change
whose effect is under about 10%, rerun the family with the longer profile
(`--sample-size 20 --warm-up-time 0.5 --measurement-time 1`), check at least
one guardrail benchmark, and confirm file-backed or I/O-backend effects with
`sas7bdat-fixture-profile` on a large file such as `ahs2013n.sas7bdat`.

| Change touches | Check against |
|---|---|
| string decoding | `fixture_topical_projection_strings` |
| mixed paths | `fixture_topical_projection` |
| numeric decoding | `fixture_topical_projection_numeric` |
| large uncompressed files | `ahs2013n.sas7bdat` with `sas7bdat-fixture-profile` |
