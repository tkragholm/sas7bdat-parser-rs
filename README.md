# sas7bdat-profiler

Utilities for profiling SAS7BDAT corpora and scan behavior.

The workspace contains:

- the `sas7bdat-simd` library crate
- the `sas7bdat-profiler` binary package

The profiler package ships Windows command-line executables including:

- `corpus_profile`
- `fixture_catalog`
- `fixture_profile`
- `fixture_string_profile`

Typical usage after installation:

```text
corpus_profile <root> --format csv --out corpus_profile.csv
```

For full-scan profiling:

```text
corpus_profile <root> --mode typed_batches --projection full --out corpus_profile.csv
```

For local development from the workspace root:

```text
cargo run -p sas7bdat-profiler --bin corpus_profile -- <root> --format csv --out corpus_profile.csv
```

## Production corpus target profile

Based on [`Transfer_708245_310326/corpus_profile_nosizebytes.csv`](Transfer_708245_310326/corpus_profile_nosizebytes.csv) and local `corpus_*.csv` analysis:

- Files discovered: `1242`
- Profiled: `1019`
- Historical failures: `223` (dominant classes were RLE compression decode errors)
- Encoding on all profiled production files: `WINDOWS-1252` (`legacy`)
- Compression mix (profiled files): `554` uncompressed, `465` compressed
- Row-weighted workload mix: `53.45%` uncompressed, `46.55%` compressed
- Row-weighted content mix: `49.68%` string-heavy, `41.26%` mixed, `9.06%` numeric-heavy
- Row-weighted width mix: `41.62%` medium, `39.33%` narrow, `19.05%` wide
- Row-weighted size mix: `74.54%` huge

Throughput from the production corpus profiling run (profiled files only):

- Uncompressed: `~9.71M` rows/s
- Compressed: `~3.33M` rows/s

This gap means compressed-path performance remains the highest-impact optimization target.

Current failed-only re-run status from [`corpus_failed_only.csv`](corpus_failed_only.csv):

- Previously failing files re-run: `223`
- Now profiling successfully: `214`
- Remaining failures: `9`
- Remaining errors are all unsupported subheader compression modes (`79`, `88`, `89`, `105`, `118`, `167`)

Optimization priority for this project:

1. Compressed `WINDOWS-1252` string-heavy and mixed huge files.
2. Medium/narrow width hot paths first, with wide-path support kept performant.
3. Correctness support for the remaining compression modes (last 9 files).
4. Uncompressed macro-fixtures (for example `ahs2013n`) kept as secondary validation targets.

## Top3 target benchmark snapshot

Command used:

```text
cargo bench --bench compression_matrix -- 'top3_target/'
```

<!-- TOP3_BENCH_TABLE:START -->

| Fixture | raw_rows time | raw_rows throughput | typed_batches time | typed_batches throughput | Notes |
| --- | --- | --- | --- | --- | --- |
| `top3_target/healthdatany_86he_eqwq/windows_1252/nysdoh_brfss_surveydata_2018_ad5548ba` | [32.11 µs 32.19 µs 32.27 µs] | [1.11 Gelem/s 1.11 Gelem/s 1.11 Gelem/s] | [49.62 ms 49.65 ms 49.67 ms] | [720.07 Kelem/s 720.41 Kelem/s 720.76 Kelem/s] | auto-generated from `target/criterion/*/new/estimates.json` |
| `top3_target/healthdatany_pbq7_ddg9/windows_1252/nyyts_2000_2018_publicuse_aec3d115` | [99.10 µs 99.19 µs 99.30 µs] | [1.19 Gelem/s 1.19 Gelem/s 1.19 Gelem/s] | [419.67 ms 419.94 ms 420.27 ms] | [280.38 Kelem/s 280.60 Kelem/s 280.78 Kelem/s] | auto-generated from `target/criterion/*/new/estimates.json` |
| `top3_target/healthdatany_pbq7_ddg9/windows_1252/nyyts_2000_2020_publicuse_c85e9144` | [122.80 µs 122.89 µs 123.00 µs] | [989.66 Melem/s 990.54 Melem/s 991.27 Melem/s] | [493.54 ms 493.87 ms 494.26 ms] | [246.29 Kelem/s 246.48 Kelem/s 246.65 Kelem/s] | auto-generated from `target/criterion/*/new/estimates.json` |

<!-- TOP3_BENCH_TABLE:END -->
