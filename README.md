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
| `top3_target/healthdatany_86he_eqwq/windows_1252/nysdoh_brfss_surveydata_2018_ad5548ba` | [32.58 µs 32.64 µs 32.69 µs] | [1.09 Gelem/s 1.10 Gelem/s 1.10 Gelem/s] | [50.18 ms 50.25 ms 50.32 ms] | [710.73 Kelem/s 711.80 Kelem/s 712.76 Kelem/s] | auto-generated from `target/criterion/*/new/estimates.json` |
| `top3_target/healthdatany_pbq7_ddg9/windows_1252/nyyts_2000_2018_publicuse_aec3d115` | [100.26 µs 100.32 µs 100.38 µs] | [1.17 Gelem/s 1.17 Gelem/s 1.18 Gelem/s] | [159.75 ms 159.86 ms 159.98 ms] | [736.55 Kelem/s 737.12 Kelem/s 737.62 Kelem/s] | auto-generated from `target/criterion/*/new/estimates.json` |
| `top3_target/healthdatany_pbq7_ddg9/windows_1252/nyyts_2000_2020_publicuse_c85e9144` | [123.74 µs 123.82 µs 123.90 µs] | [982.51 Melem/s 983.14 Melem/s 983.73 Melem/s] | [183.41 ms 183.64 ms 183.93 ms] | [661.82 Kelem/s 662.87 Kelem/s 663.69 Kelem/s] | auto-generated from `target/criterion/*/new/estimates.json` |

<!-- TOP3_BENCH_TABLE:END -->

## Typed-batches hotpath profiling

Store hotpath output next to Criterion artifacts:

```text
just hotpath-typed-batches-target
```

This writes a JSON profile to:

```text
target/criterion/hotpath/typed_batches_target.json
```

Override defaults with environment variables, for example:

```text
MAX_FILES=3 BATCH_ROWS=256 HOTPATH_OUTPUT_PATH=target/criterion/hotpath/custom.json just hotpath-typed-batches-target
```

For a quick pass over only the 3 largest target files:

```text
just hotpath-typed-batches-top3
```
