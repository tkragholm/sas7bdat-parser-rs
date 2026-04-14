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

## Current progress

The workspace now has a split benchmark for the Python plugin and the raw Rust path, with cold-start and warm steady-state measurements separated.

Recent results on `fixtures/ahs2013n.sas7bdat`:

- `batch_reader` warm steady-state is close to the raw Rust scan path, at about `1.08x` the raw average.
- `scan_sas` warm steady-state is still slower, at about `1.50x` the raw average.
- Cold-start timings are still dominated by dataset open and first-scan setup, which is expected for the lazy descriptor cache.

This means the remaining optimization work is in the Polars/DataFrame conversion and coalescing path, not the core SAS page scan.

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

| filename | runtime | thrpt | commit-id |
| --- | --- | --- | --- |
| `nysdoh_brfss_surveydata_2018_ad5548ba` | 48.75 ms [48.64 ms; 48.87 ms] | 733.65 Kelem/s [731.85 Kelem/s; 735.34 Kelem/s] | `4ca487e` |
| `nyyts_2000_2018_publicuse_aec3d115` | 154.19 ms [154.05 ms; 154.34 ms] | 764.20 Kelem/s [763.47 Kelem/s; 764.89 Kelem/s] | `4ca487e` |
| `nyyts_2000_2020_publicuse_c85e9144` | 179.69 ms [179.55 ms; 179.84 ms] | 677.43 Kelem/s [676.89 Kelem/s; 677.96 Kelem/s] | `4ca487e` |

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
