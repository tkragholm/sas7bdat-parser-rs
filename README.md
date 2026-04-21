# sas7bdat-cli

Utilities for converting and profiling SAS7BDAT data.

The workspace contains:

- the `sas7bdat-simd` library crate
- the `sas7bdat-cli` package, which ships the `sas7bdat` converter plus profiling binaries
- the `sas7bdat-polars` Python extension package

The package ships command-line executables including:

- `sas7bdat-corpus-catalog`
- `sas7bdat-corpus-profile`
- `sas7bdat-fixture-profile`
- `sas7bdat-fixture-string-profile`

Typical usage after installation:

```text
sas7bdat-corpus-profile <root> --format csv --out corpus_profile.csv
```

## Current progress

The workspace now has a split benchmark for the Python plugin and the raw Rust path, with cold-start and warm steady-state measurements separated.

Recent results on `fixtures/ahs2013n.sas7bdat`:

- `batch_reader` warm steady-state is close to the raw Rust scan path, at about `1.1x` the raw average.
- `scan_sas` warm steady-state is still slower, at about `1.5x` the raw average.
- Cold-start timings are still dominated by dataset open and first-scan setup, which is expected for the lazy descriptor cache.

This means the remaining optimization work is in the Polars/DataFrame conversion and Python handoff path, not the core SAS page scan.

Roadmap: see [docs/ROADMAP.md](/Users/tobiaskragholm/dev/sas7bdat-simd/docs/ROADMAP.md) for the repo-wide plan and [docs/PLUGIN_ROADMAP.md](/Users/tobiaskragholm/dev/sas7bdat-simd/docs/PLUGIN_ROADMAP.md) for the plugin execution plan and benchmark matrix.

Historical notes and superseded reports are kept in [docs/archive/README.md](/Users/tobiaskragholm/dev/sas7bdat-simd/docs/archive/README.md).

For full-scan profiling:

```text
sas7bdat-corpus-profile <root> --mode typed_batches --projection full --out corpus_profile.csv
```

For local development from the workspace root:

```text
cargo run -p sas7bdat-cli --bin sas7bdat-corpus-profile -- <root> --format csv --out corpus_profile.csv
```

## Testing

Use the `just` targets to keep the Rust core and the Python plugin tests separate:

```text
just test-core
just test-polars-plugin-rust
just test-polars-plugin
just test
```

- `just test-core` runs the core Rust workspace tests with `cargo nextest`.
- `just test-polars-plugin-rust` runs the plugin crate's Rust tests with the
  PyO3 extension-module feature disabled.
- `just test-polars-plugin` builds the extension module and runs the Python
  smoke tests.
- `just test` runs the full sequence.

## Standalone CLI

The workspace also ships a small `sas7bdat` conversion CLI in the
`sas7bdat-cli` package.

Examples:

```text
cargo run -p sas7bdat-cli --bin sas7bdat -- convert fixtures/ahs2013n.sas7bdat --sink parquet --out /tmp/out.parquet
cargo run -p sas7bdat-cli --bin sas7bdat -- inspect fixtures/ahs2013n.sas7bdat --json
just sas7bdat convert fixtures/ahs2013n.sas7bdat --sink parquet --out /tmp/out.parquet
```

It supports directory recursion, column projection, row limits, parquet, CSV,
or TSV output, and an optional `.sas7bcat` companion via `--catalog` so
parquet output can carry value-label metadata.

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
