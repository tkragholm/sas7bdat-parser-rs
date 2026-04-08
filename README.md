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
