# Changelog

All notable changes to the `sas7bdat` **library crate** are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Cargo's SemVer rules](https://doc.rust-lang.org/cargo/reference/semver.html).

This file covers the Rust crate only. The Python packages built from this repository,
`sas7bdat-polars` and `sas7bdat-cli`, have their own version numbers, and the plain `v*`
tags (`v0.10.1`, ...) belong to them. This crate's tags are `sas7bdat-v*`.

New entries are drafted from the Conventional Commits history with
[git-cliff](https://git-cliff.org) (`cliff.toml` at the repository root) and edited by
hand.

## [Unreleased]
## [0.9.3] - 2026-10-20

### Fixed

- A file declaring exactly `u32::MAX` rows no longer loses rows past that count. The scan now follows the page headers to the last data page. `DatasetMetadata::row_count` still reports the header value.
- Bytes 0x81, 0x8D, 0x8F, 0x90 and 0x9D in a one-character text column of a compressed file no longer decode to U+FFFD or fail strict validation. They decode as `encoding_rs` does, as in wider columns.

## [0.9.2] - 2026-10-20

### Fixed

- A buffered whole-file scan could deadlock when the buffer pool ran dry. Memory-mapped scans were not affected.

## [0.9.1] - 2026-09-11

### Added

- `Dataset::io_backend` reports how a file's bytes are read: `"mmap"`, `"buffered"` or `"bytes"`.

### Changed

- `Parallelism::Auto` no longer spawns threads for small files.
- A scan with a row limit compiles page descriptors only as far as it reads.

### Fixed

- A declared zero rows-per-page no longer collapses a whole file into one chunk.

## [0.9.0] - 2026-08-31

Four of the fixes below change what a scan returns for files that were previously read wrong. Row counts move for files with deleted rows.

### Fixed

- Deleted rows were returned as live data. They are now skipped, for both uncompressed and compressed pages. **Row counts change for affected files**, for example 37 to 36 and 2097 to 2088 on fixtures in the corpus. A compressed file with deleted rows previously failed to open with "unsupported subheader compression mode 5".
- On little-endian 64-bit files, a numeric value whose low word matched a subheader signature could make an uncompressed row be skipped or fail to parse (WizardMac/ReadStat#369).
- A 64-bit catalog lost every format past its first index page (WizardMac/ReadStat#370).
- Column formats now keep their width and decimal count. `DATETIME22.3` is no longer read as `DATETIME`, and the plain numeric `w.d` format is no longer read as no format (WizardMac/ReadStat#364 and #361). `ColumnMeta::format` still holds the bare name.
- Header timestamps were about twenty years too late (a file written in May 2015 was reported as May 2035). Date and datetime values in the data were not affected.

### Added

- `sas7bdat::VERSION`, the crate's version. The wheel exposes it as `sas7bdat_polars.__core_version__` and the CLI prints it under `--version`.
- `ColumnMeta::format_width`, `ColumnMeta::format_digits` and `ColumnMeta::format_spec()`.
- `ScanStatsSummary::path` reports which decode pipeline a scan took, as a `PageSource` and a `FillStrategy`. `ScanBuilder::predict_path(ScanEntry)` and `ScanBuilder::predict_page_source()` answer the same question without a full scan.

### Changed

- Faster handling of short string cells, and the tiled column-major fill now runs when a string column is present.
- The serial batch scan is gone as a separate pipeline. A single worker runs the parallel scan inline. A path source with one worker still streams rows and reports the new `PageSource::TwoPassRowStream`.

## [0.8.1] - 2026-08-24

### Fixed

- `MojibakePolicy::Auto` (the default) left uppercase accented characters mangled when a file declares a single-byte encoding but holds UTF-8. `KÃ˜BENHAVN` is now repaired to `KØBENHAVN` like its lowercase counterparts.

## [0.8.0] - 2026-08-14

### Changed

- The crate builds on stable Rust (rustc 1.89 or later). The vectorised kernels use [`fearless_simd`](https://crates.io/crates/fearless_simd) with runtime CPU dispatch, and no nightly toolchain is needed.
- The `bstr` and `windows-sys` dependencies are removed.

### Added

- Feature `simd` (default), using `fearless_simd`.
- Feature `nightly-simd`, the previous `std::simd` kernels. Requires nightly and overrides `simd` when both are enabled.
- With neither feature, a scalar backend that needs only `std`.

### Performance

- Dispatch is taken once per column instead of once per 8 rows, and `is_ascii` no longer reduces per chunk.

## [0.7.0] - 2026-08-13

Published as 0.7.0 because 0.5.0 and 0.6.0 were never pushed to crates.io. The registry goes from 0.4.0 to 0.7.0, and their entries below reached users in this release.

### Fixed

- **Breaking (behaviour).** SAS format names are matched exactly instead of by substring in `infer_logical_type`. Columns with user-defined formats such as `FMONTH` or `YEAR_F` are no longer read as dates, and `DATEAMPM` is now a datetime. Formats such as `DAY`, `JULIAN`, `HOUR`, `MMSS` and `E8601DN` are now read as temporal. A built-in missing from the tables is read as `Float`.
- Variable labels are trimmed of trailing ASCII padding only. Leading spaces and a trailing non-breaking space are kept. A label that is only padding is still reported as absent.

### Changed

- `BatchHint::Auto` also caps a batch at 32 MiB of decoded data, which lowers peak memory on wide files. An explicit `BatchHint::Rows` or `Bytes` is unchanged.
- Scans bounded by `RowSelection` or `ScanBuilder::limit` run in parallel and stop early. This makes `n_rows` pushdown in the Polars plugin much faster.
- **Breaking (API).** `IoBackendPreference` has three variants: `Auto`, `Mmap` (was `MmapPreferred`) and `Buffered` (was `BufferedOnly`). `BufferedPreferred` is removed. The enum implements `Display` and `FromStr`.
- **Breaking (API).** `Utf8ValidationMode` has two variants: `Lenient` (was `Auto`) and `Strict`. `Off` is removed.
- `ScanBuilder::collect_batches` on a path source is now parallel and single-pass.
- Owned-batch scans reject `DecodeMode::Raw` at the entry point, so `visit_owned_batches` no longer silently decodes every column as `Binary`.

### Added

- `Error::Internal` (with `InternalError` and `Error::internal`) and `Error::corruption`. Errors reachable from a truncated or hostile file are now `Corruption`, reader bugs are `Internal`, and `Unsupported` is kept for unimplemented layouts and platform limits.
- `ProfileMode` and `ProjectionPreset::as_str` in the `fixture-catalog` module, which has no stability guarantee.

### Removed

- **Breaking (API and CLI).** `ValidationMode`, `OpenOptionsBuilder::validation` and the CLI's `--strict-dates` flag. They had no effect.
- **Breaking (API).** `PrefetchPolicy`, `PageCachePolicy`, `OpenOptionsBuilder::prefetch` and `OpenOptionsBuilder::page_cache`. They had no effect.
- **Breaking (API).** The `RawRowSink`, `RowSink` and `BatchSink` traits and `ScanBuilder::write_raw_rows`, `write_rows` and `write_batches`. Pass a closure to `visit_rows` instead.
- `Utf8Dictionary` and the `dictionary` field of `Utf8Buffer`. `dictionary_ids` is unchanged.

## [0.6.0] - 2026-07-30

### Changed

- **Breaking (Arrow schema).** A SAS `TIME` column is now `Duration(Nanosecond)` instead of `Time64(Nanosecond)`, in the Arrow schema, in Parquet output and in the Polars plugin (`pl.Duration`). Values outside `[0, 24h)` read back as null under `Time64`. Fields carry a `sas.logical_type: TIME` metadata entry (`SAS_LOGICAL_TYPE_KEY`).

### Added

- **(CLI)** `--batch-rows` sets the scan batch size independently of `--parquet-row-group-size`.
- **(CLI)** `--encode-in-flight-bytes` caps the decoded bytes held by row groups being encoded. Defaults to 1 GiB.
- **(CLI)** The conversion summary reports source size and throughput.

### Fixed

- **(CLI)** A single-file conversion with a progress bar printed no summary.
- **(CLI)** A `TIME` value outside `[0, 24h)` printed as `00:00:00` in `head` and CSV export. It now prints its elapsed value, such as `99:48:00`.
- **(CLI)** CSV export wrote raw SAS-epoch numbers for date, datetime and time cells with sub-second values. `head` and CSV now share one formatter.
- **(CLI)** A `TIME` value with a sub-millisecond fraction rendered as `.000`. It now keeps microsecond precision.
- **(CLI)** Converting more than about 2.1 billion rows failed with "Parquet does not support more than 32767 row groups per file". Row groups are now sized to stay inside the limit.
- **(CLI)** `--parquet-row-group-size` no longer sets the scan batch size.

## [0.5.0] - 2026-07-29

Entries marked **(CLI)** describe the `sas7bdat` command, which ships on PyPI under its own version.

### Added

- **(CLI)** Parquet row groups are encoded across cores. `--parse-threads` now sets the thread budget for encoding as well as decoding.
- Progress reporting from the parallel and fused scans, which previously reported nothing.
- `ScanBuilder::with_progress_observer`, taking an already-shared `ScanProgressObserver`.
- **(CLI)** A per-file progress bar with bytes read, rate and ETA.
- Single-pass owned-batch scans of path sources, so the file is read once instead of twice. Applies to `visit_owned_batches` when the source is an unmapped path and the scan covers every row.

### Changed

- **(CLI)** Outputs are written to a temporary file and moved into place when complete. `--tmp-dir` sets where the temporary file goes.
- **(CLI)** Parquet output goes through a 1 MB write buffer.

### Fixed

- A streamed parallel scan of a path source could hang if the consumer stopped early.

## [0.4.0] - 2026-07-28

### Added

- Parallel decode for path sources, which previously had no parallel path.

### Changed

- `Parallelism::Auto` resolves to every logical core. It previously resolved to a single worker.
- `IoBackendPreference::Auto` no longer memory-maps a file on a network share. On Windows, UNC paths and mapped network drives are detected. `MmapPreferred` still maps a remote file.
- Page-descriptor compilation reads larger blocks instead of one syscall per page.
- `LabelSet`, `ValueLabel`, `ValueKey` and `ValueType` derive `Serialize`.

### Fixed

- A read that failed partway through a parallel scan made the scan return fewer rows and report success. The I/O error is now returned.

## [0.3.0] - 2026-07-28

A rewrite. **No part of the 0.2.0 public API survives.** The `dataset`, `parser` and `cell` modules, the `RowSink`, `ColumnarSink` and `RowValue` traits, and every feature flag are replaced. There is no incremental migration path from 0.2.x.

### Added

- Memory-mapped I/O (`memmap2`) with a selectable I/O backend.
- SIMD string decoding (`std::simd` and `simdutf8`) and parallel page scanning with `rayon`. The crate requires a nightly toolchain (`portable_simd`).
- A scan API, `Dataset::scan()` returning a `ScanBuilder`, with column projection, row range selection and batch or row-at-a-time consumption.
- Columnar output types (`ColumnarBatch`, `OwnedColumnBuffer`, `Utf8Buffer`).
- Arrow `RecordBatch` output behind the `arrow` feature.
- String-column dictionary encoding behind the `dictionary` feature.
- `.sas7bcat` catalog support for value labels: `catalog::parse_catalog_file`, `LabelSet`, `ValueLabel`, `ValueKey`.
- `Serialize` on `LabelSet`, `ValueLabel`, `ValueKey` and `ValueType`, and `Debug` on `CatalogLayout`.
- RLE and CHAR compression, and `WINDOWS-1252` and UTF-8 encodings.

### Changed

- `catalog::parse_catalog` now returns a `HeaderError` for a file carrying the dataset magic number, instead of an empty catalog.
- Feature flags are replaced: `chrono`, `cli`, `csv`, `parquet`, `time` and `fast-string` give way to `arrow` and `dictionary`, plus the internal `internal-bench` and `fixture-catalog`.
- `RowSelection` is an enum instead of a struct.
- Dependency minimums were raised to versions that build: `bytemuck` 1.4, `serde` 1.0.130, `encoding_rs` 0.8.11, `ahash` 0.8.7, `cardinality-estimator` 1.0.1.

### Removed

- The `sas7` binary. The command-line tool is now the separate `sas7bdat-cli` package.
- `fixture_catalog` and its exports from the default build. Still available behind the `fixture-catalog` feature.
- `serde_json` as a dependency.

## [0.2.0] and earlier

Released before this changelog was kept; see the git history. 0.2.0 was a
different implementation with a `dataset`/`parser`/`cell` module layout and a bundled
`sas7` binary.

<!-- Crate releases are tagged `sas7bdat-v*`. The bare `v*` tags belong to the Python
     packages (see the note at the top of this file) and are not this crate's versions;
     `v0.7.0` in particular was a wheel release in July, unrelated to 0.7.0 here.
     `sas7bdat-v0.5.0` was never created, so 0.5.0 has no release to link to. -->

[Unreleased]: https://github.com/tkragholm/sas7bdat-parser-rs/compare/sas7bdat-v0.9.3...HEAD
[0.9.3]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.9.3
[0.9.2]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.9.2
[0.9.1]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.9.1
[0.9.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.9.0
[0.8.1]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.8.1
[0.8.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.8.0
[0.7.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.7.0
[0.6.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.6.0
[0.4.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.4.0
[0.3.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.3.0
