# Changelog

All notable changes to the `sas7bdat` **library crate** are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Cargo's SemVer rules](https://doc.rust-lang.org/cargo/reference/semver.html).

This file covers the Rust crate only. The Python packages built from this repository —
`sas7bdat-polars` and `sas7bdat-cli` — carry their own version line, currently 0.5.x, and
git tags (`v0.5.0`, …) track *those*, not this crate. A tag name is not this crate's
version.

Entries from 0.4.0 onward are generated from Conventional Commits with
[git-cliff](https://git-cliff.org) (`cliff.toml` at the repository root); 0.3.0 and
earlier are written by hand.

## [Unreleased]

## [0.3.0] - 2026-07-28

A ground-up rewrite. **No part of the 0.2.0 public API survives**: the `dataset`,
`parser`, and `cell` modules are gone, as are the `RowSink`, `ColumnarSink`, and
`RowValue` traits, and every feature flag has been replaced. `cargo-semver-checks`
counts 13 categories of breaking change against 0.2.0. Treat this as a new library that
happens to share a name — there is no incremental migration path from 0.2.x.

### Added

- Zero-copy memory-mapped I/O (`memmap2`), with a selectable I/O backend.
- SIMD-accelerated string decoding (`std::simd` + `simdutf8`) and parallel page scanning
  via `rayon`. The crate now requires a **nightly** toolchain (`portable_simd`); the
  pinned version is in `rust-toolchain.toml`.
- A scan API — `Dataset::scan()` returning a `ScanBuilder` — with column projection, row
  range selection, and batch or row-at-a-time consumption.
- Columnar output types (`ColumnarBatch`, `OwnedColumnBuffer`, `Utf8Buffer`, …).
- Optional Arrow `RecordBatch` output behind the `arrow` feature.
- Optional string-column dictionary encoding behind the `dictionary` feature.
- Companion `.sas7bcat` catalog support for hydrating value labels, exposed as
  `catalog::parse_catalog_file` plus the `LabelSet` / `ValueLabel` / `ValueKey` types.
- `Serialize` on `LabelSet`, `ValueLabel`, `ValueKey`, and `ValueType`, so a label set
  can be persisted verbatim by downstream tools.
- `Debug` on `CatalogLayout`.
- RLE and CHAR compression support; `WINDOWS-1252` and UTF-8 encodings.

### Changed

- **`catalog::parse_catalog` now rejects a file carrying the sas7bdat dataset magic
  number.** Datasets and catalogs share a header layout, so such a file previously
  decoded into an empty catalog and silently attached no labels at all. It is now a
  `HeaderError`.
- Feature flags are entirely different: 0.2.0's `chrono`, `cli`, `csv`, `parquet`,
  `time`, and `fast-string` are replaced by `arrow`, `dictionary`, plus the internal
  `internal-bench` and `fixture-catalog`.
- `RowSelection` changed from a struct to an enum.
- Dependency floors were raised to versions that actually build: `bytemuck` 1.4,
  `serde` 1.0.130, `encoding_rs` 0.8.11, `ahash` 0.8.7, `cardinality-estimator` 1.0.1.
  The previously declared minimums did not compile when resolved.

### Removed

- The `sas7` binary that shipped inside the crate in 0.2.0. The command-line tool is now
  the separate `sas7bdat-cli` package (`sas7bdat` on PyPI and via `cargo install`).
- `fixture_catalog` and its exports (`FixtureCatalog`, `build_catalog`,
  `discover_fixture_paths`, `profile_fixture`, …) from the default build. This is corpus
  profiling for the in-repo benchmark harness, not reader API — a consumer has no fixture
  corpus to point it at. Still available behind the `fixture-catalog` feature.
- `serde_json` as a dependency. It was only ever used by examples and benchmarks, so
  consumers were compiling it for nothing.

## [0.2.0] and earlier

Released before this changelog was kept; see the git history. 0.2.0 was a
different implementation with a `dataset`/`parser`/`cell` module layout and a bundled
`sas7` binary.

[Unreleased]: https://github.com/tkragholm/sas7bdat-parser-rs/compare/v0.5.0...HEAD
[0.3.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/v0.5.0
