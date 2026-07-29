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

This file covers the library crate. Two entries below describe the `sas7bdat` CLI, which
shares this repository but ships on PyPI under its own version.

### Added

- **(CLI)** Parquet row groups are encoded across cores instead of on the thread that reads
  them. With the input cached, so decode and I/O are as cheap as they get, that single
  thread was 80-90% of a conversion. Row groups are the axis that scales here — production
  SAS files often carry 10 columns and sometimes 3, so splitting by column alone would
  leave most of a large host idle. Several row groups encode at once and are written in
  file order; their columns are encoded in parallel too, which is what covers tables wide
  enough that one row group already fills the pool. Isolated on 12 cores, zstd-3:

  | columns | serial | parallel | |
  |---|---|---|---|
  | 3 | 177 ms | 26 ms | 6.8× |
  | 10 | 611 ms | 82 ms | 7.4× |
  | 28 | 1.85 s | 246 ms | 7.5× |

  `--parse-threads` now sets the thread budget for encoding as well as decoding.
- Single-pass owned-batch scans of path sources. Page descriptors are now compiled from
  the same 4 MB extents that feed decode, so the file is read once rather than twice —
  once for the descriptor table, once for the rows. `row_base` is a running total, so a
  sequential stage sits between the concurrent readers and the decode pool, reordering
  extents and carrying the row index forward. The descriptor table is never built, which
  also drops its memory: a compressed dataset previously held one row span per row for the
  whole scan.

  Fusion applies to `visit_owned_batches` (and so to the CLI's parquet export and the
  Polars plugin) when the source is an unmapped path, the scan covers every row, and the
  descriptor table isn't already cached. Everything else keeps the two-pass path.

### Changed

- **(CLI)** Parquet output goes through a 1 MB buffer. parquet-rs wraps the sink in a
  `BufWriter` of its own, but at the 8 KiB default, which a column chunk writes straight
  past — so each one was its own round trip to a network share.

### Fixed

- A streamed parallel scan of a path source could hang if the consumer stopped early. The
  scan held a receiver open past the point its workers had exited, leaving a reader parked
  on a send that would never complete.

## [0.4.0] - 2026-07-28

Throughput work for large files on network storage, driven by measurements against SMB
(`scripts/probe.py` reproduces them). On a 104 GB file over a 341 MB/s link, a
conversion went from ~37 minutes to ~10.

### Added

- Parallel decode for path sources. Pages stream as 4 MB extents read by up to 4
  concurrent readers, each with its own file handle. Previously a path source had no
  parallel path at all: it fell back to a serial scan issuing one `seek` + `read_exact`
  per page.

### Changed

- `Parallelism::Auto` resolves to every logical core. It previously resolved to a single
  worker, so serial decode was the default and callers had to pass
  `Parallelism::Threads(n)` to get any parallelism.
- `IoBackendPreference::Auto` no longer memory-maps a file on a network share. Mapping a
  remote file turns each access into a round-trip with no readahead. On Windows, UNC paths
  and mapped network drives are detected; other platforms are treated as local.
  `MmapPreferred` still maps a remote file.
- Page-descriptor compilation reads ~4 MB blocks instead of one syscall per page. The pass
  runs before any row is decoded, and a 100 GB file holds on the order of a million pages.
- `LabelSet`, `ValueLabel`, `ValueKey` and `ValueType` derive `Serialize`.

### Fixed

- A read that failed partway through a parallel scan closed its channel, which the decoders
  saw as end of input: the scan returned fewer rows and reported success. The I/O error is
  now returned.


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
[0.3.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.3.0
