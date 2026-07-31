# Changelog

All notable changes to the `sas7bdat` **library crate** are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Cargo's SemVer rules](https://doc.rust-lang.org/cargo/reference/semver.html).

This file covers the Rust crate only. The Python packages built from this repository —
`sas7bdat-polars` and `sas7bdat-cli` — carry their own version line, currently 0.8.x, and
git tags (`v0.8.0`, …) track *those*, not this crate. A tag name is not this crate's
version.

Entries from 0.4.0 onward are generated from Conventional Commits with
[git-cliff](https://git-cliff.org) (`cliff.toml` at the repository root); 0.3.0 and
earlier are written by hand.

## [Unreleased]

### Removed

- **Breaking (API).** `PrefetchPolicy` and `PageCachePolicy`, along with the
  `OpenOptionsBuilder::prefetch` and `::page_cache` setters. Both were stored on `OpenOptions`
  and read by nothing — they never influenced a single read.
- **Breaking (API).** The `RawRowSink`, `RowSink` and `BatchSink` traits and the
  `ScanBuilder::write_raw_rows`, `::write_rows` and `::write_batches` methods that drove them.
  They were one-line wrappers around the corresponding `visit_*` closures, and nothing in the
  workspace — no crate, test, example or benchmark — ever implemented or called them. Callers
  wanting a push sink can pass `|row| sink.push(row)` to `visit_rows` directly.
- The `direct_numeric` batch decode family, which was unreachable. Every numeric decode kernel
  compiles a `NumericTileMode`, and `compile_batch_column_families` routes any column with a
  tile mode to `staged_numeric` before the family match runs, so no column could ever land in
  it. Verified by replacing its selection arm with a panic: the full test suite and a decode of
  all 387 corpus fixtures never reached it. Removing it also retires the five
  `OwnedBatchColumnBuilder::append_*_fast` methods it was the only caller of, and drops one
  branch from the per-row family dispatch. The internal `batch_direct_numeric_cells` scan
  counter is gone with it; `ScanStatsSummary` is unaffected.

Behaviour is unchanged: converting the corpus with the previous release and with this build
produces byte-identical Parquet and CSV output across all 363 files.

## [0.6.0] - 2026-07-30

### Changed

- **Breaking (Arrow schema).** A SAS `TIME` column is now `Duration(Nanosecond)` instead of
  `Time64(Nanosecond)`, in the library's Arrow schema, in Parquet output, and in the Polars
  plugin (`pl.Duration` rather than `pl.Time`). SAS stores `TIME` as a plain numeric count of
  seconds since midnight, and real files carry values outside `[0, 24h)` — an elapsed
  duration recorded with a `TIME` format, or a negative offset. `Time64` is defined only over
  `[0, 24h)`, so those columns were written correctly but read back as **null** by every
  spec-following reader (arrow-rs, pyarrow, DuckDB, Polars). Both types carry the same `i64`
  nanosecond payload, so the change costs nothing in file size and a caller who knows a
  column is a clock time can `.cast(pl.Time)` for free. Fields also carry a
  `sas.logical_type: TIME` metadata entry (exported as `SAS_LOGICAL_TYPE_KEY`), which
  survives a Parquet round-trip, so the clock-time intent is still recoverable.

### Added

- **(CLI)** `--batch-rows` sets the scan batch size independently of
  `--parquet-row-group-size`. Each batch costs serial work on the collector thread, so a
  larger value takes that stage off the critical path, at the cost of stretching read extents.
  The plumbing already existed but nothing could reach it.
- **(CLI)** `--encode-in-flight-bytes` caps the decoded bytes held by row groups that are
  encoding but not yet written, which is what decides how many encode concurrently. The column
  axis tops out at the column count, so on a narrow table this is what fills a large host —
  raise it when cores sit idle at a large `--parquet-row-group-size`. Defaults to 1 GiB.
- **(CLI)** The conversion summary now reports source size and sustained throughput
  (`2.0 GB → 180.7 MB · 1.5 s · 1373 MiB/s`). Throughput is measured over *input* bytes,
  since output size moves with the codec and dictionary policy and so cannot be compared
  across runs.

### Fixed

- **(CLI)** A single file converted with a progress bar — the default on a terminal — printed
  no summary at all. The bar was erased on completion and neither the per-file line (suppressed
  so it cannot corrupt the bar) nor the aggregate line (batch runs only) was written, so a long
  conversion ended with no elapsed time and no row count. The closing line is now printed
  whenever the run did not already account for itself per file.
- **(CLI)** A SAS `TIME` value outside `[0, 24h)` printed as `00:00:00` in both `head` and
  CSV export. `NaiveTime` cannot represent such a value, and the fallback silently
  substituted midnight — so 359,280 seconds (99h48m) was written to CSV as midnight, with no
  error. Out-of-range and negative times now render their real elapsed value (`99:48:00`,
  `-00:01:17`); in-range values are unchanged.
- **(CLI)** CSV export ignored a column's logical type for temporal cells that widened to
  `Float64` (any sub-second date/datetime/time), writing raw SAS-epoch numbers next to
  properly formatted neighbours — one column could emit `19:18:27.950`, `00:00:00` and
  `359960.4` in three consecutive rows. `head` and CSV now share one formatter per type.
- **(CLI)** A SAS `TIME` value with a sub-millisecond fraction rendered as `.000`. It now
  keeps microsecond precision.
- **(CLI)** A conversion of more than about 2.1 billion rows failed part-way with `Parquet
  does not support more than 32767 row groups per file`. Row groups held 65,536 rows
  whatever the file's size, and 32,767 of those cover 2,147,418,112 rows — which a 234 GiB
  SAS file went past. Row groups are now sized from the declared row count, and the writer
  doubles both of its triggers as it goes, so the count stays inside the format's limit
  without needing a row count it can trust.
- **(CLI)** `--parquet-row-group-size` no longer sets the scan's batch size too. The writer
  gathers scan batches into row groups, so the two are independent — and tying them meant a
  large row group stretched the read extents to match, undoing the read tuning.

## [0.5.0] - 2026-07-29

Read the file once instead of twice, and stop encoding Parquet on a single core. Together
these are what a conversion on network storage was waiting on. Entries marked **(CLI)**
describe the `sas7bdat` command, which shares this repository but ships on PyPI under its
own version (0.7.0).

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
- Progress reporting from the parallel and fused scans. The observer registered with
  `ScanBuilder::with_progress` was only ever called from the serial page loop, so the paths
  that handle large files reported nothing at all. Counters are now updated as each chunk
  completes and reported as batches are delivered.
- `ScanBuilder::with_progress_observer`, taking an already-shared `ScanProgressObserver` so
  one callback can be reused across scans without being boxed again.
- **(CLI)** A per-file progress bar showing bytes read, rate and ETA, under the existing
  file-count bar. A single multi-minute conversion previously showed only `0/1 files`.
  Suppressed above eight concurrent files, where the bars would be noise.
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

- **(CLI)** Outputs are written to a temporary file and moved into place when complete, so a
  run that fails or is interrupted leaves nothing at the destination. By default the
  temporary sits beside the output, making the move a rename. `--tmp-dir` stages somewhere
  else — point it at a local disk and only the finished file crosses a network link.
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

[Unreleased]: https://github.com/tkragholm/sas7bdat-parser-rs/compare/sas7bdat-v0.6.0...HEAD
[0.6.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.6.0
[0.5.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.5.0
[0.3.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.3.0
