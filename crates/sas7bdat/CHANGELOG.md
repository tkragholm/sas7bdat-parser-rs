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

## [0.7.0] - 2026-08-13

Published as 0.7.0, not 0.5.0 or 0.6.0: those two versions were developed, tagged and
documented below but never pushed to crates.io, so the registry goes 0.4.0 -> 0.7.0 while
this file records every step. Their entries are kept as written — nothing about them is
retracted, they simply reached users as part of this release rather than their own.

### Fixed

- **Breaking (behaviour). SAS format names are matched exactly instead of by substring.**
  `infer_logical_type` classified a numeric column as temporal when its format *name
  contained* `DATE`, `YY`, `MON`, `WEEK`, `YEAR` or `TIME`, or ended in `DA`/`DT`/`TM`.
  Those tests cannot tell a built-in format from a user-defined one, so survey variables
  were silently read as dates: a column formatted `FMONTH` holding the values 1-12 came
  back as dates in January 1960, and `PSATIME`, `_TOTINDA`, `DRNKSODA`, `STDWEEK`,
  `YEAR_F`, `DRETIME`, `MONTHLYINCPR` and `YEARLYINCPR` failed the same way — 17 columns
  across six real government survey files in the fixture corpus.

  Formats are now looked up in exact, sorted tables grouped by the **scale of the stored
  value** rather than by what the format prints. That distinction fixes a second defect:
  `DATEAMPM` formats a *datetime* but was classified `Date`, scaling its seconds as if
  they were days.

  The old tests also *under*-matched. `DAY`, `DOWNAME`, `JULDAY`, `JULIAN`, `QTRR`,
  `WORDDATX`, `HOUR`, `MMSS`, `MDYAMPM`, `E8601DN`, `B8601DN`, `E8601DZ`, `B8601DZ` and
  `E8601LZ` were all read as plain numbers; 176 columns in the corpus gain a correct
  temporal type. A built-in that is still missing from the tables degrades to `Float` — a
  visibly wrong type rather than silently wrong values.

- **Variable labels are trimmed the way SAS pads them.** Labels went through the same
  trim as column and format names, which are identifiers where surrounding space is noise.
  A label is different: SAS writes it at its declared width, so the trailing run is padding,
  but a *leading* space is inside the text the author typed. Labels are now trimmed at the
  end only, and only of ASCII padding (`0x20`, NUL) — a trailing non-breaking space is
  content, and `str::trim` would have eaten it because U+00A0 carries the Unicode
  `White_Space` property.

  All three rules matter: trimming both ends left 411 corpus labels differing from `haven`,
  trimming neither left 3, and trimming Unicode whitespace at the end left 1. Trimming
  trailing ASCII padding leaves 0 of 1,529. A label that is nothing but padding is still
  reported as absent.

### Changed

- **`BatchHint::Auto` sizes batches by bytes, not just rows.** `Auto` resolved to
  `rows_per_page` with no upper bound and no knowledge of row width, so batch size tracked
  page geometry rather than memory cost: on a 4,041-column file a 4,096-row batch
  materializes ~200 MB, and the parallel scan holds `workers * 2` batches queued plus one
  per worker in flight. Peak RSS therefore scaled with core count — measured at 0.30 GB
  (1 worker), 1.72 (2), 3.65 (4) and 4.81 (8) on that file, while throughput stopped
  improving past 8 workers. Wrong direction for a many-core host.

  `Auto` now also caps a batch at 32 MiB of decoded data. On the same file peak RSS falls
  to 0.07/0.39/0.72/0.98 GB for 1/2/4/8 workers — 4.5× lower at 8 — and the scan gets
  *faster* (0.74 s → 0.57 s), because more, smaller batches give the work stealer finer
  granularity. Narrow tables are untouched: their rows are small enough that the row rule
  still binds, and all 388 corpus fixtures produce byte-identical output. An explicit
  `BatchHint::Rows`/`Bytes` is unaffected.

- **Bounded scans are now parallel and stop early.** `RowSelection` and
  `ScanBuilder::limit` were enforced by two separate mechanisms with opposite costs: the
  selection was a per-row predicate that kept parallelism but never ended the scan, while the
  limit ended it early but disabled every parallel path — it counted *emitted* rows, a
  per-worker quantity that is meaningless once decode is split across threads. Both now resolve
  to one absolute `[start, end)` row range, which is worker-independent, so the parallel and
  fused paths accept a bounded scan; and because page descriptors carry `row_base`, that range
  maps to a contiguous slice of pages, so pages outside the window are never read or faulted in.

  Measured on a 138,929-row fixture: `.limit(130_000)` went from 157 ms to 24 ms (6.6× — it was
  silently single-threaded), and `select(First(100))` from 1090 µs to 181 µs (6× — it used to
  walk every page). The two spellings now perform identically, as their documentation always
  claimed. Whole-file scans are unaffected: the window is unbounded, and page pruning
  short-circuits to the full descriptor slice.

  This reaches the Polars plugin directly — `n_rows` pushdown uses `limit`, so every Polars
  query carrying one was running on a single core.

- **Breaking (API).** `IoBackendPreference` has three variants instead of four:
  `MmapPreferred` is now `Mmap`, `BufferedOnly` is now `Buffered`, and `BufferedPreferred` is
  gone — it took the same branch as `BufferedOnly` in the only place the setting is read, so
  the pair named a preference the opener could not act on. The enum now implements `Display`
  and `FromStr` (`auto` / `mmap` / `buffered`, with the old hyphenated spellings still
  accepted), replacing four hand-maintained string tables across the CLI, the profiling
  binaries, an example and a benchmark.
- **Breaking (API).** `Utf8ValidationMode` has two variants instead of three: `Auto` is now
  `Lenient` and `Off` is gone. Every decision site tests for `Strict` and treats everything
  else as lossy, so `Off` and `Auto` were the same behaviour under two names. The name now
  matches the `*Lenient` decode kernels it selects.
- `ScanBuilder::collect_batches` is now a thin wrapper over the streaming owned-batch driver
  instead of a second, parallel dispatch tree. The two had drifted: the collect side never
  grew the fused single-pass scan or the extent-streamed parallel reader, so a path-backed
  dataset (a network share, where `Dataset::open` declines to memory-map) collected serially
  in two passes while the identical scan streamed in one. Collecting from such a source is now
  parallel and single-pass — this is the R binding's path. Output is unchanged: all 387
  decodable corpus fixtures produce the same batch count, row count and cell digest as before.
- Owned-batch scans now reject `DecodeMode::Raw` at the entry point rather than only in the
  row-major fallback, so `visit_owned_batches` and `collect_batches` agree. Previously
  `collect_batches` errored while `visit_owned_batches` silently decoded every column as
  `Binary` whenever it took a parallel or fused branch.

### Added

- `Error::Internal` (with `InternalError` and `Error::internal`) and `Error::corruption`, a
  general constructor for the existing `Corruption` variant. `Error::unsupported` had become a
  catch-all covering five unrelated failure kinds at 62 call sites, which mattered because this
  crate parses untrusted files: a caller could not tell "your file is corrupt" from "this
  reader has a bug" from "this build cannot read that shape yet". 15 sites that a truncated or
  hostile file can reach (`row span exceeds page bounds`, `page slice exceeds source bounds`,
  the offset overflows) are now `Corruption`; 8 that are reader invariants (`compiled plan did
  not match column builder`, poisoned locks, worker panics) are now `Internal`, which prints
  "(please report)". `Unsupported` keeps what it should always have meant: genuinely
  unimplemented layouts, unsupported mode combinations, and platform width limits.
- `ProfileMode` and `ProjectionPreset::as_str` in the `fixture-catalog` module, so the two
  profiling binaries share one definition instead of carrying identical copies. In-repo
  tooling behind an off-by-default feature; no stability guarantee.

### Removed

- **Breaking (API and CLI).** `ValidationMode`, `OpenOptionsBuilder::validation`, and the
  CLI's `--strict-dates` flag. The option was stored on `OpenOptions` and read by nothing, so
  the flag — documented as "reject out-of-range dates/times instead of passing them through" —
  had no effect whatsoever. Implementing it was considered and rejected: a census of the corpus
  found 357 of 894 temporal columns widening to `Float64`, every one of them because of
  *sub-second* values rather than an out-of-range one, and not a single genuinely out-of-range
  date, datetime or time in 388 files. The check would have had nothing to reject. Widening is
  also not observable downstream — `column_buffer_to_arrow` and the Polars converter both
  reconcile a widened `F64` buffer back into the column's declared temporal type with
  sub-second precision intact — so there is no schema surprise to guard against either.
- `Utf8Dictionary` and `Utf8Buffer`'s `dictionary` field: ids without a vocabulary. The field
  was `#[allow(dead_code)]`, documented as "not yet populated", and no scan path ever set it.
  `dictionary_ids` itself is unaffected and still populated by dictionary staging.

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

<!-- Crate releases are tagged `sas7bdat-v*`. The bare `v*` tags belong to the Python
     packages (see the note at the top of this file) and are not this crate's versions;
     `v0.7.0` in particular was a wheel release in July, unrelated to 0.7.0 here.
     `sas7bdat-v0.5.0` was never created, so 0.5.0 has no release to link to. -->

[Unreleased]: https://github.com/tkragholm/sas7bdat-parser-rs/compare/sas7bdat-v0.7.0...HEAD
[0.7.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.7.0
[0.6.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.6.0
[0.4.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.4.0
[0.3.0]: https://github.com/tkragholm/sas7bdat-parser-rs/releases/tag/sas7bdat-v0.3.0
