# Roadmap

This document tracks improvements we want to pursue so the parser can turn
large `sas7bdat` sources into modern, ergonomic data structures efficiently.

## Parser Foundations
- ✅ Reworked `RowIterator` to yield `Value<'row>` entries instead of cloning into `'static`
  values. Borrowing data directly from page buffers now keeps string and byte
  extraction zero-copy for the common case.
- ✅ Integrate `smallvec` allocations for row decoding to keep the hot path on the
  stack when column counts are modest.
- ✅ Added a streaming row view (`StreamingRow`/`StreamingCell`) and sink hook so
  adapters can process rows without materialising an intermediate `Vec<Value>`.
- Maintain a pool of reusable row buffers (e.g. `SmallVec`) so we can recycle the
  allocation used to hold decoded `Value`s when iterating over large datasets.
- Cache decoded column names and labels in the metadata builder to avoid
  repeatedly materializing identical `String` values for every consumer.
- Eliminate remaining compiler warnings by pruning unused constants/pointers and
  tightening row-page bookkeeping so `cargo test` runs clean.

## Throughput Optimisations
- ✅ Integrate `simdutf8` in `decode_string` to fast-path ASCII/UTF-8 validation and
  fall back to `encoding_rs` only when required.
- Adopt `smallvec` where per-row buffers are small and short-lived to reduce heap
  pressure when iterating mixed-width datasets.
- Explore parallel row decoding by handing fully read pages to Rayon workers
  while the main reader advances to the next page. Keep the public iterator
  sequential but let Rayon accelerate value materialisation.

## Conversion Pipeline
- Expose a chunked conversion API so downstream consumers can build Arrow, Polars
  or parquet outputs incrementally, even when the full dataset does not fit in
  memory.
- Encapsulate metadata and row batches behind a lightweight conversion trait
  that downstream adapters (Arrow writer, serde consumer, etc.) can implement.
- Ship reference adapters for common sinks (Arrow arrays/parquet files) so users
  can plug the parser straight into modern analytics stacks.
- Stream Parquet column writers directly from SAS pages: keep Parquet column
  writers open for the lifetime of a row group, feed them consecutive SAS pages
  (rather than buffering entire row groups), and reuse fixed-size scratch
  buffers so we eliminate the `extend_columnar` allocations entirely. Combine
  this with optional uncompressed/dictionary-off writer properties for maximum
  throughput.
- SIMD/memcpy column encoders: for numeric widths copy an entire column via
  `memcpy`/SIMD lanes into the Parquet buffers, while character columns share a
  single arena + offset index so we allocate strings only once per row group.
- Parallel column flushing: when staging full row groups, use Rayon to
  flush/write columns concurrently so multi-core machines can keep column
  encoders saturated.

These steps keep the parser lean while opening the door to high-throughput,
streaming conversions from legacy SAS files into contemporary columnar formats.
