# Roadmap

This project is still in active development; the priorities below reflect the current state of the CLI and library after the recent performance work.

## Current Focus
- **Columnar + staging pipeline**
  - ✅ Columnar staging now copies SAS pages once and reuses the contiguous buffer per batch.
  - ✅ Numeric columns are materialised once per row group (date/time/datetime into typed `Vec<i32/i64>`, doubles into `Vec<f64>`), so Parquet streams pre-converted values instead of re-running `sas_*` conversions.
  - ✅ UTF-8 columns use a per-column intern pool to reuse `ByteArray` handles and avoid reallocating strings when repeated.
  - ✅ Character columns are staged with per-column arenas/dictionaries; Parquet reads dictionary IDs directly without re-iterating row slices.

- **Parquet sink hot path**
  - ✅ Streaming chunk size now matches the configured row-group size, so each row group is flushed in a single pass.
  - ✅ Numeric encoders read the materialised vectors from staging when available.
  - ✅ Hotpath instrumentation marks each encoder, making it clear UTF-8 is the remaining bottleneck.
  - ✅ Staged string arenas feed `stream_columnar::utf8` directly, avoiding per-slice `ByteArray` rebuilds.

- **Dictionary-aware string handling**
  - ✅ Small per-column dictionaries (capped at 4K unique values) deduplicate common categories without penalising high-cardinality columns.
  - ✅ Dictionaries are promoted to the staging layer so we can emit dictionary IDs per column and share the dictionary with the Parquet writer.

## Near-Term Tasks
1. **SIMD trim/null for staged strings**: scan staged character arenas with SIMD to cut remaining UTF-8 hot spots.
2. **Def-level optimisation**: skip definition levels for all-present columns and evaluate bitmapped/null-boxed defs for sparse columns.
3. **Optional parallel flushing**: revisit Rayon-based column flushing now that staging is in place, or redesign caches to allow controlled parallelism.

## Done
- Reworked `RowIterator` to expose streaming row views and avoid cloning `Value<'_>` into `'static` storage.
- Added `StreamingRow`/`StreamingCell` hooks so row sinks can process data without heap allocations.
- Zero-copy ASCII/UTF-8 validation via `simdutf8` and selective `encoding_rs` fallback.
- Column-major path retains compatibility by copying into per-column buffers for sinks that still rely on row slices.

This roadmap is updated as major milestones land; the emphasis right now is squeezing the remaining 7–8 s out of the columnar+staging path so AHS-scale conversions hit maximum throughput.
