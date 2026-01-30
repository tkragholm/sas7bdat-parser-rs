# Roadmap

This project is still in active development; the priorities below reflect the current state of the CLI and library after the recent performance work.

## Current Focus

- **Parser refactor**
  - ✅ Row iterator split into paging/buffering/decoding modules for readability and targeted testing.
  - ✅ Row/unit tests cover compressed pages, mix pages, and decoding edge cases.
- **Sinks refactor**
  - ✅ Parquet sink split into plan/stream/utf8/constants modules; CSV sink split into sink/encode/time_format/constants.
  - ✅ Row-group lifecycle helpers ensure writers are closed (fixes “Previous column writer was not closed” errors).
  - ⏩ Parquet stream encoders still duplicate some numeric/UTF-8 logic; consider smaller helpers without lifetime churn.
- **Columnar + staging pipeline**
  - ✅ Columnar staging copies SAS pages once and reuses contiguous buffers per batch.
  - ✅ Numeric columns are materialised once per row group; Parquet streams pre-converted values.
  - ✅ UTF-8 columns use per-column intern pools/dictionaries to reuse `ByteArray` handles when repeated.
  - ⏩ Remaining bottleneck: UTF-8 staging/writing still dominates ~20–27% on AHS-scale files.

## Near-Term Tasks

1. **Parquet streaming helpers**: dedupe numeric/time/date/time encoders with a lightweight helper that keeps column writers closed; add regression test to catch dangling writer errors.
2. **UTF-8 hot path**: profile `stream_columnar::utf8[_staged]` and tighten dictionary hashing or add a dictionary-off mode for high-cardinality columns.
3. **Def-level optimisation**: investigate bitmap/compact def-levels for sparse nulls while keeping writer requirements satisfied.
4. **CSV/Parquet tests**: add integration tests for streaming columnar paths and CSV time/date formatting.
5. **Optional parallel flushing**: revisit Rayon-style column flushing now that staging is split and caches are contained.

## Done

- Reworked `RowIterator` to expose streaming row views and avoid cloning `Value<'_>` into `'static` storage.
- Added `StreamingRow`/`StreamingCell` hooks so row sinks can process data without heap allocations.
- Zero-copy ASCII/UTF-8 validation via `simdutf8` and selective `encoding_rs` fallback.
- Column-major path retains compatibility by copying into per-column buffers for sinks that still rely on row slices.

This roadmap is updated as major milestones land; the emphasis right now is squeezing the remaining 7–8 s out of the columnar+staging path so AHS-scale conversions hit maximum throughput.
