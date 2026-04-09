# typed_batches Optimization Plan

Focus area: `typed_batches` throughput on large `WINDOWS-1252` datasets.

## Priority order

1. String decode hot path (`WINDOWS-1252`)
- Highest likely ROI in encoded string decoding and mojibake handling.
- Main code paths:
  - `src/scan/row_decode.rs`
  - `src/scan/batch.rs`
- Rationale: top fixtures are large survey-style datasets with many text-like fields.

2. Reduce fallback decoding in batch path
- Ensure common columns hit direct families (`direct_numeric`, `direct_utf8_*`) instead of `fallback`.
- Add temporary counters per family to measure where rows actually route.
- Main code path:
  - `src/scan/batch.rs`

3. Tune batch size
- Benchmark `BatchHint::Rows(256)` vs `512` vs `1024`.
- Larger batches can improve throughput by amortizing per-batch overhead.
- Benchmark entry:
  - `benches/compression_matrix.rs`

4. Implement dictionary staging for repeated strings
- Use existing `DictionaryStaging` option and columnar dictionary support.
- High upside on categorical survey data.
- Relevant code:
  - `src/options.rs`
  - `src/columnar.rs`
  - `src/scan/*` (integration path)

5. Parallel decode (larger effort, high upside)
- `Parallelism` is exposed in the API but not currently wired into execution.
- Strategic step-change improvement after single-thread hot-path cleanup.
- Relevant code:
  - `src/scan/builder.rs`
  - `src/scan/raw.rs`
  - `src/scan/batch.rs`

## Recommended first implementation step

Start with step 2: add family-level counters and fallback visibility in `typed_batches`, then run benchmarks to identify exact hotspots before deeper optimization work.
