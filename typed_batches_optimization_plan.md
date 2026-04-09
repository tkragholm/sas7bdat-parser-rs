# typed_batches Optimization Plan

Focus area: maximize `typed_batches` throughput on large `WINDOWS-1252` fixtures with stable, reproducible benchmark signals.

## Progress log

Completed:
1. Added family routing counters and target-corpus stats tooling.
- Added `ScanStats` family counters and reporting in batch scan path.
- Added examples:
  - `examples/batch_family_stats.rs`
  - `examples/batch_family_stats_target.rs`
- Result on full target corpus:
  - `staged_numeric_cells=93.67%`
  - `direct_utf8_owned_cells=6.33%`
  - `fallback_cells=0%`

2. Added hotpath profiling integration next to Criterion artifacts.
- Added optional `hotpath-profile` feature and function instrumentation on batch hot paths.
- Added example + recipes:
  - `examples/hotpath_typed_batches_target.rs`
  - `just hotpath-typed-batches-target`
  - output in `target/criterion/hotpath/`.

3. Implemented and kept high-ROI numeric-path optimizations.
- Direct numeric dispatch specialized by compiled kernel groups.
- `push_row` now avoids calling empty families.
- Expanded staged numeric tiling coverage across widths.
- Added staged+utf8 fast path and precomputed per-row counter increments.
- Packed batch plan state into bitflags (removed clippy suppressions, improved branch-friendly state checks).

4. Improved benchmark quality for long typed-batch runs.
- Increased typed-batches benchmark measurement window in `benches/compression_matrix.rs`.
- Reduces repeated Criterion warnings and improves stability for large fixtures.

## Key learnings

1. Numeric routing dominates cost and opportunity.
- Moving more numeric cells into staged path produced the largest gains.
- After staged expansion, `direct_numeric` pressure dropped to ~0 on target corpus.

2. Raw hotpath elapsed is useful for ranking, not final throughput decisions.
- Hotpath runs showed high variance across runs; Criterion top3 remains primary decision signal.

3. Some intuitive micro-optimizations regressed real workloads.
- Two attempted follow-up experiments were explicitly reverted after top3 regressions.
- Keep strict loop: implement -> profile -> benchmark -> keep/revert.

4. Bitflags are the right state representation for this planner.
- Plan states are not globally mutually exclusive; mixed-family schemas are common.
- Enum-only modeling is less practical than bitflags + targeted fast-path checks.

## Current priority order

1. Reduce staged numeric materialization overhead (`take_batch` / staged finalize path).
- Main files: `src/scan/batch.rs`, `src/scan/numeric.rs`.

2. Optimize remaining owned UTF-8 direct path (6.33% routed cells).
- Main files: `src/scan/batch.rs`, `src/scan/row_decode.rs`.

3. Batch-size sweep for large fixtures (`256` vs `512` vs `1024`) after current optimizations settle.
- Main file: `benches/compression_matrix.rs`.

4. Dictionary staging for repeated strings.
- Main files: `src/options.rs`, `src/columnar.rs`, `src/scan/*`.

5. Parallel typed-batch decode (larger effort).
- Main files: `src/scan/builder.rs`, `src/scan/batch.rs`, `src/scan/raw.rs`.

## Working rule

Treat `top3_target` typed-batches Criterion results as the acceptance gate. Keep only changes that are neutral/improving across the large fixtures and revert mixed regressions.
