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

5. Implemented SIMD integral/range probe for staged numeric materialization.
- Added `first_non_integral_in_range_index_simd` in `src/scan/numeric.rs` using 4-lane SIMD
  `floor + range + finite` checks over staged raw bits.
- Applied probe to all staged typed-or-f64 materializers:
  - `materialize_staged_i64_or_f64_column`
  - `materialize_staged_date_or_f64_column`
  - `materialize_staged_datetime_or_f64_column`
  - `materialize_staged_time_or_f64_column`
- On `top3_target` typed-batches (Criterion), this produced:
  - clear improvement on the largest NYYTS 2020 file (~3.3% faster),
  - neutral/noise-level movement on the other two targets.

6. Implemented SIMD validity-mask materialization for staged `f64` columns.
- Updated `materialize_staged_f64_column` to process nullable staged chunks in 8-lane SIMD
  (`u64x8` bits + `u8x8` validity mask), writing zero bits for null lanes before conversion.
- Kept scalar fallback only for the remainder (<8 lanes).
- On `top3_target` typed-batches (Criterion), this gave broad wins:
  - NYYTS 2020: ~6.3% faster,
  - NYYTS 2018: ~6.6% faster,
  - NYSDOH BRFSS 2018: ~4.4% faster.

## Key learnings

1. Numeric routing dominates cost and opportunity.
- Moving more numeric cells into staged path produced the largest gains.
- After staged expansion, `direct_numeric` pressure dropped to ~0 on target corpus.

2. Raw hotpath elapsed is useful for ranking, not final throughput decisions.
- Hotpath runs showed high variance across runs; Criterion top3 remains primary decision signal.

3. Some intuitive micro-optimizations regressed real workloads.
- Two attempted follow-up experiments were explicitly reverted after top3 regressions.
- Keep strict loop: implement -> profile -> benchmark -> keep/revert.
- Example: direct owned-UTF8 dispatch simplification regressed all top3 fixtures and was reverted.

4. Bitflags are the right state representation for this planner.
- Plan states are not globally mutually exclusive; mixed-family schemas are common.
- Enum-only modeling is less practical than bitflags + targeted fast-path checks.

## Existing SIMD coverage

| Location | What | SIMD width |
|---|---|---|
| `src/scan/numeric.rs:classify_missing_raw_bits` | missing-sentinel detection | `u64x8` |
| `src/scan/string.rs:trim_trailing_space_or_nul_simd` | trailing space/nul trim | `u8x64` |
| `src/scan/string.rs:is_ascii_simd` | ASCII classification | `u8x64` |
| `src/scan/string.rs:is_all_space_or_nul_12` | exact-12-byte empty check | word (u64+u32) |

The string and missing-detection paths are well covered. The numeric **finalize** path (`materialize_staged_*`) has no SIMD yet — that is the main gap.

## Current priority order

1. **Single-pass materialization** — eliminate double-scan in `materialize_staged_*_or_f64_column`.
- All four functions (`i64_or_f64`, `date_or_f64`, `datetime_or_f64`, `time_or_f64`) do a full
  probe-pass (`all_integral` / `all_dates` check) followed by a separate convert-pass: 2× memory
  bandwidth over the `raw_bits` vector, and `validity_is_null` is called twice per element in the
  nullable case.
- Fix: write output directly in a single pass; widen to f64 only on the first non-integral value.
  The widen fallback only runs for rare mixed-type batches; all-integer batches get one pass.
- Main file: `src/scan/numeric.rs`.

2. **SIMD `all_integral` / `all_dates` check via `SimdFloat::floor()`** — vectorize the probe-pass.
- The current probe-pass calls `try_i64_from_f64` per element (involves `is_finite`, range check,
  cast, round-trip compare — multiple branches per value).
- `std::simd::num::SimdFloat::floor()` is available on nightly. With `Simd<f64, 4>` and the
  existing `NUMERIC_EXP_MASK` constant (reused from `classify_missing_raw_bits`):
  ```
  finite   = (bits & EXP_MASK) != EXP_MASK   // non-finite lanes = null/NaN → excluded
  integral = floor(vals) == vals
  ok       = !finite | integral               // any lane failing → widen to f64
  ```
- Replaces 4 calls to `try_i64_from_f64` (each multi-branch) with a couple of SIMD ops per chunk.
- Applies identically to all four `materialize_staged_*_or_f64_column` type-check passes.
- If `SimdFloat::cast::<i64>()` is also available, the convert-pass can be vectorized too.
- Best implemented together with item 1 (single-pass structure makes the probe naturally SIMD).
- Main file: `src/scan/numeric.rs`.

3. **Optimize remaining owned UTF-8 direct path** (6.33% routed cells).
- Main files: `src/scan/batch.rs`, `src/scan/row_decode.rs`.

4. **SIMD for validity-byte–masked f64 materialization**.
- In `materialize_staged_f64_column` with `valid.is_some()`, per-element `validity_is_null` loads
  `valid[index]` (a byte), which prevents autovectorization of the transmute loop.
- Fix: load 8 `raw_bits` + 8 validity bytes simultaneously; blend with `simd_ne(0)` mask; push 8
  f64s. Straightforward `u64x8` blend, no gather needed since `valid: Vec<u8>` is contiguous.
- Main file: `src/scan/numeric.rs`.

5. **Bit-packed validity bitmap** — architectural enabler for deeper SIMD.
- `valid: Option<Vec<u8>>` uses 1 byte per row. `classify_missing_raw_bits` already internally
  computes a `u64` bitmask per 8-lane chunk (`to_bitmask()`) and then expands it back to bytes —
  that expansion is pure waste.
- Switching to a bit-packed `Vec<u64>` (Arrow-style, 1 bit/row) would:
  - Let `classify_missing_raw_bits` write bitmask words directly.
  - Make validity masking in materialize a bitwise op on `u64` words rather than byte loads.
  - Cut validity footprint by 8× → better cache behaviour for large batches.
- Depends on whether downstream batch consumers can handle bit-packed validity (larger change).
- Main files: `src/scan/numeric.rs`, `src/columnar.rs`.

6. **Word-at-a-time trim for 8–63-byte strings**.
- `trim_and_classify_ascii` takes the SIMD path only for `>= 64` bytes; 8–63-byte strings use a
  scalar byte loop from the tail.
- For the common case of trailing-space-padded fixed-width columns of width 8–32, a word-at-a-time
  scan checking 8 bytes at a time (`u64::from_ne_bytes`) is 8× fewer iterations.
- The exact-12 special case already does this; generalise it to the full 8–63-byte range.
- Main file: `src/scan/string.rs`.

7. **Batch-size sweep** for large fixtures (`256` vs `512` vs `1024`) after materialization improvements settle.
- Main file: `benches/compression_matrix.rs`.

8. **Dictionary staging for repeated strings**.
- Main files: `src/options.rs`, `src/columnar.rs`, `src/scan/*`.

9. **Parallel typed-batch decode** (larger effort).
- Main files: `src/scan/builder.rs`, `src/scan/batch.rs`, `src/scan/raw.rs`.

## Working rule

Treat `top3_target` typed-batches Criterion results as the acceptance gate. Keep only changes that are neutral/improving across the large fixtures and revert mixed regressions.
