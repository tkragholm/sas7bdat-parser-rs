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

7. Added 8-byte word path for short-string trim/classify (`8..63` bytes).
- Updated small-string branch in `trim_and_classify_ascii` to use:
  - `trim_trailing_space_or_nul_word` (8-byte tail chunks),
  - `is_ascii_word` (8-byte high-bit checks).
- Important tuning note:
  - Initial implementation using per-byte `iter().all(...)` for 8-byte trim checks regressed and
    was replaced with a pure bitwise test: `(word & !SPACES_HEAD_12) == 0`.
- On top3 typed-batches, the tuned variant showed improvement on the two NYYTS files and
  near-noise movement on BRFSS (monitor for run-to-run variance).

8. Post-review cleanup and small refinements.
- Removed unreachable dead `else` branch in `materialize_staged_f64_column` (lines 194-202 were
  never reached since `valid.is_none()` returned early; the `if let Some` was always true).
- Replaced double chunk-index in `first_non_integral_in_range_index_simd`: `F64x4` now built via
  `bits.to_array().map(f64::from_bits)` rather than re-indexing `chunk[0..3]`.
- `trim_trailing_space_or_nul_simd` now delegates its sub-64-byte remainder to
  `trim_trailing_space_or_nul_word` instead of the scalar byte loop.
- Removed now-unused `validity_is_null` helper (was only called from the dead code removed above).

9. Eliminated `.to_vec()` clone in `materialize_staged_*_or_f64_column` f64 fallback.
- Changed all four `*_or_f64` functions from `raw_bits: &[u64]` to `raw_bits: Vec<u64>` (owned).
- Updated `materialize_staged_numeric_column` callers to move rather than borrow, allowing the
  f64 fallback path to call `materialize_staged_f64_column(raw_bits, valid)` without allocating
  and copying the entire `raw_bits` vector. Previously each f64-type-fallback batch paid a full
  `Vec<u64>` clone before materialization.

10. WINDOWS-1252 direct transcode for the lenient UTF-8 owned path — **REVERTED**.
- Implemented compile-time lookup table + direct transcode into column data buffer, bypassing
  `encoding_rs` and mojibake allocations for non-ASCII WINDOWS-1252 strings without 0xC2/0xC3.
- Benchmarked on top3_target typed-batches:
  - NYYTS 2020: neutral (p=0.85)
  - NYYTS 2018: **+1.3% slower** (p=0.00, statistically significant regression)
  - BRFSS 2018: -1.1% faster (p=0.02)
- Mixed result → reverted per working rule.
- Root cause: 93.67% of cells are staged_numeric; string fast path only touches 6.33% of cells,
  leaving minimal leverage. The mojibake pre-check (`memchr2`) and branch overhead in the fast
  path cost slightly more than the allocation savings for these fixtures.

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
- Example: first 8-byte short-string trim draft regressed; bitwise-only refinement recovered.

4. Bitflags are the right state representation for this planner.
- Plan states are not globally mutually exclusive; mixed-family schemas are common.
- Enum-only modeling is less practical than bitflags + targeted fast-path checks.

11. Bit-packed validity bitmap (Arrow-style `Vec<u64>`, 1 bit/row).
- Changed `valid: Option<Vec<u8>>` (1 byte/row) to `valid: Option<Vec<u64>>` (64 rows/word)
  throughout: `OwnedColumnBuffer`, `OwnedBatchColumnBuilder`, `ColumnBuffer`/`BitSlice`.
- `classify_missing_raw_bits` now writes one `u64` bitmask word per 64 rows directly from the
  SIMD `to_bitmask()` result, eliminating the byte-expansion loop.
- `materialize_staged_f64_column` extracts 8 validity bits per SIMD chunk from the packed word
  via shift+expand, removing the 8-byte validity load and `U8x8` comparison.
- `first_non_integral_in_range_index_simd` now extracts 4 validity bits from a packed word with a
  single shift+mask, replacing a 4-byte loop.
- Row-by-row push functions (`push_primitive_*`, `push_variable_*`) use `set_valid_bit` and
  `init_validity_first_null` helpers for incremental bit-accumulation.
- Validity footprint: 8× smaller → better cache behaviour for large batches.
- On top3_target typed-batches (Criterion):
  - NYYTS 2020: **-11.7% faster** (p=0.00)
  - NYYTS 2018: **-9.6% faster** (p=0.00)
  - BRFSS 2018: **-3.1% faster** (p=0.00)

## Existing SIMD coverage

| Location | What | SIMD width |
|---|---|---|
| `src/scan/numeric.rs:classify_missing_raw_bits` | missing-sentinel detection | `u64x8` |
| `src/scan/numeric.rs:first_non_integral_in_range_index_simd` | integral+range probe | `f64x4` |
| `src/scan/numeric.rs:materialize_staged_f64_column` | nullable f64 materialization | `u64x8` |
| `src/scan/numeric.rs:materialize_staged_i64_or_f64_column` | nullable i64 materialization | `f64x8`→`i64x8` + `u64x8` validity |
| `src/scan/numeric.rs:materialize_staged_date_or_f64_column` | nullable date materialization | `f64x8`→`i64x8` + `u64x8` validity |
| `src/scan/numeric.rs:materialize_staged_datetime_or_f64_column` | nullable datetime materialization | `f64x8`→`i64x8` + `u64x8` validity |
| `src/scan/numeric.rs:materialize_staged_time_or_f64_column` | nullable time materialization | `f64x8`→`i64x8` + `u64x8` validity |
| `src/scan/string.rs:trim_trailing_space_or_nul_simd` | trailing space/nul trim + CLZ final position | `u8x64` |
| `src/scan/string.rs:is_ascii_simd` | ASCII classification | `u8x64` |
| `src/scan/string.rs:is_all_space_or_nul_12` | exact-12-byte empty check | word (u64+u32) |
| `src/scan/string.rs:trim_trailing_space_or_nul_word` | 8-63-byte trim | word (u64) |
| `src/scan/string.rs:is_ascii_word` | 8-63-byte ASCII check | word (u64) |

## Current priority order

1. **Batch-size sweep** for large fixtures (`256` vs `512` vs `1024`) after materialization improvements settle.
- Main file: `benches/compression_matrix.rs`.

5. **Dictionary staging for repeated strings**.
- Main files: `src/options.rs`, `src/columnar.rs`, `src/scan/*`.

6. **Parallel typed-batch decode** (larger effort).
- Main files: `src/scan/builder.rs`, `src/scan/batch.rs`, `src/scan/raw.rs`.

## Working rule

Treat `top3_target` typed-batches Criterion results as the acceptance gate. Keep only changes that are neutral/improving across the large fixtures and revert mixed regressions.
