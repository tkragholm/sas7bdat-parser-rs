//! `fearless_simd` kernels — stable Rust, dispatched on the CPU's actual
//! capabilities at runtime.
//!
//! Each kernel is a generic `fn <S: Simd>(simd: S, …)` that the `dispatch!` macro
//! monomorphises once per instruction set, selecting between them on first use.
//! [`Level::new()`] is a cheap cached lookup, so taking it per call is fine.
//!
//! Widths here match the `nightly-simd` backend exactly — `u8x64`, `u64x8`,
//! `u64x4` — so the two backends stay comparable. On a 128-bit target these are
//! logical vectors that lower to four native registers; on AVX-512 they are one.

// `#[inline(always)]` on the per-chunk kernels: they are called from a hot loop
// and a real call there would stop the caller vectorizing across chunks.
#![allow(clippy::must_use_candidate)]
// The per-chunk kernels stay compiled for the differential tests and the
// `simd_backends` benchmark; production uses the column entry points instead.
#![allow(dead_code)]
#![allow(clippy::inline_always)]

use super::{NUMERIC_EXP_MASK, NUMERIC_FRACTION_MASK, scalar};
use fearless_simd::{Level, Select, Simd, SimdBase, SimdFloat, SimdInto, SimdMask, dispatch};
use fearless_simd::{f64x4, f64x8, i64x8, mask64x8, u8x64, u64x4, u64x8};

#[inline(always)]
fn missing_bitmask_impl<S: Simd>(simd: S, chunk: &[u64; 8]) -> u8 {
    let lanes = u64x8::from_slice(simd, chunk);
    let exp = u64x8::splat(simd, NUMERIC_EXP_MASK);
    let frac = u64x8::splat(simd, NUMERIC_FRACTION_MASK);
    let zeros = u64x8::splat(simd, 0);
    // fearless_simd 0.7 has no `simd_ne` on integer vectors; negate the equality mask.
    #[allow(clippy::cast_possible_truncation)]
    let mask = ((lanes & exp).simd_eq(exp) & !(lanes & frac).simd_eq(zeros)).to_bitmask() as u8;
    mask
}

#[inline]
pub fn missing_bitmask_x8(chunk: &[u64; 8]) -> u8 {
    dispatch!(Level::new(), simd => missing_bitmask_impl(simd, chunk))
}

#[inline(always)]
fn any_missing_impl<S: Simd>(simd: S, chunk: &[u64; 8]) -> bool {
    let lanes = u64x8::from_slice(simd, chunk);
    let exp = u64x8::splat(simd, NUMERIC_EXP_MASK);
    let frac = u64x8::splat(simd, NUMERIC_FRACTION_MASK);
    let zeros = u64x8::splat(simd, 0);
    // `any_true` is cheaper than materialising a bitmask on targets without movemask.
    ((lanes & exp).simd_eq(exp) & !(lanes & frac).simd_eq(zeros)).any_true()
}

#[inline]
pub fn any_missing_x8(chunk: &[u64; 8]) -> bool {
    dispatch!(Level::new(), simd => any_missing_impl(simd, chunk))
}

#[inline(always)]
fn first_non_integral_impl<S: Simd>(
    simd: S,
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    min: f64,
    max: f64,
) -> Option<usize> {
    let exp_mask = u64x4::splat(simd, NUMERIC_EXP_MASK);
    let min_lanes = f64x4::splat(simd, min);
    let max_lanes = f64x4::splat(simd, max);
    let (chunks, remainder) = raw_bits.as_chunks::<4>();

    for (chunk_index, chunk) in chunks.iter().enumerate() {
        let bits = u64x4::from_slice(simd, chunk);
        let numbers = f64x4::from_slice(simd, &chunk.map(f64::from_bits));
        let finite = !(bits & exp_mask).simd_eq(exp_mask);
        let integral = numbers.floor().simd_eq(numbers);
        let in_range = numbers.simd_ge(min_lanes) & numbers.simd_le(max_lanes);
        #[allow(clippy::cast_possible_truncation)]
        let eligible = (finite & integral & in_range).to_bitmask() as u8;
        let failing = scalar::required_bitmask(valid, chunk_index) & !eligible;
        if failing != 0 {
            return Some((chunk_index * 4) + failing.trailing_zeros() as usize);
        }
    }
    scalar::scan_remainder(raw_bits, remainder, valid, min, max)
}

pub fn first_non_integral_in_range_index(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    min: f64,
    max: f64,
) -> Option<usize> {
    dispatch!(Level::new(), simd => first_non_integral_impl(simd, raw_bits, valid, min, max))
}

/// `cvt_i64_precise` rather than `cvt_i64`: it matches Rust's saturating `as i64`
/// on every target. Callers have already proven the lanes in range, so the
/// distinction cannot bite here — but keeping the semantics identical to the
/// other two backends is what makes the differential test meaningful.
#[inline(always)]
fn to_i64_impl<S: Simd>(simd: S, chunk: &[u64; 8]) -> [i64; 8] {
    let numbers = f64x8::from_slice(simd, &chunk.map(f64::from_bits));
    simd.cvt_i64_precise_f64x8(numbers).as_array()
}

#[inline]
pub fn chunk_to_i64(chunk: &[u64; 8]) -> [i64; 8] {
    dispatch!(Level::new(), simd => to_i64_impl(simd, chunk))
}

#[inline(always)]
fn to_i64_masked_impl<S: Simd>(simd: S, chunk: &[u64; 8], valid_byte: u8) -> [i64; 8] {
    let numbers = f64x8::from_slice(simd, &chunk.map(f64::from_bits));
    let converted = simd.cvt_i64_precise_f64x8(numbers);
    // Bit i of `valid_byte` is 1 for present, and `select` takes the mask's true
    // arm first — so build the mask from the bits directly and put the value there.
    let present = mask64x8::from_bitmask(simd, u64::from(valid_byte));
    present.select(converted, i64x8::splat(simd, 0)).as_array()
}

#[inline]
pub fn chunk_to_i64_masked(chunk: &[u64; 8], valid_byte: u8) -> [i64; 8] {
    dispatch!(Level::new(), simd => to_i64_masked_impl(simd, chunk, valid_byte))
}

#[inline(always)]
fn to_i32_impl<S: Simd>(simd: S, chunk: &[u64; 8]) -> [i32; 8] {
    let converted = to_i64_impl(simd, chunk);
    #[allow(clippy::cast_possible_truncation)]
    converted.map(|x| x as i32)
}

#[inline]
pub fn chunk_to_i32(chunk: &[u64; 8]) -> [i32; 8] {
    dispatch!(Level::new(), simd => to_i32_impl(simd, chunk))
}

#[inline(always)]
fn to_i32_masked_impl<S: Simd>(simd: S, chunk: &[u64; 8], valid_byte: u8) -> [i32; 8] {
    let converted = to_i64_masked_impl(simd, chunk, valid_byte);
    #[allow(clippy::cast_possible_truncation)]
    converted.map(|x| x as i32)
}

#[inline]
pub fn chunk_to_i32_masked(chunk: &[u64; 8], valid_byte: u8) -> [i32; 8] {
    dispatch!(Level::new(), simd => to_i32_masked_impl(simd, chunk, valid_byte))
}

#[inline(always)]
fn trim_impl<S: Simd>(simd: S, slice: &[u8]) -> usize {
    let mut end = slice.len();
    let spaces = u8x64::splat(simd, b' ');
    let nuls = u8x64::splat(simd, 0);

    while end >= 64 {
        let start = end - 64;
        let chunk = u8x64::from_slice(simd, &slice[start..end]);
        let bitmask = (chunk.simd_eq(spaces) | chunk.simd_eq(nuls)).to_bitmask();
        if bitmask == u64::MAX {
            end = start;
            continue;
        }
        // Lane 0 is the LSB, so the last content byte is the highest clear bit.
        let last_content = 63 - (!bitmask).leading_zeros() as usize;
        return start + last_content + 1;
    }
    scalar::trim_trailing_space_or_nul_wide(&slice[..end]).len()
}

pub fn trim_trailing_space_or_nul_wide(slice: &[u8]) -> &[u8] {
    let end = dispatch!(Level::new(), simd => trim_impl(simd, slice));
    &slice[..end]
}

#[inline(always)]
fn is_ascii_impl<S: Simd>(simd: S, slice: &[u8]) -> bool {
    let (chunks, remainder) = slice.as_chunks::<64>();
    // OR the chunks together and test the high bits once, rather than reducing and
    // branching per chunk. The branch only pays when a non-ASCII byte appears early,
    // which for SAS character data is the rare case; the accumulate costs one
    // instruction per chunk and removes a mask reduction plus a branch from each.
    // `simd_into` on the fixed-size array is a plain transmute; `from_slice` carries
    // a length check that cannot be elided inside the loop.
    let mut acc = u8x64::splat(simd, 0);
    for chunk in chunks {
        let v: u8x64<S> = (*chunk).simd_into(simd);
        acc |= v;
    }
    let high_bits = u8x64::splat(simd, 0x80);
    let zeros = u8x64::splat(simd, 0);
    (acc & high_bits).simd_eq(zeros).all_true() && remainder.is_ascii()
}

pub fn is_ascii_wide(slice: &[u8]) -> bool {
    dispatch!(Level::new(), simd => is_ascii_impl(simd, slice))
}

// ---------------------------------------------------------------- column entry points
//
// The reason this module is shaped the way it is. `dispatch!` selects a
// `#[target_feature]` implementation, and such a function cannot be inlined into a
// caller that lacks the feature — so routing a column through the per-chunk kernels
// above costs one real call per 8 rows. Measured on a Zen 3 EPYC at baseline,
// `chunk_to_i64_masked` ran at 6.8 GiB/s that way against scalar's 55.9.
//
// Dispatching once and instantiating the whole loop inside the selected
// implementation removes that entirely: the shared loops in the parent module are
// `#[inline(always)]` and generic over the per-chunk kernel, so the closures below
// fold into the target-feature body.

/// See [`super::classify_missing_with`].
pub fn classify_missing_raw_bits(raw_bits: &[u64]) -> Option<Vec<u64>> {
    dispatch!(Level::new(), simd =>
        super::classify_missing_with(raw_bits, |chunk| missing_bitmask_impl(simd, chunk)))
}

/// See [`super::convert_column_with`].
pub fn convert_column_i64<T>(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    out: &mut Vec<T>,
    wrap: impl Fn(i64) -> T,
) {
    dispatch!(Level::new(), simd =>
    super::convert_column_with(raw_bits, valid, out, wrap, |chunk, valid_byte| {
        to_i64_masked_impl(simd, chunk, valid_byte)
    }));
}

/// See [`super::convert_column_with`].
pub fn convert_column_i32<T>(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    out: &mut Vec<T>,
    wrap: impl Fn(i32) -> T,
) {
    dispatch!(Level::new(), simd =>
    super::convert_column_with(raw_bits, valid, out, wrap, |chunk, valid_byte| {
        to_i32_masked_impl(simd, chunk, valid_byte)
    }));
}

/// See [`super::gather_missing_with`].
pub fn gather_missing(
    page: &[u8],
    base: usize,
    stride: usize,
    len: usize,
    raw_bits: &mut Vec<u64>,
) -> bool {
    dispatch!(Level::new(), simd =>
    super::gather_missing_with(page, base, stride, len, raw_bits, |lane| {
        any_missing_impl(simd, lane)
    }))
}
