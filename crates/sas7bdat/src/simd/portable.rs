//! `std::simd` kernels — the original implementation, now one backend of three.
//!
//! Requires a nightly toolchain for `#![feature(portable_simd)]`. Unlike
//! [`super::fearless`] this does **not** dispatch at runtime: it lowers to
//! whatever target features were enabled at compile time, so on a portable
//! x86-64 build every vector below executes as SSE2 regardless of what the CPU
//! supports. Set `-C target-cpu` to change that.

// `#[inline(always)]` on the per-chunk kernels: they are called from a hot loop
// and a real call there would stop the caller vectorizing across chunks.
#![allow(clippy::inline_always)]

use super::{NUMERIC_EXP_MASK, NUMERIC_FRACTION_MASK, scalar};
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::num::{SimdFloat, SimdUint};
use std::simd::{Mask, Select, Simd, StdFloat};

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn missing_bitmask_x8(chunk: &[u64; 8]) -> u8 {
    type U64x8 = Simd<u64, 8>;
    let lanes = U64x8::from_array(*chunk);
    let exp = U64x8::splat(NUMERIC_EXP_MASK);
    let frac = U64x8::splat(NUMERIC_FRACTION_MASK);
    let zeros = U64x8::splat(0);
    ((lanes & exp).simd_eq(exp) & (lanes & frac).simd_ne(zeros)).to_bitmask() as u8
}

#[inline(always)]
pub(crate) fn any_missing_x8(chunk: &[u64; 8]) -> bool {
    type U64x8 = Simd<u64, 8>;
    let lanes = U64x8::from_array(*chunk);
    let exp = U64x8::splat(NUMERIC_EXP_MASK);
    let frac = U64x8::splat(NUMERIC_FRACTION_MASK);
    let zeros = U64x8::splat(0);
    ((lanes & exp).simd_eq(exp) & (lanes & frac).simd_ne(zeros)).any()
}

pub(crate) fn first_non_integral_in_range_index(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    min: f64,
    max: f64,
) -> Option<usize> {
    type U64x4 = Simd<u64, 4>;
    type F64x4 = Simd<f64, 4>;

    let exp_mask = U64x4::splat(NUMERIC_EXP_MASK);
    let min_lanes = F64x4::splat(min);
    let max_lanes = F64x4::splat(max);
    let (chunks, remainder) = raw_bits.as_chunks::<4>();

    for (chunk_index, chunk) in chunks.iter().enumerate() {
        let bits = U64x4::from_array(*chunk);
        let numbers = F64x4::from_array(bits.to_array().map(f64::from_bits));
        let finite = (bits & exp_mask).simd_ne(exp_mask);
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

/// Expand 8 packed validity bits into a per-lane mask that is `true` where the
/// lane is **null**, matching `select(zeros, converted)` below.
#[inline(always)]
fn null_mask(valid_byte: u8) -> Mask<i64, 8> {
    type U64x8 = Simd<u64, 8>;
    let shifts = U64x8::from_array([0, 1, 2, 3, 4, 5, 6, 7]);
    let spread = U64x8::splat(u64::from(valid_byte)) >> shifts;
    (spread & U64x8::splat(1)).simd_eq(U64x8::splat(0))
}

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
fn to_i64(chunk: &[u64; 8]) -> Simd<i64, 8> {
    type F64x8 = Simd<f64, 8>;
    type U64x8 = Simd<u64, 8>;
    F64x8::from_array(U64x8::from_array(*chunk).to_array().map(f64::from_bits)).cast()
}

#[inline(always)]
pub(crate) fn chunk_to_i64(chunk: &[u64; 8]) -> [i64; 8] {
    to_i64(chunk).to_array()
}

#[inline(always)]
pub(crate) fn chunk_to_i64_masked(chunk: &[u64; 8], valid_byte: u8) -> [i64; 8] {
    null_mask(valid_byte)
        .select(Simd::<i64, 8>::splat(0), to_i64(chunk))
        .to_array()
}

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
fn to_i32(chunk: &[u64; 8]) -> Simd<i32, 8> {
    type F64x8 = Simd<f64, 8>;
    type U64x8 = Simd<u64, 8>;
    F64x8::from_array(U64x8::from_array(*chunk).to_array().map(f64::from_bits)).cast()
}

#[inline(always)]
pub(crate) fn chunk_to_i32(chunk: &[u64; 8]) -> [i32; 8] {
    to_i32(chunk).to_array()
}

#[inline(always)]
pub(crate) fn chunk_to_i32_masked(chunk: &[u64; 8], valid_byte: u8) -> [i32; 8] {
    // The null mask is over `i64` lanes; cast it to `i32` lanes to select on the
    // narrower vector without changing which lanes are set.
    null_mask(valid_byte)
        .cast::<i32>()
        .select(Simd::<i32, 8>::splat(0), to_i32(chunk))
        .to_array()
}

#[inline(always)]
pub(crate) fn trim_trailing_space_or_nul_wide(slice: &[u8]) -> &[u8] {
    type U8x64 = Simd<u8, 64>;

    let mut end = slice.len();
    let spaces = U8x64::splat(b' ');
    let nuls = U8x64::splat(0);

    while end >= 64 {
        let start = end - 64;
        let chunk = U8x64::from_slice(&slice[start..end]);
        // SAS pads with spaces; corrupted or partial records may carry NULs.
        let bitmask = (chunk.simd_eq(spaces) | chunk.simd_eq(nuls)).to_bitmask();
        if bitmask == u64::MAX {
            end = start;
            continue;
        }
        // Lane 0 is the LSB, so the last content byte is the highest clear bit.
        let last_content = 63 - (!bitmask).leading_zeros() as usize;
        return &slice[..=(start + last_content)];
    }

    scalar::trim_trailing_space_or_nul_wide(&slice[..end])
}

#[inline(always)]
pub(crate) fn is_ascii_wide(slice: &[u8]) -> bool {
    type U8x64 = Simd<u8, 64>;

    let (chunks, remainder) = slice.as_chunks::<64>();
    let high_bits = U8x64::splat(0x80);
    for chunk in chunks {
        if (U8x64::from_array(*chunk) & high_bits).reduce_or() != 0 {
            return false;
        }
    }
    remainder.is_ascii()
}
