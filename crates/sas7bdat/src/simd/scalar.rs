//! Stable-Rust kernels with no explicit vector types.
//!
//! Always compiled, for three reasons: it is the oracle the differential tests
//! hold the other two against, it keeps a path that needs no dependency beyond
//! std for anyone stuck on an older toolchain, and on some kernels it is simply
//! the fastest of the three.
//!
//! Written to be *autovectorizable* rather than scalar for its own sake — the
//! loops are over fixed-size arrays with no early exits, so LLVM can widen them.
//! That is why [`missing_bitmask_x8`] beats a hand-written `to_bitmask()` on
//! NEON: the shift-accumulate form lowers to something cheaper than the
//! movemask emulation portable SIMD is forced into.

// Compiled in every configuration as the differential oracle, so when a vector
// backend is live some of these have no caller outside the tests.
#![allow(clippy::must_use_candidate)]
#![allow(dead_code)]
// `&` and `|` on bools rather than `&&`/`||` is the point of this module: the
// short-circuiting operators introduce branches, and a branch inside the loop
// body is what stops LLVM widening it. These kernels are branchless on purpose.
#![allow(clippy::needless_bitwise_bool)]
// Likewise `#[inline(always)]`: these are single-chunk kernels called from a hot
// loop, and the caller's loop cannot vectorize across a real call.
#![allow(clippy::inline_always)]
// `floor(x) == x` IS the integrality test. An epsilon comparison would be wrong
// here, and the value compare has to match the vector backends' `simd_eq` exactly
// or the differential test is meaningless.
#![allow(clippy::float_cmp)]

use super::{NUMERIC_EXP_MASK, NUMERIC_FRACTION_MASK, valid_bit};

const ASCII_HIGH_BITS_8: u64 = 0x8080_8080_8080_8080;

/// Bit `i` set when lane `i` holds a SAS missing sentinel.
#[inline(always)]
pub fn missing_bitmask_x8(chunk: &[u64; 8]) -> u8 {
    let mut mask = 0u8;
    for (i, &bits) in chunk.iter().enumerate() {
        let is_missing =
            ((bits & NUMERIC_EXP_MASK) == NUMERIC_EXP_MASK) & ((bits & NUMERIC_FRACTION_MASK) != 0);
        mask |= u8::from(is_missing) << i;
    }
    mask
}

#[inline(always)]
pub fn any_missing_x8(chunk: &[u64; 8]) -> bool {
    missing_bitmask_x8(chunk) != 0
}

/// Index of the first cell that is not a finite integer inside `[min, max]`,
/// skipping cells marked null by `valid`.
pub fn first_non_integral_in_range_index(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    min: f64,
    max: f64,
) -> Option<usize> {
    let (chunks, remainder) = raw_bits.as_chunks::<4>();
    for (chunk_index, chunk) in chunks.iter().enumerate() {
        let mut eligible = 0u8;
        for (i, &bits) in chunk.iter().enumerate() {
            let number = f64::from_bits(bits);
            let ok = ((bits & NUMERIC_EXP_MASK) != NUMERIC_EXP_MASK)
                & (number.floor() == number)
                & (number >= min)
                & (number <= max);
            eligible |= u8::from(ok) << i;
        }
        let required = required_bitmask(valid, chunk_index);
        let failing = required & !eligible;
        if failing != 0 {
            return Some((chunk_index * 4) + failing.trailing_zeros() as usize);
        }
    }
    scan_remainder(raw_bits, remainder, valid, min, max)
}

/// The 4 validity bits covering a 4-lane chunk, as the low nibble of a byte.
/// All-ones when the column has no nulls.
#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
pub(super) fn required_bitmask(valid: Option<&[u64]>, chunk_index: usize) -> u8 {
    valid.map_or(0b1111u8, |validity| {
        let bit_base = chunk_index * 4;
        ((validity[bit_base / 64] >> (bit_base % 64)) & 0xF) as u8
    })
}

/// The sub-4-lane tail of [`first_non_integral_in_range_index`]. Identical in all
/// three backends, so it lives here and they call it.
#[inline]
pub(super) fn scan_remainder(
    raw_bits: &[u64],
    remainder: &[u64],
    valid: Option<&[u64]>,
    min: f64,
    max: f64,
) -> Option<usize> {
    let processed = raw_bits.len() - remainder.len();
    for (offset, &bits) in remainder.iter().enumerate() {
        let index = processed + offset;
        if valid.is_some_and(|validity| !valid_bit(validity, index)) {
            continue;
        }
        let number = f64::from_bits(bits);
        if !number.is_finite()
            || number < min
            || number > max
            || number.floor().to_bits() != number.to_bits()
        {
            return Some(index);
        }
    }
    None
}

/// Convert 8 staged f64 bit patterns to `i64`. Callers guarantee every lane is a
/// finite integer in range, via [`first_non_integral_in_range_index`].
#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
pub fn chunk_to_i64(chunk: &[u64; 8]) -> [i64; 8] {
    chunk.map(|bits| f64::from_bits(bits) as i64)
}

/// As [`chunk_to_i64`], but lanes marked null in `valid_byte` become 0.
#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
pub fn chunk_to_i64_masked(chunk: &[u64; 8], valid_byte: u8) -> [i64; 8] {
    let mut out = [0i64; 8];
    for (i, slot) in out.iter_mut().enumerate() {
        if (valid_byte >> i) & 1 == 1 {
            *slot = f64::from_bits(chunk[i]) as i64;
        }
    }
    out
}

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
pub fn chunk_to_i32(chunk: &[u64; 8]) -> [i32; 8] {
    chunk.map(|bits| f64::from_bits(bits) as i32)
}

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
pub fn chunk_to_i32_masked(chunk: &[u64; 8], valid_byte: u8) -> [i32; 8] {
    let mut out = [0i32; 8];
    for (i, slot) in out.iter_mut().enumerate() {
        if (valid_byte >> i) & 1 == 1 {
            *slot = f64::from_bits(chunk[i]) as i32;
        }
    }
    out
}

const SPACES_8: u64 = u64::from_ne_bytes([b' '; 8]);

/// Byte-at-a-time trim. The tail of every other trim kernel.
#[inline]
pub fn trim_trailing_space_or_nul(slice: &[u8]) -> &[u8] {
    let mut end = slice.len();
    while end > 0 {
        let byte = slice[end - 1];
        if byte != b' ' && byte != 0 {
            break;
        }
        end -= 1;
    }
    &slice[..end]
}

/// Whether all 8 bytes are ASCII space or NUL. Clearing the 0x20 bits of a
/// spaces-and-NULs word leaves nothing.
#[inline]
fn all_space_or_nul_8(chunk: &[u8]) -> bool {
    debug_assert_eq!(chunk.len(), 8);
    let word = u64::from_ne_bytes(chunk.try_into().expect("8-byte chunk"));
    (word & !SPACES_8) == 0
}

/// Trim trailing spaces and NULs, word at a time.
///
/// This is the scalar backend's whole trim kernel, and it is *also* the sub-64
/// byte path in all three backends — the vector kernels only engage at ≥64
/// bytes, and below that they measured no better than this. The width gate lives
/// in [`crate::scan::string`].
#[inline]
pub fn trim_trailing_space_or_nul_wide(slice: &[u8]) -> &[u8] {
    let mut end = slice.len();
    while end >= 8 {
        let start = end - 8;
        let chunk = &slice[start..end];
        if all_space_or_nul_8(chunk) {
            end = start;
            continue;
        }

        // Partial word: the last content byte is inside it, so walk back to it.
        let mut i = end;
        while i > start {
            let byte = slice[i - 1];
            if byte != b' ' && byte != 0 {
                return &slice[..i];
            }
            i -= 1;
        }
        end = start;
    }
    trim_trailing_space_or_nul(&slice[..end])
}

#[inline]
pub fn is_ascii_wide(slice: &[u8]) -> bool {
    let (chunks, remainder) = slice.as_chunks::<8>();
    for chunk in chunks {
        // `*chunk` is already [u8; 8], so this drops the try_into/expect the
        // slice-based version needed.
        if u64::from_ne_bytes(*chunk) & ASCII_HIGH_BITS_8 != 0 {
            return false;
        }
    }
    remainder.is_ascii()
}

// ---------------------------------------------------------------- column entry points

/// See [`super::classify_missing_with`].
pub fn classify_missing_raw_bits(raw_bits: &[u64]) -> Option<Vec<u64>> {
    super::classify_missing_with(raw_bits, missing_bitmask_x8)
}

/// See [`super::convert_column_with`].
pub fn convert_column_i64<T>(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    out: &mut Vec<T>,
    wrap: impl Fn(i64) -> T,
) {
    super::convert_column_with(raw_bits, valid, out, wrap, chunk_to_i64_masked);
}

/// See [`super::convert_column_with`].
pub fn convert_column_i32<T>(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    out: &mut Vec<T>,
    wrap: impl Fn(i32) -> T,
) {
    super::convert_column_with(raw_bits, valid, out, wrap, chunk_to_i32_masked);
}

/// See [`super::gather_missing_with`].
pub fn gather_missing(
    page: &[u8],
    base: usize,
    stride: usize,
    len: usize,
    raw_bits: &mut Vec<u64>,
) -> bool {
    super::gather_missing_with(page, base, stride, len, raw_bits, any_missing_x8)
}
