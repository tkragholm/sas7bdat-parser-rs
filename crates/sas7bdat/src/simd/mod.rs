//! The vectorized kernels, behind one backend-agnostic boundary.
//!
//! Three implementations exist and exactly one is live:
//!
//! | backend    | feature        | needs        | notes                                    |
//! |------------|----------------|--------------|------------------------------------------|
//! | [`scalar`] | *(none)*       | stable, 1.81 | Always compiled. The correctness oracle. |
//! | [`fearless`] | `simd`       | stable, 1.89 | Runtime-dispatched. The default.         |
//! | [`portable`] | `nightly-simd` | nightly    | `std::simd`. Overrides `simd`.           |
//!
//! `nightly-simd` wins when both are on, so `--all-features` resolves to a single
//! coherent backend rather than failing to compile.
//!
//! # Why the default is not `std::simd`
//!
//! `std::simd` has no runtime dispatch: it lowers to whatever target features were
//! enabled at compile time, which for a portable binary on x86-64 is SSE2. Every
//! 512-bit logical vector in here is then split into four 128-bit operations. The
//! repository's `.cargo/config.toml` raises that to `x86-64-v3` for its own builds,
//! but cargo only finds that file by walking up from the working directory — a crate
//! consumed from crates.io, a wheel, or an R package tarball never sees it.
//! `fearless_simd` detects AVX2/AVX-512 at runtime instead, so a portable build gets
//! the wide path without the caller setting anything.
//!
//! # Why `scalar` is not merely a fallback
//!
//! Measured on aarch64/NEON at baseline, plain stable Rust is *faster* than
//! `std::simd` on [`missing_bitmask_x8`] — by about a third — because NEON has no
//! `movemask` and `to_bitmask()` lowers to a multi-instruction emulation that LLVM's
//! autovectoriser routes around. The narrow-string kernels are within noise across
//! all three. SIMD's clear wins are the ≥64-byte string kernels (2–3×) and, for
//! `fearless_simd`, [`first_non_integral_in_range_index`].
//!
//! These rankings are aarch64-only so far and the x86-64 numbers are expected to
//! differ — on AVX-512 `to_bitmask` is a native `kmov`. Until that is measured, every
//! kernel keeps all three implementations rather than hard-coding a winner.

pub(crate) mod scalar;

#[cfg(all(feature = "simd", not(feature = "nightly-simd")))]
pub(crate) mod fearless;

#[cfg(feature = "nightly-simd")]
pub(crate) mod portable;

#[cfg(feature = "nightly-simd")]
pub(crate) use portable as backend;

#[cfg(all(feature = "simd", not(feature = "nightly-simd")))]
pub(crate) use fearless as backend;

#[cfg(not(any(feature = "simd", feature = "nightly-simd")))]
pub(crate) use scalar as backend;

pub(crate) use backend::{
    any_missing_x8, chunk_to_i32, chunk_to_i32_masked, chunk_to_i64, chunk_to_i64_masked,
    first_non_integral_in_range_index, is_ascii_wide, missing_bitmask_x8,
    trim_trailing_space_or_nul_wide,
};

/// IEEE-754 double exponent field: all ones means Inf or NaN.
pub(crate) const NUMERIC_EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
/// IEEE-754 double fraction field: non-zero alongside a full exponent means NaN,
/// which is how SAS encodes its missing values (`.`, `.A`–`.Z`, `._`).
pub(crate) const NUMERIC_FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;

/// Bit `i` of a packed validity vector: 1 when row `i` is present.
#[inline]
pub(crate) const fn valid_bit(validity: &[u64], index: usize) -> bool {
    (validity[index / 64] >> (index % 64)) & 1 == 1
}

/// The 8 validity bits covering `chunk_index`'s rows, as a byte. Bit `i` is 1 when
/// lane `i` is present.
#[inline]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn validity_byte(validity: &[u64], chunk_index: usize) -> u8 {
    let bit_base = chunk_index * 8;
    (validity[bit_base / 64] >> (bit_base % 64)) as u8
}

/// A SAS missing sentinel: exponent all ones with a non-zero fraction.
///
/// The scalar reference for every backend's vectorized form of the same test.
#[inline]
pub(crate) const fn bits_is_missing(bits: u64) -> bool {
    (bits & NUMERIC_EXP_MASK) == NUMERIC_EXP_MASK && (bits & NUMERIC_FRACTION_MASK) != 0
}

/// Bit-packed validity for `raw_bits`: bit `i % 64` of word `i / 64` is 1 when row
/// `i` is present. `None` when every row is present, which lets a column with no
/// missing values skip allocating a validity vector at all.
///
/// Only the 8-lane test inside is backend-specific, so this outer structure is
/// shared rather than written three times.
pub(crate) fn classify_missing_raw_bits(raw_bits: &[u64]) -> Option<Vec<u64>> {
    let mut valid: Option<Vec<u64>> = None;
    let mut processed_words = 0usize;

    // Groups of 64 rows, each producing one validity word.
    let (chunks64, remainder) = raw_bits.as_chunks::<64>();
    for chunk64 in chunks64 {
        let mut valid_word = 0u64;
        let mut any_missing = false;
        // The outer chunk is exactly 64 wide, so this inner split has no remainder.
        let (sub_chunks, _) = chunk64.as_chunks::<8>();
        for (i, sub_chunk) in sub_chunks.iter().enumerate() {
            let missing = missing_bitmask_x8(sub_chunk);
            valid_word |= u64::from(!missing) << (i * 8);
            any_missing |= missing != 0;
        }

        if any_missing {
            valid
                .get_or_insert_with(|| vec![u64::MAX; processed_words])
                .push(valid_word);
        } else if let Some(valid_vec) = &mut valid {
            valid_vec.push(u64::MAX);
        }
        processed_words += 1;
    }

    // Fewer than 64 rows left, producing one partial validity word.
    if !remainder.is_empty() {
        let mut valid_word = 0u64;
        let mut any_missing = false;
        let mut bit_offset = 0usize;
        let (sub_chunks8, tail) = remainder.as_chunks::<8>();
        for sub_chunk in sub_chunks8 {
            let missing = missing_bitmask_x8(sub_chunk);
            valid_word |= u64::from(!missing) << bit_offset;
            any_missing |= missing != 0;
            bit_offset += 8;
        }
        for &bits in tail {
            if bits_is_missing(bits) {
                any_missing = true;
            } else {
                valid_word |= 1u64 << bit_offset;
            }
            bit_offset += 1;
        }

        if any_missing {
            valid
                .get_or_insert_with(|| vec![u64::MAX; processed_words])
                .push(valid_word);
        } else if let Some(valid_vec) = &mut valid {
            valid_vec.push(valid_word);
        }
    }

    valid
}

#[cfg(test)]
mod tests;
