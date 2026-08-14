//! The vectorized kernels, behind one backend-agnostic boundary.
//!
//! Three implementations exist and exactly one is live:
//!
//! | backend    | feature        | needs        | notes                                    |
//! |------------|----------------|--------------|------------------------------------------|
//! | [`scalar`] | *(none)*       | stable, 1.88 | Always compiled. The correctness oracle. |
//! | [`fearless`] | `simd`       | stable, 1.89 | Runtime-dispatched. The default.         |
//! | [`portable`] | `nightly-simd` | nightly    | `std::simd`. Overrides `simd`.           |
//!
//! The scalar figure is the crate's own floor (`slice::as_chunks`, stable in 1.88),
//! not something the backend chooses; leaving `simd` off only avoids
//! `fearless_simd`'s 1.89. One version between them, so a low MSRV is a weak reason
//! to pick `scalar` — see below for the real ones.
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
//! # Kernel rankings do not survive a change of architecture
//!
//! On aarch64/NEON at baseline, plain stable Rust is *faster* than `std::simd` on
//! [`missing_bitmask_x8`] — by about a third — because NEON has no `movemask` and
//! `to_bitmask()` lowers to a multi-instruction emulation that LLVM's autovectoriser
//! routes around.
//!
//! On x86-64 that reverses: scalar is the *slowest* of the three there
//! (9.1 GiB/s, against 22.7 for `fearless` and 10.9 for `portable`, measured at
//! baseline on a Zen 3 EPYC). Take no ranking here as architecture-independent.
//!
//! What does hold across both: the narrow-string kernels are best left to `scalar`,
//! which is why [`crate::scan::string`] gates on `< 64` rather than calling a
//! backend; and the ≥64-byte string kernels are a clear vector win.
//!
//! # Known: `dispatch!` is paid per call
//!
//! [`missing_bitmask_x8`] and [`chunk_to_i64_masked`] are invoked once per 8 rows, so
//! the `fearless` backend pays a runtime dispatch per chunk — 6.8 GiB/s against
//! `scalar`'s 55.9 on `chunk_to_i64_masked`. The coarse kernels dispatch once per
//! array or per row and show no such penalty, and `fearless` leads them.
//!
//! The fix is to hoist dispatch out of the per-chunk loops: give the backend a coarse
//! entry point that dispatches once and iterates inside, instead of
//! [`classify_missing_raw_bits`] calling a dispatched per-chunk kernel. Not yet done.
//!
//! Run `scripts/simd-matrix.sh`, or the `simd-matrix` workflow, to re-measure. AVX-512
//! is still unmeasured — GitHub's runners are Zen 3.

// These are internal kernels, `pub` only so the `simd_backends` benchmark can
// reach them under `internal-bench` — the module itself is private otherwise.
// `#[must_use]` is API hygiene for a real public surface; this is not one.
#![allow(clippy::must_use_candidate)]
// `#[inline(always)]` on the shared column loops is the mechanism, not a hint:
// it is what gets their bodies instantiated *inside* a backend's selected
// `#[target_feature]` implementation. Without it the loop stays outside and
// calls back in once per chunk, which is the cost these exist to remove.
#![allow(clippy::inline_always)]

pub mod scalar;

#[cfg(all(feature = "simd", not(feature = "nightly-simd")))]
pub mod fearless;

#[cfg(feature = "nightly-simd")]
pub mod portable;

#[cfg(feature = "nightly-simd")]
pub use portable as backend;

#[cfg(all(feature = "simd", not(feature = "nightly-simd")))]
pub use fearless as backend;

#[cfg(not(any(feature = "simd", feature = "nightly-simd")))]
pub use scalar as backend;

// The per-chunk kernels. Production goes through the column entry points below
// instead, so most of these have no caller outside `tests` and the `simd_backends`
// benchmark — which is where they earn their keep, since they are the unit the
// differential tests compare backend against backend.
#[allow(unused_imports)]
pub use backend::{
    any_missing_x8, chunk_to_i32, chunk_to_i32_masked, chunk_to_i64, chunk_to_i64_masked,
    first_non_integral_in_range_index, is_ascii_wide, missing_bitmask_x8,
    trim_trailing_space_or_nul_wide,
};

// The column-level entry points. These exist so a runtime-dispatching backend can
// dispatch *once* and run the whole loop inside the selected implementation. The
// per-chunk kernels above are still the unit of work, but going through them one
// call at a time costs a cross-`#[target_feature]` call per 8 rows, which no
// optimiser can remove — measured at 6.8 GiB/s against scalar's 55.9 before this
// existed. Call these, not the per-chunk kernels, from the scan pipeline.
pub use backend::{
    classify_missing_raw_bits, convert_column_i32, convert_column_i64, gather_missing,
};

/// IEEE-754 double exponent field: all ones means Inf or NaN.
pub const NUMERIC_EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
/// IEEE-754 double fraction field: non-zero alongside a full exponent means NaN,
/// which is how SAS encodes its missing values (`.`, `.A`–`.Z`, `._`).
pub const NUMERIC_FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;

/// Bit `i` of a packed validity vector: 1 when row `i` is present.
#[inline]
pub const fn valid_bit(validity: &[u64], index: usize) -> bool {
    (validity[index / 64] >> (index % 64)) & 1 == 1
}

/// The 8 validity bits covering `chunk_index`'s rows, as a byte. Bit `i` is 1 when
/// lane `i` is present.
#[inline]
#[allow(clippy::cast_possible_truncation)]
pub fn validity_byte(validity: &[u64], chunk_index: usize) -> u8 {
    let bit_base = chunk_index * 8;
    (validity[bit_base / 64] >> (bit_base % 64)) as u8
}

/// A SAS missing sentinel: exponent all ones with a non-zero fraction.
///
/// The scalar reference for every backend's vectorized form of the same test.
#[inline]
pub const fn bits_is_missing(bits: u64) -> bool {
    (bits & NUMERIC_EXP_MASK) == NUMERIC_EXP_MASK && (bits & NUMERIC_FRACTION_MASK) != 0
}

/// Bit-packed validity for `raw_bits`: bit `i % 64` of word `i / 64` is 1 when row
/// `i` is present. `None` when every row is present, which lets a column with no
/// missing values skip allocating a validity vector at all.
///
/// Generic over the per-chunk test so the loop is written once and each backend
/// decides where its dispatch sits — `#[inline(always)]` so a dispatching backend
/// gets this whole body instantiated *inside* its selected implementation.
#[inline(always)]
pub fn classify_missing_with<F>(raw_bits: &[u64], mut missing_bitmask: F) -> Option<Vec<u64>>
where
    F: FnMut(&[u64; 8]) -> u8,
{
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
            let missing = missing_bitmask(sub_chunk);
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
            let missing = missing_bitmask(sub_chunk);
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

/// Convert a staged numeric column, appending each value through `wrap`.
///
/// `convert` takes a chunk and the 8 validity bits covering it, and returns the
/// converted lanes with null lanes zeroed; an all-ones byte means "all present", so
/// one closure serves both the validity and no-validity paths.
///
/// Callers guarantee every present lane is a finite integer in range, via
/// [`first_non_integral_in_range_index`].
#[inline(always)]
pub fn convert_column_with<N, T, F>(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    out: &mut Vec<T>,
    wrap: impl Fn(N) -> T,
    mut convert: F,
) where
    N: Copy,
    F: FnMut(&[u64; 8], u8) -> [N; 8],
{
    let (chunks, remainder) = raw_bits.as_chunks::<8>();
    for (chunk_index, chunk) in chunks.iter().enumerate() {
        let valid_byte = valid.map_or(0xFF, |v| validity_byte(v, chunk_index));
        out.extend(convert(chunk, valid_byte).map(&wrap));
    }

    // The sub-8 tail, one lane at a time. Routed through the same `convert` so the
    // conversion semantics cannot drift between the body and the tail.
    if !remainder.is_empty() {
        let processed = raw_bits.len() - remainder.len();
        let mut padded = [0u64; 8];
        padded[..remainder.len()].copy_from_slice(remainder);
        let mut valid_byte = 0u8;
        for offset in 0..remainder.len() {
            let present = valid.is_none_or(|v| valid_bit(v, processed + offset));
            if present {
                valid_byte |= 1 << offset;
            }
        }
        let converted = convert(&padded, valid_byte);
        for value in converted.iter().take(remainder.len()) {
            out.push(wrap(*value));
        }
    }
}

/// Strided gather of `len` full-width little-endian numerics, with the SAS-missing
/// test vectorized across each group of 8. Returns whether any lane was missing.
///
/// The strided loads themselves stay scalar — portable SIMD has no byte-offset
/// gather and the dominant targets lack a hardware one — but this removes the
/// per-cell branch and per-element push. The loop lives here so a dispatching
/// backend wraps the whole thing rather than one group at a time.
///
/// # Panics
///
/// If `page` is too short for `len` cells at `stride` from `base`. The caller is the
/// batch decoder, which derives all three from the page's own row layout.
#[inline(always)]
pub fn gather_missing_with<F>(
    page: &[u8],
    mut base: usize,
    stride: usize,
    len: usize,
    raw_bits: &mut Vec<u64>,
    mut any_missing: F,
) -> bool
where
    F: FnMut(&[u64; 8]) -> bool,
{
    const LANES: usize = 8;
    let mut missing = false;
    let mut i = 0;
    while i + LANES <= len {
        let mut lane = [0u64; LANES];
        for (l, slot) in lane.iter_mut().enumerate() {
            let off = base + l * stride;
            *slot = u64::from_le_bytes(page[off..off + 8].try_into().expect("8-byte field"));
        }
        missing |= any_missing(&lane);
        raw_bits.extend_from_slice(&lane);
        base += LANES * stride;
        i += LANES;
    }
    while i < len {
        let raw = u64::from_le_bytes(page[base..base + 8].try_into().expect("8-byte field"));
        missing |= bits_is_missing(raw);
        raw_bits.push(raw);
        base += stride;
        i += 1;
    }
    missing
}

#[cfg(test)]
mod tests;
