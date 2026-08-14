//! Differential tests: the live backend must agree with [`scalar`] on every input.
//!
//! Only one vector backend compiles at a time, so this compares the selected one
//! against the scalar reference. Under the default feature set that is
//! `fearless` vs `scalar`; with `--features nightly-simd` it is `portable` vs
//! `scalar`; with `--no-default-features` it is a tautology, which is fine — the
//! point is that whichever backend ships was checked against the reference.
//!
//! These kernels decide column types and null masks, so a divergence would be
//! silent rather than loud. That is what justifies testing them separately from
//! the reader's own end-to-end tests.

use super::{backend, scalar};

/// Deterministic LCG. Reproducible failures matter more here than distribution
/// quality, and it keeps `rand` out of the dependency tree.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

/// Bit patterns chosen to sit on every boundary the kernels branch on.
fn interesting_bits() -> Vec<u64> {
    let mut values: Vec<u64> = vec![
        0.0_f64.to_bits(),
        (-0.0_f64).to_bits(),
        1.0_f64.to_bits(),
        (-1.0_f64).to_bits(),
        0.5_f64.to_bits(),    // non-integral
        (-0.5_f64).to_bits(), // non-integral, negative
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        f64::NAN.to_bits(),
        0x7FF0_0000_0000_0001, // SAS missing `.`
        0xFFF0_0000_0000_0001, // negative-signed NaN
        0x7FF8_0000_0000_0000, // quiet NaN
        f64::from(i32::MAX).to_bits(),
        f64::from(i32::MIN).to_bits(),
        (f64::from(i32::MAX) + 1.0).to_bits(),
        (f64::from(i32::MIN) - 1.0).to_bits(),
        9_007_199_254_740_992.0_f64.to_bits(), // 2^53
        1e300_f64.to_bits(),
        (-1e300_f64).to_bits(),
        f64::MIN_POSITIVE.to_bits(),
    ];
    let mut rng = Lcg(0xA5A5_1234);
    for _ in 0..400 {
        let raw = rng.next();
        // Mostly plausible integral values, with a minority of raw bit soup.
        if raw.is_multiple_of(4) {
            values.push(raw);
        } else {
            #[allow(clippy::cast_possible_truncation)]
            let n = (raw >> 32) as i32;
            values.push(f64::from(n).to_bits());
        }
    }
    values
}

#[test]
fn missing_bitmask_matches_scalar() {
    let values = interesting_bits();
    for window in values.windows(8) {
        let chunk: [u64; 8] = window.try_into().expect("windows(8)");
        assert_eq!(
            backend::missing_bitmask_x8(&chunk),
            scalar::missing_bitmask_x8(&chunk),
            "missing_bitmask_x8 diverged on {chunk:016X?}"
        );
        assert_eq!(
            backend::any_missing_x8(&chunk),
            scalar::any_missing_x8(&chunk),
            "any_missing_x8 diverged on {chunk:016X?}"
        );
        // The two must also agree with each other.
        assert_eq!(
            backend::any_missing_x8(&chunk),
            backend::missing_bitmask_x8(&chunk) != 0
        );
    }
}

#[test]
fn first_non_integral_matches_scalar() {
    let values = interesting_bits();
    let ranges = [
        (f64::from(i32::MIN), f64::from(i32::MAX)),
        #[allow(clippy::cast_precision_loss)]
        (i64::MIN as f64, i64::MAX as f64),
    ];

    // Exercise every length so the 4-lane remainder path is covered too.
    for len in 0..values.len().min(64) {
        let slice = &values[..len];
        for (min, max) in ranges {
            assert_eq!(
                backend::first_non_integral_in_range_index(slice, None, min, max),
                scalar::first_non_integral_in_range_index(slice, None, min, max),
                "len {len}, range {min}..={max}, no validity"
            );

            // With a validity vector that nulls out every third row — nulls are
            // skipped, so they can mask an otherwise-failing cell.
            let words = len.div_ceil(64).max(1);
            let mut validity = vec![0u64; words];
            for i in 0..len {
                if i % 3 != 0 {
                    validity[i / 64] |= 1u64 << (i % 64);
                }
            }
            assert_eq!(
                backend::first_non_integral_in_range_index(slice, Some(&validity), min, max),
                scalar::first_non_integral_in_range_index(slice, Some(&validity), min, max),
                "len {len}, range {min}..={max}, with validity"
            );
        }
    }
}

#[test]
fn chunk_conversions_match_scalar() {
    // Only in-range integral values: the callers guarantee this, and out-of-range
    // float-to-int conversion is deliberately not part of the contract.
    let mut rng = Lcg(0xBEEF);
    for _ in 0..500 {
        let mut chunk = [0u64; 8];
        for slot in &mut chunk {
            #[allow(clippy::cast_possible_truncation)]
            let v = i64::from((rng.next() >> 40) as i32);
            #[allow(clippy::cast_precision_loss)]
            let f = v as f64;
            *slot = f.to_bits();
        }
        #[allow(clippy::cast_possible_truncation)]
        let valid_byte = rng.next() as u8;

        assert_eq!(backend::chunk_to_i64(&chunk), scalar::chunk_to_i64(&chunk));
        assert_eq!(backend::chunk_to_i32(&chunk), scalar::chunk_to_i32(&chunk));
        assert_eq!(
            backend::chunk_to_i64_masked(&chunk, valid_byte),
            scalar::chunk_to_i64_masked(&chunk, valid_byte),
            "i64 masked diverged, valid_byte {valid_byte:08b}"
        );
        assert_eq!(
            backend::chunk_to_i32_masked(&chunk, valid_byte),
            scalar::chunk_to_i32_masked(&chunk, valid_byte),
            "i32 masked diverged, valid_byte {valid_byte:08b}"
        );
    }
}

#[test]
fn masked_conversion_zeroes_null_lanes() {
    let chunk = [7.0_f64.to_bits(); 8];
    assert_eq!(backend::chunk_to_i64_masked(&chunk, 0b0000_0000), [0i64; 8]);
    assert_eq!(backend::chunk_to_i64_masked(&chunk, 0b1111_1111), [7i64; 8]);
    assert_eq!(
        backend::chunk_to_i64_masked(&chunk, 0b1010_1010),
        [0, 7, 0, 7, 0, 7, 0, 7]
    );
    assert_eq!(
        backend::chunk_to_i32_masked(&chunk, 0b0000_0011),
        [7, 7, 0, 0, 0, 0, 0, 0]
    );
}

/// Byte strings that straddle the 64-byte vector width in both kernels.
fn interesting_strings() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut rng = Lcg(0xFEED);

    for len in [0usize, 1, 7, 8, 12, 63, 64, 65, 127, 128, 129, 200, 256] {
        // All padding — the whole-chunk skip path.
        out.push(vec![b' '; len]);
        out.push(vec![0u8; len]);
        // No padding at all — the early-return path.
        out.push(vec![b'x'; len]);

        // Content then padding, at several content lengths.
        for content in [0usize, 1, 63, 64] {
            if content > len {
                continue;
            }
            let mut v = vec![b' '; len];
            for slot in v.iter_mut().take(content) {
                #[allow(clippy::cast_possible_truncation)]
                let byte = b'a' + (rng.next() % 26) as u8;
                *slot = byte;
            }
            out.push(v);
        }

        // Non-ASCII in varying positions, including the final byte.
        for pos in [0usize, 1, 63, 64] {
            if pos >= len {
                continue;
            }
            let mut v = vec![b'a'; len];
            v[pos] = 0xC3;
            out.push(v.clone());
            let mut w = vec![b'a'; len];
            w[len - 1] = 0xFF;
            out.push(w);
        }

        // Interior NULs, which trim treats as padding but only from the right.
        if len >= 4 {
            let mut v = vec![b'a'; len];
            v[len / 2] = 0;
            v[len - 1] = 0;
            out.push(v);
        }
    }
    out
}

#[test]
fn string_kernels_match_scalar() {
    for bytes in interesting_strings() {
        assert_eq!(
            backend::trim_trailing_space_or_nul_wide(&bytes),
            scalar::trim_trailing_space_or_nul_wide(&bytes),
            "trim diverged on len {} ({:?}…)",
            bytes.len(),
            &bytes[..bytes.len().min(16)]
        );
        assert_eq!(
            backend::is_ascii_wide(&bytes),
            scalar::is_ascii_wide(&bytes),
            "is_ascii diverged on len {}",
            bytes.len()
        );
        // The vector kernels must also agree with the standard library.
        assert_eq!(backend::is_ascii_wide(&bytes), bytes.is_ascii());
    }
}

#[test]
fn classify_missing_matches_a_naive_reference() {
    let values = interesting_bits();
    for len in [0usize, 1, 7, 8, 63, 64, 65, 127, 128, 200, 419] {
        let slice = &values[..len.min(values.len())];
        let got = super::classify_missing_raw_bits(slice);

        let any_missing = slice.iter().copied().any(super::bits_is_missing);
        assert_eq!(
            got.is_some(),
            any_missing,
            "len {len}: validity vector should be present iff a row is missing"
        );

        if let Some(validity) = got {
            assert_eq!(validity.len(), slice.len().div_ceil(64), "len {len}");
            for (i, &bits) in slice.iter().enumerate() {
                assert_eq!(
                    super::valid_bit(&validity, i),
                    !super::bits_is_missing(bits),
                    "len {len}, row {i}"
                );
            }
        }
    }
}
