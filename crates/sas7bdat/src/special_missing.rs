//! SAS special missing values: `.A` to `.Z` and `._`, as distinct from the plain `.`.
//!
//! SAS stores a missing number as a NaN whose byte 5 (bits 47 to 40) says which of `.`,
//! `._` or `.A` to `.Z` it is. Two spellings of that byte occur in files:
//!
//! - ordinal: `0xFF - n` over the sequence `_ . A .. Z`, so `0xFF` is `._`, `0xFE` is
//!   `.`, and `0xFD` down to `0xE4` are `.A` to `.Z`;
//! - complement: the one's complement of the character's ASCII code, so `.` is `0xD1`,
//!   `_` is `0xA0`, and `.A` to `.Z` are `0xBE` down to `0xA5`.
//!
//! Both are decoded. Complement-spelled tags occur in real files: a census of the
//! repository's corpus found 6.3 million such cells across nine files, PIAAC's
//! `.V`/`.N`/`.D`/`.R` scheme among them. The two ranges cannot collide, since the
//! complement family tops out at `0xD1`, below the ordinal family's `0xE4`; the table's
//! `const` builder asserts that, so an overlap fails the build.
//!
//! A scan keeps a missing cell's raw bits in an `F64` column, so the tag can be read
//! back from the value behind the validity mask.

/// Indicator byte to tag character; 0 for the plain `.` and for bytes that are neither.
const MISSING_TAG: [u8; 256] = build_missing_tag_table();

const fn build_missing_tag_table() -> [u8; 256] {
    let mut table = [0u8; 256];

    // Ordinal spelling. `0xFE` (plain `.`) is left 0 deliberately.
    table[0xFF] = b'_';
    let mut i = 0usize;
    while i < 26 {
        assert!(table[0xFD - i] == 0, "special-missing spellings overlap");
        #[allow(clippy::cast_possible_truncation)]
        {
            table[0xFD - i] = b'A' + i as u8;
        }
        i += 1;
    }

    // Complement spelling. `!b'.'` == `0xD1` (plain `.`) is left 0 deliberately.
    assert!(
        table[0xFF - b'_' as usize] == 0,
        "special-missing spellings overlap"
    );
    table[0xFF - b'_' as usize] = b'_';
    let mut c = 0u8;
    while c < 26 {
        let slot = 0xFF - (b'A' + c) as usize;
        assert!(table[slot] == 0, "special-missing spellings overlap");
        table[slot] = b'A' + c;
        c += 1;
    }

    table
}

/// The tag of a SAS special missing value from its raw bits: `'A'` to `'Z'` for `.A`
/// to `.Z`, `'_'` for `._`, and `None` for the plain `.` and for anything that is not a
/// missing value.
///
/// The bits are a missing cell's value as an `F64` column holds it behind its validity
/// mask. Any NaN is accepted, the quiet form (`0x7FF8_..`) some writers use included.
///
/// ```
/// // `.A` as SAS writes it, in the ordinal spelling.
/// assert_eq!(sas7bdat::special_missing_tag(0xFFFF_FD00_0000_0000), Some('A'));
/// assert_eq!(sas7bdat::special_missing_tag(1.5f64.to_bits()), None);
/// ```
#[must_use]
pub const fn special_missing_tag(bits: u64) -> Option<char> {
    const EXPONENT: u64 = 0x7FF0_0000_0000_0000;
    const MANTISSA: u64 = 0x000F_FFFF_FFFF_FFFF;
    if bits & EXPONENT != EXPONENT || bits & MANTISSA == 0 {
        return None;
    }
    match MISSING_TAG[((bits >> 40) & 0xFF) as usize] {
        0 => None,
        tag => Some(tag as char),
    }
}

#[cfg(test)]
mod tests {
    use super::{MISSING_TAG, special_missing_tag};

    /// The raw bits SAS writes for a missing cell with indicator byte `b`, in the
    /// signalling form (`0xFFFF_bb_00..`) both spellings use in practice.
    const fn missing_bits(indicator: u8) -> u64 {
        0xFFFF_0000_0000_0000 | ((indicator as u64) << 40)
    }

    #[test]
    fn the_ordinal_spelling_decodes() {
        assert_eq!(special_missing_tag(missing_bits(0xFF)), Some('_'));
        assert_eq!(special_missing_tag(missing_bits(0xFD)), Some('A'));
        assert_eq!(special_missing_tag(missing_bits(0xFC)), Some('B'));
        assert_eq!(special_missing_tag(missing_bits(0xE4)), Some('Z'));
        // Plain `.` carries no tag.
        assert_eq!(special_missing_tag(missing_bits(0xFE)), None);
    }

    #[test]
    fn the_complement_spelling_decodes() {
        // The bytes observed in the corpus, with the tags haven gives them.
        for (byte, tag) in [
            (0xA9u8, 'V'),
            (0xAA, 'U'),
            (0xAD, 'R'),
            (0xB1, 'N'),
            (0xB2, 'M'),
            (0xBB, 'D'),
            (0xBD, 'B'),
            (0xBE, 'A'),
            (0xA5, 'Z'),
            (0xA0, '_'),
        ] {
            assert_eq!(
                special_missing_tag(missing_bits(byte)),
                Some(tag),
                "byte {byte:#04x}"
            );
        }
        // `!b'.'` is a plain missing, the most common indicator in the corpus.
        assert_eq!(special_missing_tag(missing_bits(0xD1)), None);
    }

    #[test]
    fn the_two_spellings_never_collide() {
        let ordinal = (0xE4u8..=0xFF).collect::<Vec<_>>();
        let complement: Vec<u8> = (b'A'..=b'Z').chain(*b"_.").map(|c| 0xFF - c).collect();
        for b in &complement {
            assert!(
                !ordinal.contains(b),
                "byte {b:#04x} is claimed by both spellings"
            );
        }
        for b in 0..=u8::MAX {
            if !ordinal.contains(&b) && !complement.contains(&b) {
                assert_eq!(
                    MISSING_TAG[b as usize], 0,
                    "byte {b:#04x} should be untagged"
                );
            }
        }
    }

    #[test]
    fn the_quiet_nan_spelling_is_accepted() {
        // `many_columns.sas7bdat` writes `._` as a quiet NaN.
        assert_eq!(special_missing_tag(0x7FF8_FF00_0000_0000), Some('_'));
    }

    #[test]
    fn non_nan_bits_are_never_tagged() {
        assert_eq!(special_missing_tag(0f64.to_bits()), None);
        assert_eq!(special_missing_tag(1.5f64.to_bits()), None);
        assert_eq!(special_missing_tag(f64::INFINITY.to_bits()), None);
        assert_eq!(special_missing_tag(f64::NEG_INFINITY.to_bits()), None);
    }
}
