use simdutf8::basic::from_utf8 as simd_from_utf8;

use super::{Encoding, MojibakePolicy, TrimMode, TrimmedString};
use crate::simd::scalar::{
    is_ascii_wide as is_ascii_word,
    trim_trailing_space_or_nul_wide as trim_trailing_space_or_nul_word,
};
use crate::simd::{is_ascii_wide, trim_trailing_space_or_nul_wide};

const SPACES_HEAD_12: u64 = u64::from_ne_bytes([b' '; 8]);
const SPACES_TAIL_12: u32 = u32::from_ne_bytes([b' '; 4]);

#[inline]
pub(super) fn trim_and_classify_ascii(slice: &[u8]) -> TrimmedString<'_> {
    if slice.len() == 12 && is_all_space_or_nul_12(slice) {
        return TrimmedString {
            bytes: &[],
            is_ascii: true,
        };
    }

    if slice.len() < 64 {
        let trimmed = trim_trailing_space_or_nul_word(slice);
        return TrimmedString {
            bytes: trimmed,
            is_ascii: is_ascii_word(trimmed),
        };
    }

    let trimmed = trim_trailing_space_or_nul_wide(slice);
    TrimmedString {
        bytes: trimmed,
        is_ascii: is_ascii_wide(trimmed),
    }
}

#[inline]
pub(super) fn trim_and_classify_for_mode(slice: &[u8], mode: TrimMode) -> TrimmedString<'_> {
    match mode {
        TrimMode::Preserve => TrimmedString {
            bytes: slice,
            is_ascii: if slice.len() < 64 {
                is_ascii_word(slice)
            } else {
                is_ascii_wide(slice)
            },
        },
        TrimMode::RTrim => trim_and_classify_ascii(slice),
        TrimMode::Strip => {
            let mut start = 0usize;
            while start < slice.len() {
                let byte = slice[start];
                if byte != b' ' && byte != 0 {
                    break;
                }
                start += 1;
            }
            let trimmed = trim_trailing_space_or_nul_word(&slice[start..]);
            TrimmedString {
                bytes: trimmed,
                is_ascii: if trimmed.len() < 64 {
                    is_ascii_word(trimmed)
                } else {
                    is_ascii_wide(trimmed)
                },
            }
        }
    }
}

#[inline]
pub(super) fn is_blank_after_trim_mode(slice: &[u8], mode: TrimMode) -> bool {
    match mode {
        TrimMode::Preserve => !slice.is_empty() && slice.iter().all(|&b| b == b' '),
        TrimMode::RTrim | TrimMode::Strip => slice.is_empty(),
    }
}

#[inline]
fn is_all_space_or_nul_12(slice: &[u8]) -> bool {
    debug_assert_eq!(slice.len(), 12);
    let head = u64::from_ne_bytes(slice[..8].try_into().expect("fixed-width head"));
    let tail = u32::from_ne_bytes(slice[8..12].try_into().expect("fixed-width tail"));

    (head == 0 && tail == 0) || (head == SPACES_HEAD_12 && tail == SPACES_TAIL_12)
}

/// Recover the text of a cell whose bytes are really UTF-8 but were declared as
/// `encoding`, or `None` when that is not what happened.
///
/// Decoding UTF-8 bytes with a single-byte encoding turns "Ø" into "Ã˜" and "ø" into
/// "Ã¸": every two-byte sequence keeps its `0xC2`/`0xC3` lead byte, which is what the
/// probe below looks for. The repair is then to read the bytes as the UTF-8 they
/// already are, so it borrows the row and allocates nothing.
///
/// This used to run the other way, walking the *decoded* string back to bytes with
/// `u8::try_from`. That silently gave up on every character whose second UTF-8 byte
/// lands in `0x80..=0x9F`, because windows-1252 decodes that band to typographic
/// characters above U+00FF — a small tilde for "Ø", an ellipsis for "Å", a dagger for
/// "Æ". Those bytes are exactly the codepoints `U+00C0..=U+00DF`, so the entire
/// uppercase half of Latin-1 was left mangled while the lowercase half was repaired,
/// which is worse than repairing neither: one column came back holding both
/// "Nørrebro" and "KÃ˜BENHAVN".
///
/// Some byte strings are legitimate single-byte text *and* valid UTF-8 — "Ã¸" in
/// windows-1252 is "ø" in UTF-8 — and nothing in the bytes tells the two apart. This
/// keeps the older code's answer and prefers the UTF-8 reading.
#[inline]
pub(super) fn mojibake_repaired<'a>(
    encoding: &'static Encoding,
    slice: &'a [u8],
    policy: MojibakePolicy,
) -> Option<&'a str> {
    if !matches!(policy, MojibakePolicy::Auto) || !encoding.is_single_byte() {
        return None;
    }
    // Cheap rejection first: without a two-byte lead there is no mojibake to undo,
    // and this runs on every non-ASCII cell of an encoded scan.
    memchr::memchr2(0xC2, 0xC3, slice)?;
    simd_from_utf8(slice).ok()
}
