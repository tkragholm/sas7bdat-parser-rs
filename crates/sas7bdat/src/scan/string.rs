use simdutf8::basic::from_utf8 as simd_from_utf8;

use super::{Encoding, MojibakePolicy, TrimMode, TrimmedString};
use crate::simd::scalar::{
    is_ascii_wide as is_ascii_word,
    trim_trailing_space_or_nul_wide as trim_trailing_space_or_nul_word,
};
use crate::simd::{is_ascii_wide, trim_trailing_space_or_nul_wide};

const SPACES_HEAD_12: u64 = u64::from_ne_bytes([b' '; 8]);
const SPACES_TAIL_12: u32 = u32::from_ne_bytes([b' '; 4]);

// ---------------------------------------------------------------- narrow SWAR kernel
//
// Cells of at most 16 bytes, which is every string column in a register extract and most
// of any other corpus, are trimmed and classified with two word loads and no loops.
//
// The shipped generic path costs about 4.4 ns per cell: a chunk loop that walks backwards
// eight bytes at a time, a byte-at-a-time walk once it lands in the word holding the last
// content byte, then a second pass over the trimmed bytes for the ASCII test. This does the
// same work in about 2.3 ns, measured over a simulated batch at real widths and offsets.
//
// The ASCII test rides along for free here. It is taken over the *padded* words rather than
// the trimmed bytes, which is the same answer because trimming removes only spaces and NULs
// and the pad byte is a space, all of which are ASCII. Computing it separately over the
// untrimmed slice was tried with the generic kernels and lost 10-14%, because there it meant
// a second pass over three times the bytes; here it is two OR operations on words already
// in registers.

const SWAR_ONES: u64 = 0x0101_0101_0101_0101;
const SWAR_HIGH: u64 = 0x8080_8080_8080_8080;
const SWAR_SPACES: u64 = u64::from_ne_bytes([b' '; 8]);

/// The widest cell the narrow kernel handles: two words.
const SWAR_MAX_WIDTH: usize = 16;

/// `0x80` in every byte of `word` that is a space or a NUL, `0x00` elsewhere.
///
/// Two applications of the standard zero-byte trick, one against the word itself for NULs
/// and one against the word XOR spaces.
#[inline]
const fn swar_pad_mask(word: u64) -> u64 {
    let nul = word.wrapping_sub(SWAR_ONES) & !word & SWAR_HIGH;
    let spaced = word ^ SWAR_SPACES;
    let space = spaced.wrapping_sub(SWAR_ONES) & !spaced & SWAR_HIGH;
    nul | space
}

/// The eight bytes starting at `offset`, which the caller guarantees are present.
#[inline]
fn swar_word_at(slice: &[u8], offset: usize) -> u64 {
    let chunk = &slice[offset..offset + 8];
    u64::from_le_bytes(chunk.try_into().expect("eight bytes"))
}

/// Index one past the last non-padding byte of a word, or `None` when it is all padding.
#[inline]
const fn swar_content_end(word: u64) -> Option<usize> {
    let content = !swar_pad_mask(word) & SWAR_HIGH;
    if content == 0 {
        return None;
    }
    // Little-endian: byte `i` occupies bits `8i..8i+8`, so the highest set high-bit is the
    // last content byte.
    Some(7 - (content.leading_zeros() as usize / 8) + 1)
}

/// Trim and classify a cell of 8 to [`SWAR_MAX_WIDTH`] bytes.
///
/// Two loads, both whole: the first eight bytes, and the *last* eight. For a 16-byte cell
/// those are disjoint; for anything narrower they overlap, which costs nothing. Reading the
/// tail rather than padding a buffer is what keeps a 10 or 12 byte cell as cheap as a
/// 16-byte one, and padding was measurably worse than the scan it replaced.
#[inline]
fn swar_trim_and_classify(slice: &[u8]) -> TrimmedString<'_> {
    debug_assert!((8..=SWAR_MAX_WIDTH).contains(&slice.len()));
    let tail_offset = slice.len() - 8;
    let low = swar_word_at(slice, 0);
    let tail = swar_word_at(slice, tail_offset);

    // Every byte of the cell is in one word or the other, so the OR covers all of them.
    let is_ascii = ((low | tail) & SWAR_HIGH) == 0;

    // The last content byte is in the tail unless the tail is all padding, in which case it
    // is in the low word, which for these widths reaches back past the tail's start.
    let end = swar_content_end(tail).map_or_else(
        || swar_content_end(low).unwrap_or_default(),
        |offset| tail_offset + offset,
    );
    TrimmedString {
        bytes: &slice[..end],
        is_ascii,
    }
}

#[inline]
pub(super) fn trim_and_classify_ascii(slice: &[u8]) -> TrimmedString<'_> {
    if slice.len() == 12 && is_all_space_or_nul_12(slice) {
        return TrimmedString {
            bytes: &[],
            is_ascii: true,
        };
    }

    // Only when at least the low word loads whole. Below eight bytes every load would go
    // through the padding buffer, and for the one and two byte cells that dominate survey
    // data that copy costs far more than the scan it replaces: gating on `<= 16` alone
    // measured 13-16% slower on two such fixtures while making register files 14% faster.
    if (8..=SWAR_MAX_WIDTH).contains(&slice.len()) {
        return swar_trim_and_classify(slice);
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
