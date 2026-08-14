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

pub(super) fn maybe_fix_mojibake(value: String, policy: MojibakePolicy) -> String {
    if !matches!(policy, MojibakePolicy::Auto) || value.is_ascii() {
        return value;
    }
    if !(value.contains("Ã") || value.contains("Â")) {
        return value;
    }
    let mut bytes = Vec::with_capacity(value.len());
    for ch in value.chars() {
        let code = u32::from(ch);
        let Ok(byte) = u8::try_from(code) else {
            return value;
        };
        bytes.push(byte);
    }
    match std::str::from_utf8(&bytes) {
        Ok(decoded) if decoded != value => decoded.to_owned(),
        _ => value,
    }
}

#[inline]
pub(super) fn mojibake_fix_maybe_needed_for_encoded_bytes(
    encoding: &'static Encoding,
    slice: &[u8],
    policy: MojibakePolicy,
) -> bool {
    matches!(policy, MojibakePolicy::Auto)
        && encoding.is_single_byte()
        && memchr::memchr2(0xC2, 0xC3, slice).is_some()
}
