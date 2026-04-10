use super::{Encoding, MojibakePolicy, Simd, SimdPartialEq, SimdUint, TrimmedString};

const SPACES_HEAD_12: u64 = u64::from_ne_bytes([b' '; 8]);
const SPACES_TAIL_12: u32 = u32::from_ne_bytes([b' '; 4]);
const ASCII_HIGH_BITS_8: u64 = 0x8080_8080_8080_8080;

#[inline]
pub(super) fn trim_trailing_space_or_nul(slice: &[u8]) -> &[u8] {
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

    let trimmed = trim_trailing_space_or_nul_simd(slice);
    TrimmedString {
        bytes: trimmed,
        is_ascii: is_ascii_simd(trimmed),
    }
}

#[inline]
fn is_all_space_or_nul_12(slice: &[u8]) -> bool {
    debug_assert_eq!(slice.len(), 12);
    let head = u64::from_ne_bytes(slice[..8].try_into().expect("fixed-width head"));
    let tail = u32::from_ne_bytes(slice[8..12].try_into().expect("fixed-width tail"));

    (head == 0 && tail == 0) || (head == SPACES_HEAD_12 && tail == SPACES_TAIL_12)
}

#[inline]
fn all_space_or_nul_8(chunk: &[u8]) -> bool {
    debug_assert_eq!(chunk.len(), 8);
    let word = u64::from_ne_bytes(chunk.try_into().expect("8-byte chunk"));
    (word & !SPACES_HEAD_12) == 0
}

#[inline]
fn trim_trailing_space_or_nul_word(slice: &[u8]) -> &[u8] {
    let mut end = slice.len();
    while end >= 8 {
        let start = end - 8;
        let chunk = &slice[start..end];
        if all_space_or_nul_8(chunk) {
            end = start;
            continue;
        }

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

#[inline(always)]
pub(super) fn trim_trailing_space_or_nul_simd(slice: &[u8]) -> &[u8] {
    type U8x64 = Simd<u8, 64>;

    let mut end = slice.len();
    let spaces = U8x64::splat(b' ');
    let nuls = U8x64::splat(0);

    while end >= 64 {
        let start = end - 64;
        let chunk = U8x64::from_slice(&slice[start..end]);
        let trim_mask = chunk.simd_eq(spaces) | chunk.simd_eq(nuls);
        if trim_mask.to_bitmask() == u64::MAX {
            end = start;
            continue;
        }

        let tail = &slice[start..end];
        let mut local_end = tail.len();
        while local_end > 0 {
            let byte = tail[local_end - 1];
            if byte != b' ' && byte != 0 {
                break;
            }
            local_end -= 1;
        }
        return &slice[..start + local_end];
    }

    trim_trailing_space_or_nul_word(&slice[..end])
}

#[inline(always)]
pub(super) fn is_ascii_simd(slice: &[u8]) -> bool {
    type U8x64 = Simd<u8, 64>;

    let mut chunks = slice.chunks_exact(64);
    let high_bits = U8x64::splat(0x80);
    for chunk in &mut chunks {
        let lanes = U8x64::from_slice(chunk);
        if (lanes & high_bits).reduce_or() != 0 {
            return false;
        }
    }
    chunks.remainder().is_ascii()
}

#[inline]
fn is_ascii_word(slice: &[u8]) -> bool {
    let mut chunks = slice.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_ne_bytes(chunk.try_into().expect("8-byte chunk"));
        if (word & ASCII_HIGH_BITS_8) != 0 {
            return false;
        }
    }
    chunks.remainder().is_ascii()
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
