use super::*;
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
        let trimmed = trim_trailing_space_or_nul(slice);
        return TrimmedString {
            bytes: trimmed,
            is_ascii: trimmed.is_ascii(),
        };
    }

    let trimmed = trim_trailing_space_or_nul_simd(slice);
    TrimmedString {
        bytes: trimmed,
        is_ascii: is_ascii_simd(trimmed),
    }
}

#[inline(always)]
fn is_all_space_or_nul_12(slice: &[u8]) -> bool {
    debug_assert_eq!(slice.len(), 12);
    let head = u64::from_ne_bytes(slice[..8].try_into().expect("fixed-width head"));
    let tail = u32::from_ne_bytes(slice[8..12].try_into().expect("fixed-width tail"));

    const SPACES_HEAD: u64 = u64::from_ne_bytes([b' '; 8]);
    const SPACES_TAIL: u32 = u32::from_ne_bytes([b' '; 4]);

    (head == 0 && tail == 0) || (head == SPACES_HEAD && tail == SPACES_TAIL)
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

    trim_trailing_space_or_nul(&slice[..end])
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
