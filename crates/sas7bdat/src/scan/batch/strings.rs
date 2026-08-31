//! Appending decoded string and byte cells into a batch column.
//!
//! Split from the accumulator because these are pure encode kernels: given a trimmed cell and
//! a destination buffer they push bytes, holding none of the accumulator's per-batch state.
//! Three of the six decode families land here.
//!
//! The windows-1252 path is the fiddly one. Bytes 0x00-0x7F and 0xA0-0xFF map straight to the
//! codepoint of the same value, so they encode without a lookup table; only the assigned bytes
//! in 0x80-0x9F need one, and the unassigned ones in that range are what `strict` decides the
//! fate of.

use super::{
    CompiledColumnPlan, CompiledDecodeKernel, DICT_ID_NONE, Error, OwnedBatchColumnBuilder, Result,
    RowDecodePlan, StageLookupHit, StagedStringLookup, StringDecodeKernel, TrimMode, TrimmedString,
    TrustedOffsets, push_dictionary_id, push_variable_valid, push_variable_valid_overcopy,
    push_variable_valid_without_validity, simd_from_utf8, staged_entry_to_dictionary_id,
};
use crate::columnar::BLANK_ID;
use crate::scan::DecodedUtf8BatchValue;
use crate::scan::{is_blank_after_trim_mode, trim_and_classify_for_mode};
use encoding_rs::WINDOWS_1252;

pub(super) fn push_utf8_bytes_fast(
    offsets: &mut TrustedOffsets,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    value: &[u8],
) -> Result<()> {
    if valid.is_none() {
        push_variable_valid_without_validity(offsets, data, value)
    } else {
        push_variable_valid(offsets, data, valid, value)
    }
}

#[derive(Debug, Clone, Copy)]
enum TrimmedCellClass<'a> {
    Blank,
    Ascii(&'a [u8]),
    NonAscii(TrimmedString<'a>),
}

#[inline]
fn classify_trimmed_cell(slice: &[u8], mode: TrimMode) -> TrimmedCellClass<'_> {
    let trimmed = trim_and_classify_for_mode(slice, mode);
    let bytes = trimmed.bytes;
    let is_blank = bytes.is_empty()
        || (matches!(mode, TrimMode::Preserve) && is_blank_after_trim_mode(slice, mode));
    if is_blank {
        TrimmedCellClass::Blank
    } else if trimmed.is_ascii {
        TrimmedCellClass::Ascii(bytes)
    } else {
        TrimmedCellClass::NonAscii(trimmed)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct DirectUtf8OwnedBreakdown {
    pub(super) interned_hits: u64,
    pub(super) seen_once_promotions: u64,
}

#[inline]
pub(super) fn append_direct_raw_bytes_batch_column(
    _row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = RowDecodePlan::slice_in_bounds(row, column);
    match batch_column {
        OwnedBatchColumnBuilder::RawBytes {
            offsets,
            data,
            valid,
        } if matches!(column.kernel, CompiledDecodeKernel::RawBytes) => {
            push_variable_valid(offsets, data, valid, slice)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[inline]
pub(super) fn append_direct_utf8_single_byte_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
    utf8_decode_scratch: &mut String,
) -> Result<bool> {
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
        ) if column.width == 1 => {
            let byte = row[column.start];
            let trim_space = match row_plan.string_options.trim_mode {
                TrimMode::Preserve => false,
                TrimMode::RTrim | TrimMode::Strip => byte == b' ' || byte == 0,
            };
            let slice = if trim_space {
                &[][..]
            } else {
                &row[column.start..column.end]
            };

            if slice.is_empty() || byte.is_ascii() {
                push_variable_valid(offsets, data, valid, slice)?;
                push_dictionary_id(
                    dictionary_ids,
                    if slice.is_empty() {
                        BLANK_ID
                    } else {
                        DICT_ID_NONE
                    },
                );
                return Ok(true);
            }

            if row_plan.encoding == WINDOWS_1252
                && matches!(
                    row_plan.string_kernel,
                    StringDecodeKernel::EncodedStrict | StringDecodeKernel::EncodedLenient
                )
            {
                append_windows_1252_single_byte_utf8(
                    offsets,
                    data,
                    valid,
                    byte,
                    matches!(row_plan.string_kernel, StringDecodeKernel::EncodedStrict),
                )?;
                push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                return Ok(true);
            }

            append_non_ascii_single_byte_utf8(
                row_plan,
                offsets,
                data,
                valid,
                dictionary_ids,
                slice,
                utf8_decode_scratch,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[inline]
fn append_non_ascii_single_byte_utf8(
    row_plan: &RowDecodePlan,
    offsets: &mut TrustedOffsets,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    dictionary_ids: &mut Option<Vec<u32>>,
    slice: &[u8],
    utf8_decode_scratch: &mut String,
) -> Result<()> {
    let trimmed = TrimmedString {
        bytes: slice,
        is_ascii: false,
    };
    match row_plan.string_kernel {
        StringDecodeKernel::Utf8Strict => Err(Error::Decode(crate::error::DecodeError {
            message: "invalid UTF-8 in fixed-width string cell".to_owned(),
        })),
        StringDecodeKernel::Utf8Lenient => {
            match row_plan
                .decode_utf8_lenient_trimmed_bytes_for_batch_direct(trimmed, utf8_decode_scratch)
            {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    push_variable_valid(offsets, data, valid, bytes)?;
                }
                DecodedUtf8BatchValue::Scratch => {
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                }
            }
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(())
        }
        StringDecodeKernel::EncodedStrict => {
            match row_plan.decode_encoded_strict_trimmed_bytes_for_batch_direct(
                trimmed,
                utf8_decode_scratch,
            )? {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    push_variable_valid(offsets, data, valid, bytes)?;
                }
                DecodedUtf8BatchValue::Scratch => {
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                }
            }
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(())
        }
        StringDecodeKernel::EncodedLenient => {
            match row_plan
                .decode_encoded_lenient_trimmed_bytes_for_batch_direct(trimmed, utf8_decode_scratch)
            {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    push_variable_valid(offsets, data, valid, bytes)?;
                }
                DecodedUtf8BatchValue::Scratch => {
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                }
            }
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(())
        }
    }
}

#[inline]
fn append_windows_1252_single_byte_utf8(
    offsets: &mut TrustedOffsets,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    byte: u8,
    strict: bool,
) -> Result<()> {
    let mut encoded = [0_u8; 3];
    let len = encode_windows_1252_single_byte_utf8(byte, strict, &mut encoded)?;
    push_variable_valid(offsets, data, valid, &encoded[..len])
}

const WINDOWS_1252_REPLACEMENT_UTF8: [u8; 3] = [0xEF, 0xBF, 0xBD];

fn strict_windows_1252_decode_error() -> Error {
    Error::Decode(crate::error::DecodeError {
        message: "string decode failed under strict validation".to_owned(),
    })
}

const fn windows_1252_special_case(byte: u8) -> Option<[u8; 3]> {
    match byte {
        0x80 => Some([0xE2, 0x82, 0xAC]),
        0x81 | 0x8D | 0x8F | 0x90 | 0x9D => None,
        0x82 => Some([0xE2, 0x80, 0x9A]),
        0x83 => Some([0xC6, 0x92, 0]),
        0x84 => Some([0xE2, 0x80, 0x9E]),
        0x85 => Some([0xE2, 0x80, 0xA6]),
        0x86 => Some([0xE2, 0x80, 0xA0]),
        0x87 => Some([0xE2, 0x80, 0xA1]),
        0x88 => Some([0xCB, 0x86, 0]),
        0x89 => Some([0xE2, 0x80, 0xB0]),
        0x8A => Some([0xC5, 0xA0, 0]),
        0x8B => Some([0xE2, 0x80, 0xB9]),
        0x8C => Some([0xC5, 0x92, 0]),
        0x8E => Some([0xC5, 0xBD, 0]),
        0x91 => Some([0xE2, 0x80, 0x98]),
        0x92 => Some([0xE2, 0x80, 0x99]),
        0x93 => Some([0xE2, 0x80, 0x9C]),
        0x94 => Some([0xE2, 0x80, 0x9D]),
        0x95 => Some([0xE2, 0x80, 0xA2]),
        0x96 => Some([0xE2, 0x80, 0x93]),
        0x97 => Some([0xE2, 0x80, 0x94]),
        0x98 => Some([0xCB, 0x9C, 0]),
        0x99 => Some([0xE2, 0x84, 0xA2]),
        0x9A => Some([0xC5, 0xA1, 0]),
        0x9B => Some([0xE2, 0x80, 0xBA]),
        0x9C => Some([0xC5, 0x93, 0]),
        0x9E => Some([0xC5, 0xBE, 0]),
        0x9F => Some([0xC5, 0xB8, 0]),
        _ => Some([0, 0, 0]),
    }
}

fn encode_windows_1252_single_byte_utf8(
    byte: u8,
    strict: bool,
    out: &mut [u8; 3],
) -> Result<usize> {
    if let Some(encoded) = windows_1252_special_case(byte) {
        if encoded != [0, 0, 0] {
            *out = encoded;
            return Ok(if encoded[2] == 0 { 2 } else { 3 });
        }
    } else if strict {
        return Err(strict_windows_1252_decode_error());
    } else {
        *out = WINDOWS_1252_REPLACEMENT_UTF8;
        return Ok(3);
    }

    if (0xA0..=0xBF).contains(&byte) {
        *out = [0xC2, byte, 0];
    } else {
        *out = [0xC3, byte - 64, 0];
    }
    Ok(2)
}

#[inline]
pub(super) fn append_direct_utf8_borrowed_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = RowDecodePlan::slice_in_bounds(row, column);
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
        ) => {
            let bytes = row_plan.decode_string_bytes_for_batch_borrowed(slice)?;
            push_variable_valid(offsets, data, valid, bytes)?;
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// The four buffers a UTF-8 batch column is built from.
type Utf8BuilderParts<'a> = (
    &'a mut TrustedOffsets,
    &'a mut Vec<u8>,
    &'a mut Option<Vec<u64>>,
    &'a mut Option<Vec<u32>>,
);

/// The ASCII arm of the owned path, lifted out so its caller stays inside the line budget.
fn push_ascii_cell(
    builder: Utf8BuilderParts<'_>,
    row: &[u8],
    start: usize,
    keep: usize,
) -> Result<bool> {
    let (offsets, data, valid, dictionary_ids) = builder;
    // No width gate. One was tried, on the reasoning that a two-byte cell should not move
    // sixteen bytes, and it measured worse on the very fixtures it was meant to protect:
    // the fixed move beats the `memcpy` call even when most of what it moves is discarded.
    push_variable_valid_overcopy(offsets, data, valid, row, start, keep)?;
    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
    Ok(true)
}

pub(super) fn append_direct_utf8_owned_batch_column(
    source: (&RowDecodePlan, &CompiledColumnPlan, &[u8]),
    batch_column: &mut OwnedBatchColumnBuilder,
    utf8_decode_scratch: &mut String,
    mut staged_lookup: Option<&mut StagedStringLookup>,
    decode_owned: impl for<'a> Fn(
        &'a RowDecodePlan,
        TrimmedString<'a>,
        &'a mut String,
    ) -> Result<DecodedUtf8BatchValue<'a>>,
    fast_valid_utf8_non_ascii: bool,
    breakdown: &mut DirectUtf8OwnedBreakdown,
) -> Result<bool> {
    let (row_plan, column, row) = source;
    let slice = RowDecodePlan::slice_in_bounds(row, column);
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
        ) => {
            let (trimmed, slice) =
                match classify_trimmed_cell(slice, row_plan.string_options.trim_mode) {
                    TrimmedCellClass::Blank => {
                        push_variable_valid(offsets, data, valid, &[])?;
                        push_dictionary_id(dictionary_ids, BLANK_ID);
                        return Ok(true);
                    }
                    TrimmedCellClass::Ascii(bytes) => {
                        // The dominant case on register data: every cell is ASCII, so this
                        // is the arm that runs 158,000 times for a single lmdb year.
                        return push_ascii_cell(
                            (offsets, data, valid, dictionary_ids),
                            row,
                            column.start,
                            bytes.len(),
                        );
                    }
                    TrimmedCellClass::NonAscii(trimmed) => (trimmed, trimmed.bytes),
                };

            if fast_valid_utf8_non_ascii && simd_from_utf8(slice).is_ok() {
                push_variable_valid(offsets, data, valid, slice)?;
                push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                return Ok(true);
            }

            if let Some(dict) = staged_lookup.as_deref_mut()
                && dict.should_use()
            {
                dict.observe_lookup();
                if let Some(hit) = dict.lookup(slice) {
                    match hit {
                        StageLookupHit::Interned(entry_idx) => {
                            let interned = dict.interned_utf8(entry_idx);
                            push_variable_valid(offsets, data, valid, interned)?;
                            breakdown.interned_hits = breakdown.interned_hits.saturating_add(1);
                            push_dictionary_id(
                                dictionary_ids,
                                staged_entry_to_dictionary_id(entry_idx),
                            );
                            return Ok(true);
                        }
                        StageLookupHit::SeenOnce(entry_idx) => {
                            breakdown.seen_once_promotions =
                                breakdown.seen_once_promotions.saturating_add(1);
                            match decode_owned(row_plan, trimmed, utf8_decode_scratch)? {
                                DecodedUtf8BatchValue::Borrowed(bytes) => {
                                    // Append once from freshly decoded bytes, then promote for subsequent hits.
                                    push_variable_valid(offsets, data, valid, bytes)?;
                                    dict.promote_interned(entry_idx, bytes, bytes == slice);
                                    push_dictionary_id(
                                        dictionary_ids,
                                        staged_entry_to_dictionary_id(entry_idx),
                                    );
                                }
                                DecodedUtf8BatchValue::Scratch => {
                                    // Avoid reloading from dictionary arena on the promotion row.
                                    let promoted_utf8 = utf8_decode_scratch.as_bytes();
                                    push_variable_valid(offsets, data, valid, promoted_utf8)?;
                                    dict.promote_interned(entry_idx, promoted_utf8, false);
                                    push_dictionary_id(
                                        dictionary_ids,
                                        staged_entry_to_dictionary_id(entry_idx),
                                    );
                                }
                            }
                            return Ok(true);
                        }
                    }
                }
            }

            match decode_owned(row_plan, trimmed, utf8_decode_scratch)? {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    if let Some(dict) = staged_lookup.as_mut() {
                        let _ = dict.insert_seen_once(slice);
                    }
                    push_variable_valid(offsets, data, valid, bytes)?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                }
                DecodedUtf8BatchValue::Scratch => {
                    if let Some(dict) = staged_lookup.as_mut() {
                        let _ = dict.insert_seen_once(slice);
                    }
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod windows_1252_tests {
    use super::{WINDOWS_1252_REPLACEMENT_UTF8, encode_windows_1252_single_byte_utf8};

    /// Encode a single windows-1252 byte and return the produced UTF-8 bytes.
    fn enc(byte: u8, strict: bool) -> Vec<u8> {
        let mut out = [0_u8; 3];
        let len = encode_windows_1252_single_byte_utf8(byte, strict, &mut out)
            .expect("lenient encode should not fail");
        out[..len].to_vec()
    }

    #[test]
    fn special_cases_match_their_unicode_codepoints() {
        // Cross-check the lookup table against Rust string literals (an independent
        // oracle): the 0x80-0x9F window holds the windows-1252 "smart punctuation".
        assert_eq!(enc(0x80, false), "€".as_bytes()); // 3-byte
        assert_eq!(enc(0x82, false), "‚".as_bytes());
        assert_eq!(enc(0x83, false), "ƒ".as_bytes()); // 2-byte
        assert_eq!(enc(0x8A, false), "Š".as_bytes());
        assert_eq!(enc(0x8C, false), "Œ".as_bytes());
        assert_eq!(enc(0x95, false), "•".as_bytes());
        assert_eq!(enc(0x99, false), "™".as_bytes());
        assert_eq!(enc(0x9F, false), "Ÿ".as_bytes());
    }

    #[test]
    fn high_ranges_map_to_latin1() {
        // 0xA0..=0xBF take the [0xC2, byte, 0] branch; 0xC0..=0xFF take [0xC3, byte-64, 0].
        assert_eq!(enc(0xA0, false), "\u{00A0}".as_bytes()); // non-breaking space
        assert_eq!(enc(0xBF, false), "¿".as_bytes());
        assert_eq!(enc(0xC0, false), "À".as_bytes());
        assert_eq!(enc(0xE9, false), "é".as_bytes());
        assert_eq!(enc(0xFF, false), "ÿ".as_bytes());
    }

    #[test]
    fn undefined_bytes_replace_when_lenient_and_error_when_strict() {
        for byte in [0x81_u8, 0x8D, 0x8F, 0x90, 0x9D] {
            assert_eq!(
                enc(byte, false),
                WINDOWS_1252_REPLACEMENT_UTF8,
                "byte {byte:#x} should become U+FFFD when lenient",
            );
            let mut out = [0_u8; 3];
            assert!(
                encode_windows_1252_single_byte_utf8(byte, true, &mut out).is_err(),
                "byte {byte:#x} should error under strict validation",
            );
        }
    }

    #[test]
    fn every_high_byte_yields_exactly_one_char() {
        // Exercises every arm of the special-case match plus both range branches:
        // each high byte must decode to a single, valid UTF-8 scalar.
        for byte in 0x80_u8..=0xFF {
            let out = enc(byte, false);
            let text = std::str::from_utf8(&out)
                .unwrap_or_else(|_| panic!("byte {byte:#x} produced invalid UTF-8"));
            assert_eq!(
                text.chars().count(),
                1,
                "byte {byte:#x} should map to exactly one char",
            );
        }
    }
}
