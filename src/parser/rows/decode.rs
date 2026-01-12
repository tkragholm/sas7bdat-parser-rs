use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::mem::size_of;

use encoding_rs::{Encoding, UTF_8};
use simdutf8::basic;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};

use crate::cell::{CellValue, MissingValue};
use crate::dataset::{Endianness, MissingLiteral, TaggedMissing};
use crate::parser::core::encoding::trim_trailing;
use crate::parser::core::float_utils::try_int_from_f64;
use crate::parser::metadata::{ColumnKind, NumericKind};

#[derive(Clone)]
pub enum NumericCell {
    Missing(MissingValue),
    Number(f64),
}

pub fn decode_value_inner<'data>(
    kind: ColumnKind,
    raw_width: u32,
    slice: &'data [u8],
    encoding: &'static Encoding,
    endianness: Endianness,
) -> CellValue<'data> {
    match kind {
        ColumnKind::Character => CellValue::Str(decode_string(slice, encoding)),
        ColumnKind::Numeric(numeric_kind) => match decode_numeric_cell(slice, endianness) {
            NumericCell::Missing(missing) => CellValue::Missing(missing),
            NumericCell::Number(number) => match numeric_kind {
                NumericKind::Double => numeric_value_from_width(number, raw_width),
                NumericKind::Date => sas_days_to_datetime(number).map_or_else(
                    || numeric_value_from_width(number, raw_width),
                    CellValue::Date,
                ),
                NumericKind::DateTime => sas_seconds_to_datetime(number).map_or_else(
                    || numeric_value_from_width(number, raw_width),
                    CellValue::DateTime,
                ),
                NumericKind::Time => sas_seconds_to_time(number).map_or_else(
                    || numeric_value_from_width(number, raw_width),
                    CellValue::Time,
                ),
            },
        },
    }
}

pub fn decode_string<'a>(slice: &'a [u8], encoding: &'static Encoding) -> Cow<'a, str> {
    let trimmed = trim_trailing(slice);
    if trimmed.is_empty() {
        return Cow::Borrowed("");
    }

    if let Ok(text) = basic::from_utf8(trimmed) {
        return maybe_fix_mojibake(Cow::Borrowed(text));
    }

    if encoding == UTF_8 {
        let mut owned = String::from_utf8_lossy(trimmed).into_owned();
        let trimmed_len = owned.trim_end_matches([' ', '\u{0000}']).len();
        if trimmed_len != owned.len() {
            owned.truncate(trimmed_len);
        }
        return maybe_fix_mojibake(Cow::Owned(owned));
    }

    let (decoded, had_errors) = encoding.decode_without_bom_handling(trimmed);
    let mut owned = decoded.into_owned();
    if had_errors && owned.is_empty() {
        owned = String::from_utf8_lossy(trimmed).into_owned();
    }
    let trimmed_len = owned.trim_end_matches([' ', '\u{0000}']).len();
    if trimmed_len != owned.len() {
        owned.truncate(trimmed_len);
    }
    maybe_fix_mojibake(Cow::Owned(owned))
}

fn maybe_fix_mojibake(value: Cow<'_, str>) -> Cow<'_, str> {
    let text = value.as_ref();
    if text.is_ascii() {
        return value;
    }

    let mut bytes = Vec::with_capacity(text.len());
    let mut has_extended = false;

    for ch in text.chars() {
        let code = ch as u32;
        if code > 0xFF {
            return value;
        }
        if code >= 0x80 {
            has_extended = true;
        }
        bytes.push(u8::try_from(code).expect("code <= 0xFF enforced above"));
    }

    if has_extended
        && let Ok(decoded) = std::str::from_utf8(&bytes)
        && decoded != text
    {
        return Cow::Owned(decoded.to_owned());
    }
    value
}

pub fn decode_numeric_cell(slice: &[u8], endian: Endianness) -> NumericCell {
    if slice.is_empty() {
        return NumericCell::Missing(MissingValue::system());
    }
    let raw = numeric_bits(slice, endian);
    if numeric_bits_is_missing(raw) {
        NumericCell::Missing(decode_missing_from_bits(raw))
    } else {
        NumericCell::Number(f64::from_bits(raw))
    }
}

#[inline]
pub fn numeric_bits(slice: &[u8], endian: Endianness) -> u64 {
    debug_assert!(slice.len() <= 8);
    if slice.len() == 8 {
        match endian {
            Endianness::Little => {
                let bytes: [u8; 8] = slice.try_into().expect("len == 8");
                u64::from_le_bytes(bytes)
            }
            Endianness::Big => {
                let bytes: [u8; 8] = slice.try_into().expect("len == 8");
                u64::from_be_bytes(bytes)
            }
        }
    } else {
        let mut buf = [0u8; 8];
        match endian {
            Endianness::Big => {
                let len = slice.len();
                buf[..len].copy_from_slice(slice);
            }
            Endianness::Little => {
                for (idx, &byte) in slice.iter().rev().enumerate() {
                    buf[idx] = byte;
                }
            }
        }
        u64::from_be_bytes(buf)
    }
}

#[inline]
pub const fn numeric_bits_is_missing(raw: u64) -> bool {
    const EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
    const FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
    (raw & EXP_MASK) == EXP_MASK && (raw & FRACTION_MASK) != 0
}

const fn decode_missing_from_bits(raw: u64) -> MissingValue {
    let upper = (raw >> 40) & 0xFF;
    let tag_byte = !(upper as u8);
    match tag_byte {
        0 => MissingValue::Tagged(TaggedMissing {
            tag: Some('_'),
            literal: MissingLiteral::Numeric(f64::from_bits(raw)),
        }),
        2..=27 => {
            let ch = (b'A' + (tag_byte - 2)) as char;
            MissingValue::Tagged(TaggedMissing {
                tag: Some(ch),
                literal: MissingLiteral::Numeric(f64::from_bits(raw)),
            })
        }
        _ => MissingValue::System,
    }
}

fn numeric_value_from_width<'a>(number: f64, width: u32) -> CellValue<'a> {
    if let Some(int) = try_int_from_f64::<i64>(number) {
        if width <= 4 {
            if let Ok(value32) = i32::try_from(int) {
                return CellValue::Int32(value32);
            }
            return CellValue::Int64(int);
        } else if width <= 8 {
            return CellValue::Int64(int);
        }
    }
    CellValue::Float(number)
}

const fn repeat_byte_usize(byte: u8) -> usize {
    let mut value = 0usize;
    let mut i = 0usize;
    while i < (usize::BITS / 8) as usize {
        value |= (byte as usize) << (i * 8);
        i += 1;
    }
    value
}

const USIZE_BYTES: usize = size_of::<usize>();
const SPACE_MASK_USIZE: usize = repeat_byte_usize(b' ');
const SPACE_MASK_U128: u128 = repeat_byte_u128(b' ');

const fn repeat_byte_u128(byte: u8) -> u128 {
    let mut value = 0u128;
    let mut i = 0;
    while i < 16 {
        value |= (byte as u128) << (i * 8);
        i += 1;
    }
    value
}

#[inline]
pub fn is_blank(slice: &[u8]) -> bool {
    let mut offset = slice.len();
    while offset >= 16 {
        let chunk = &slice[offset - 16..offset];
        let word = u128::from_ne_bytes(chunk.try_into().unwrap());
        if word & !SPACE_MASK_U128 != 0 {
            // Non-space/NUL encountered; fall back to slower path below.
            break;
        }
        offset -= 16;
    }

    let mut chunks = slice[..offset].chunks_exact(USIZE_BYTES);
    for chunk in chunks.by_ref() {
        let word = usize::from_ne_bytes(chunk.try_into().unwrap());
        if word & !SPACE_MASK_USIZE != 0 {
            return false;
        }
    }
    chunks.remainder().iter().all(|&b| b == 0 || b == b' ')
}

#[inline]
pub fn trim_trailing_space_or_nul_simd(slice: &[u8]) -> &[u8] {
    let mut end = slice.len();
    while end >= 16 {
        let chunk = &slice[end - 16..end];
        let word = u128::from_ne_bytes(chunk.try_into().unwrap());
        if word & !SPACE_MASK_U128 != 0 {
            break;
        }
        end -= 16;
    }
    while end >= USIZE_BYTES {
        let chunk = &slice[end - USIZE_BYTES..end];
        let word = usize::from_ne_bytes(chunk.try_into().unwrap());
        if word & !SPACE_MASK_USIZE != 0 {
            break;
        }
        end -= USIZE_BYTES;
    }
    while end > 0 {
        let byte = slice[end - 1];
        if byte != b' ' && byte != 0 {
            break;
        }
        end -= 1;
    }
    &slice[..end]
}

fn sas_epoch() -> PrimitiveDateTime {
    PrimitiveDateTime::new(
        Date::from_calendar_date(1960, Month::January, 1).expect("valid SAS epoch"),
        Time::MIDNIGHT,
    )
}

fn sas_offset_datetime(seconds: f64) -> Option<OffsetDateTime> {
    if !seconds.is_finite() {
        return None;
    }
    let duration = Duration::seconds_f64(seconds.abs());
    if seconds >= 0.0 {
        sas_epoch()
            .checked_add(duration)
            .map(PrimitiveDateTime::assume_utc)
    } else {
        sas_epoch()
            .checked_sub(duration)
            .map(PrimitiveDateTime::assume_utc)
    }
}

pub fn sas_days_to_datetime(days: f64) -> Option<OffsetDateTime> {
    sas_offset_datetime(days * 86_400.0)
}

pub fn sas_seconds_to_datetime(seconds: f64) -> Option<OffsetDateTime> {
    sas_offset_datetime(seconds)
}

pub fn sas_seconds_to_time(seconds: f64) -> Option<Duration> {
    if !seconds.is_finite() {
        return None;
    }
    Some(Duration::seconds_f64(seconds))
}
