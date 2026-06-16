use super::{
    Endianness, NumericTileMode, OwnedColumnBuffer, PlannedCell, Result, SasDate, SasDateTime,
    SasTime, Simd, SimdPartialEq, unexpected_batch_cell,
};
use std::simd::{Select, StdFloat, cmp::SimdPartialOrd, num::SimdFloat};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum TypedNumericValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum DateNumericValue {
    Null,
    Date(SasDate),
    Float64(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum DateTimeNumericValue {
    Null,
    DateTime(SasDateTime),
    Float64(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum TimeNumericValue {
    Null,
    Time(SasTime),
    Float64(f64),
}
pub(super) fn decode_numeric_cell(slice: &[u8], endianness: Endianness) -> Option<f64> {
    if slice.is_empty() {
        return None;
    }
    let raw = numeric_bits(slice, endianness);
    if numeric_bits_is_missing(raw) {
        None
    } else {
        Some(f64::from_bits(raw))
    }
}

#[inline]
pub(super) fn numeric_bits(slice: &[u8], endianness: Endianness) -> u64 {
    debug_assert!(slice.len() <= 8);
    match slice.len() {
        8 => numeric_bits_scalar_8(slice, endianness),
        5..=7 => numeric_bits_padded(slice, endianness),
        4 => {
            let word = match endianness {
                Endianness::Big => {
                    u64::from(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
                }
                Endianness::Little => {
                    u64::from(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
                }
            };
            word << 32
        }
        3 => {
            let word = match endianness {
                Endianness::Big => {
                    (u64::from(slice[0]) << 16) | (u64::from(slice[1]) << 8) | u64::from(slice[2])
                }
                Endianness::Little => {
                    (u64::from(slice[2]) << 16) | (u64::from(slice[1]) << 8) | u64::from(slice[0])
                }
            };
            word << 40
        }
        2 => {
            let word = match endianness {
                Endianness::Big => u64::from(u16::from_be_bytes([slice[0], slice[1]])),
                Endianness::Little => u64::from(u16::from_le_bytes([slice[0], slice[1]])),
            };
            word << 48
        }
        1 => u64::from(slice[0]) << 56,
        0 => 0,
        _ => unreachable!("numeric width must be <= 8"),
    }
}

#[inline]
fn numeric_bits_padded(slice: &[u8], endianness: Endianness) -> u64 {
    let len = slice.len();
    let mut word = 0u64;
    match endianness {
        Endianness::Big => {
            for &byte in slice {
                word = (word << 8) | u64::from(byte);
            }
        }
        Endianness::Little => {
            for i in (0..len).rev() {
                word = (word << 8) | u64::from(slice[i]);
            }
        }
    }
    word << ((8 - len) * 8)
}

pub(super) fn materialize_staged_numeric_column(
    raw_bits: &[u64],
    mode: NumericTileMode,
    has_missing: bool,
) -> OwnedColumnBuffer {
    match mode {
        NumericTileMode::F64RawBits => {
            if has_missing {
                let valid = classify_missing_raw_bits(raw_bits);
                materialize_staged_f64_column(raw_bits, valid)
            } else {
                materialize_staged_f64_column(raw_bits, None)
            }
        }
        NumericTileMode::IntegerWidth8 => {
            let valid = if has_missing {
                classify_missing_raw_bits(raw_bits)
            } else {
                None
            };
            materialize_staged_i64_or_f64_column(raw_bits, valid)
        }
        NumericTileMode::Date => {
            let valid = if has_missing {
                classify_missing_raw_bits(raw_bits)
            } else {
                None
            };
            materialize_staged_date_or_f64_column(raw_bits, valid)
        }
        NumericTileMode::DateTime => {
            let valid = if has_missing {
                classify_missing_raw_bits(raw_bits)
            } else {
                None
            };
            materialize_staged_datetime_or_f64_column(raw_bits, valid)
        }
        NumericTileMode::Time => {
            let valid = if has_missing {
                classify_missing_raw_bits(raw_bits)
            } else {
                None
            };
            materialize_staged_time_or_f64_column(raw_bits, valid)
        }
    }
}

/// Returns whether bit `index` in a bit-packed validity word slice is set (1 = valid, 0 = null).
#[inline]
fn valid_bit(validity: &[u64], index: usize) -> bool {
    (validity[index / 64] >> (index % 64)) & 1 == 1
}

pub(super) fn materialize_staged_f64_column(
    raw_bits: &[u64],
    valid: Option<Vec<u64>>,
) -> OwnedColumnBuffer {
    // Preserve the raw bits for every cell, including missing ones. The validity
    // bitmap (when present) marks missings; downstream consumers gate on it. SAS
    // special missing values (`.A`-`.Z`, `._`) encode a tag in the NaN payload —
    // keeping the raw bits lets bindings recover that tag (e.g. haven tagged_na)
    // rather than collapsing every missing to a single sentinel.
    let values = raw_bits.iter().map(|b| f64::from_bits(*b)).collect();
    OwnedColumnBuffer::F64 { values, valid }
}

/// Expand 8 packed validity bits into a per-lane SIMD null mask.
/// Returns a mask where lane `i` is `true` (null) when bit `i` of `valid_byte` is 0.
#[inline(always)]
fn expand_validity_byte(valid_byte: u8) -> <Simd<u64, 8> as SimdPartialEq>::Mask {
    type U64x8 = Simd<u64, 8>;
    let shifts = U64x8::from_array([0, 1, 2, 3, 4, 5, 6, 7]);
    let spread = U64x8::splat(u64::from(valid_byte)) >> shifts;
    (spread & U64x8::splat(1)).simd_eq(U64x8::splat(0))
}

pub(super) fn materialize_staged_i64_or_f64_column(
    raw_bits: &[u64],
    valid: Option<Vec<u64>>,
) -> OwnedColumnBuffer {
    type F64x8 = Simd<f64, 8>;
    type I64x8 = Simd<i64, 8>;
    type U64x8 = Simd<u64, 8>;

    if first_non_integral_in_range_index_simd(raw_bits, valid.as_deref(), I64_MIN_F64, I64_MAX_F64)
        .is_some()
    {
        return materialize_staged_f64_column(raw_bits, valid);
    }

    let mut values = Vec::with_capacity(raw_bits.len());
    match valid.as_deref() {
        None => {
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for raw_chunk in &mut raw_chunks {
                #[allow(clippy::cast_possible_truncation)]
                let converted: I64x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(converted.to_array());
            }
            for &bits in raw_chunks.remainder() {
                #[allow(clippy::cast_possible_truncation)]
                values.push(f64::from_bits(bits) as i64);
            }
        }
        Some(validity) => {
            let zeros = I64x8::splat(0);
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for (chunk_idx, raw_chunk) in raw_chunks.by_ref().enumerate() {
                let bit_base = chunk_idx * 8;
                #[allow(clippy::cast_possible_truncation)]
                let valid_byte = (validity[bit_base / 64] >> (bit_base % 64)) as u8;
                #[allow(clippy::cast_possible_truncation)]
                let converted: I64x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(
                    expand_validity_byte(valid_byte)
                        .select(zeros, converted)
                        .to_array(),
                );
            }
            let processed = raw_bits.len() - raw_chunks.remainder().len();
            for (offset, &bits) in raw_chunks.remainder().iter().enumerate() {
                let idx = processed + offset;
                #[allow(clippy::cast_possible_truncation)]
                values.push(if valid_bit(validity, idx) {
                    f64::from_bits(bits) as i64
                } else {
                    0
                });
            }
        }
    }
    OwnedColumnBuffer::I64 { values, valid }
}

pub(super) fn materialize_staged_date_or_f64_column(
    raw_bits: &[u64],
    valid: Option<Vec<u64>>,
) -> OwnedColumnBuffer {
    // Converts via i64 to keep a single Mask<i64, 8> type across all typed materializers.
    // Values are verified in-range for i32 by first_non_integral_in_range_index_simd.
    type F64x8 = Simd<f64, 8>;
    type I64x8 = Simd<i64, 8>;
    type U64x8 = Simd<u64, 8>;

    if first_non_integral_in_range_index_simd(raw_bits, valid.as_deref(), I32_MIN_F64, I32_MAX_F64)
        .is_some()
    {
        return materialize_staged_f64_column(raw_bits, valid);
    }

    let mut values = Vec::with_capacity(raw_bits.len());
    match valid.as_deref() {
        None => {
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for raw_chunk in &mut raw_chunks {
                #[allow(clippy::cast_possible_truncation)]
                let converted: I64x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(converted.to_array().map(|x| SasDate {
                    #[allow(clippy::cast_possible_truncation)]
                    days_since_sas_epoch: x as i32,
                }));
            }
            for &bits in raw_chunks.remainder() {
                #[allow(clippy::cast_possible_truncation)]
                values.push(SasDate {
                    days_since_sas_epoch: f64::from_bits(bits) as i32,
                });
            }
        }
        Some(validity) => {
            let zeros = I64x8::splat(0);
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for (chunk_idx, raw_chunk) in raw_chunks.by_ref().enumerate() {
                let bit_base = chunk_idx * 8;
                #[allow(clippy::cast_possible_truncation)]
                let valid_byte = (validity[bit_base / 64] >> (bit_base % 64)) as u8;
                #[allow(clippy::cast_possible_truncation)]
                let converted: I64x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(
                    expand_validity_byte(valid_byte)
                        .select(zeros, converted)
                        .to_array()
                        .map(|x| SasDate {
                            #[allow(clippy::cast_possible_truncation)]
                            days_since_sas_epoch: x as i32,
                        }),
                );
            }
            let processed = raw_bits.len() - raw_chunks.remainder().len();
            for (offset, &bits) in raw_chunks.remainder().iter().enumerate() {
                let idx = processed + offset;
                values.push(if valid_bit(validity, idx) {
                    #[allow(clippy::cast_possible_truncation)]
                    SasDate {
                        days_since_sas_epoch: f64::from_bits(bits) as i32,
                    }
                } else {
                    SasDate {
                        days_since_sas_epoch: 0,
                    }
                });
            }
        }
    }
    OwnedColumnBuffer::Date { values, valid }
}

pub(super) fn materialize_staged_datetime_or_f64_column(
    raw_bits: &[u64],
    valid: Option<Vec<u64>>,
) -> OwnedColumnBuffer {
    type F64x8 = Simd<f64, 8>;
    type I64x8 = Simd<i64, 8>;
    type U64x8 = Simd<u64, 8>;

    if first_non_integral_in_range_index_simd(raw_bits, valid.as_deref(), I64_MIN_F64, I64_MAX_F64)
        .is_some()
    {
        return materialize_staged_f64_column(raw_bits, valid);
    }

    let mut values = Vec::with_capacity(raw_bits.len());
    match valid.as_deref() {
        None => {
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for raw_chunk in &mut raw_chunks {
                #[allow(clippy::cast_possible_truncation)]
                let converted: I64x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(converted.to_array().map(|x| SasDateTime {
                    seconds_since_sas_epoch: x,
                }));
            }
            for &bits in raw_chunks.remainder() {
                #[allow(clippy::cast_possible_truncation)]
                values.push(SasDateTime {
                    seconds_since_sas_epoch: f64::from_bits(bits) as i64,
                });
            }
        }
        Some(validity) => {
            let zeros = I64x8::splat(0);
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for (chunk_idx, raw_chunk) in raw_chunks.by_ref().enumerate() {
                let bit_base = chunk_idx * 8;
                #[allow(clippy::cast_possible_truncation)]
                let valid_byte = (validity[bit_base / 64] >> (bit_base % 64)) as u8;
                #[allow(clippy::cast_possible_truncation)]
                let converted: I64x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(
                    expand_validity_byte(valid_byte)
                        .select(zeros, converted)
                        .to_array()
                        .map(|x| SasDateTime {
                            seconds_since_sas_epoch: x,
                        }),
                );
            }
            let processed = raw_bits.len() - raw_chunks.remainder().len();
            for (offset, &bits) in raw_chunks.remainder().iter().enumerate() {
                let idx = processed + offset;
                values.push(if valid_bit(validity, idx) {
                    #[allow(clippy::cast_possible_truncation)]
                    SasDateTime {
                        seconds_since_sas_epoch: f64::from_bits(bits) as i64,
                    }
                } else {
                    SasDateTime {
                        seconds_since_sas_epoch: 0,
                    }
                });
            }
        }
    }
    OwnedColumnBuffer::DateTime { values, valid }
}

pub(super) fn materialize_staged_time_or_f64_column(
    raw_bits: &[u64],
    valid: Option<Vec<u64>>,
) -> OwnedColumnBuffer {
    type F64x8 = Simd<f64, 8>;
    type I32x8 = Simd<i32, 8>;
    type U64x8 = Simd<u64, 8>;

    if first_non_integral_in_range_index_simd(raw_bits, valid.as_deref(), I32_MIN_F64, I32_MAX_F64)
        .is_some()
    {
        return materialize_staged_f64_column(raw_bits, valid);
    }

    let mut values = Vec::with_capacity(raw_bits.len());
    match valid.as_deref() {
        None => {
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for raw_chunk in &mut raw_chunks {
                let converted: I32x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(converted.to_array().map(|x| SasTime {
                    seconds_since_midnight: x,
                }));
            }
            for &bits in raw_chunks.remainder() {
                #[allow(clippy::cast_possible_truncation)]
                values.push(SasTime {
                    seconds_since_midnight: f64::from_bits(bits) as i32,
                });
            }
        }
        Some(validity) => {
            let zeros = I32x8::splat(0);
            let mut raw_chunks = raw_bits.chunks_exact(8);
            for (chunk_idx, raw_chunk) in raw_chunks.by_ref().enumerate() {
                let bit_base = chunk_idx * 8;
                #[allow(clippy::cast_possible_truncation)]
                let valid_byte = (validity[bit_base / 64] >> (bit_base % 64)) as u8;
                let converted: I32x8 =
                    F64x8::from_array(U64x8::from_slice(raw_chunk).to_array().map(f64::from_bits))
                        .cast();
                values.extend(
                    expand_validity_byte(valid_byte)
                        .select(zeros, converted)
                        .to_array()
                        .map(|x| SasTime {
                            seconds_since_midnight: x,
                        }),
                );
            }
            let processed = raw_bits.len() - raw_chunks.remainder().len();
            for (offset, &bits) in raw_chunks.remainder().iter().enumerate() {
                let idx = processed + offset;
                values.push(if valid_bit(validity, idx) {
                    #[allow(clippy::cast_possible_truncation)]
                    SasTime {
                        seconds_since_midnight: f64::from_bits(bits) as i32,
                    }
                } else {
                    SasTime {
                        seconds_since_midnight: 0,
                    }
                });
            }
        }
    }
    OwnedColumnBuffer::Time { values, valid }
}

#[allow(clippy::cast_precision_loss)]
const I64_MIN_F64: f64 = i64::MIN as f64;
#[allow(clippy::cast_precision_loss)]
const I64_MAX_F64: f64 = i64::MAX as f64;
#[allow(clippy::cast_precision_loss)]
const I32_MIN_F64: f64 = i32::MIN as f64;
#[allow(clippy::cast_precision_loss)]
const I32_MAX_F64: f64 = i32::MAX as f64;

/// Whether a valid f64 cell would be accepted by the i64 materializer.
///
/// Mirrors the acceptance criteria of [`first_non_integral_in_range_index_simd`]
/// with the i64 bounds, so a scalar scan with this predicate locates exactly the
/// value that forced [`materialize_staged_i64_or_f64_column`] to fall back to F64.
pub(super) fn f64_is_i64_representable(value: f64) -> bool {
    value.is_finite() && value.trunc() == value && (I64_MIN_F64..=I64_MAX_F64).contains(&value)
}

/// Efficiently find the index of the first value that cannot be represented as
/// an integer within the specified range [min, max].
///
/// This is used to decide whether a SAS numeric column (stored as floats) can
/// be "downgraded" to a more efficient Arrow/Polars integer type without loss
/// of precision.
///
/// Logic:
/// 1. Finite: Exponent bits are not all ones (ignores Infinity/NaN).
/// 2. Integral: floor(x) == x.
/// 3. In Range: min <= x <= max.
fn first_non_integral_in_range_index_simd(
    raw_bits: &[u64],
    valid: Option<&[u64]>,
    min: f64,
    max: f64,
) -> Option<usize> {
    type U64x4 = Simd<u64, 4>;
    type F64x4 = Simd<f64, 4>;

    let exp_mask = U64x4::splat(NUMERIC_EXP_MASK);
    let min_lanes = F64x4::splat(min);
    let max_lanes = F64x4::splat(max);
    let mut chunks = raw_bits.chunks_exact(4);

    for (chunk_index, chunk) in chunks.by_ref().enumerate() {
        let bits = U64x4::from_slice(chunk);
        let numbers = F64x4::from_array(bits.to_array().map(f64::from_bits));
        let finite = (bits & exp_mask).simd_ne(exp_mask);
        let integral = numbers.floor().simd_eq(numbers);
        let in_range = numbers.simd_ge(min_lanes) & numbers.simd_le(max_lanes);
        let eligible_bitmask =
            u8::try_from((finite & integral & in_range).to_bitmask()).expect("4-lane bitmask");
        // Extract 4 validity bits from the packed word for this chunk's rows.
        let required_bitmask = valid.map_or(0b1111u8, |validity| {
            let bit_base = chunk_index * 4;
            #[allow(clippy::cast_possible_truncation)]
            let b = ((validity[bit_base / 64] >> (bit_base % 64)) & 0xF) as u8;
            b
        });
        let failing = required_bitmask & !eligible_bitmask;
        if failing != 0 {
            return Some((chunk_index * 4) + failing.trailing_zeros() as usize);
        }
    }

    let processed = raw_bits.len() - chunks.remainder().len();
    for (offset, &bits) in chunks.remainder().iter().enumerate() {
        let index = processed + offset;
        if valid.is_some_and(|validity| !valid_bit(validity, index)) {
            continue;
        }
        let number = f64::from_bits(bits);
        if !number.is_finite()
            || number < min
            || number > max
            || number.floor().to_bits() != number.to_bits()
        {
            return Some(index);
        }
    }
    None
}

/// Classify missing values in `raw_bits` and return a bit-packed validity vector.
///
/// Each `u64` word in the result covers 64 rows: bit `i % 64` of word `i / 64` is 1 if
/// row `i` is valid (not a SAS missing sentinel), 0 if null.
///
/// Returns `None` when all rows are valid (no SAS missing values found).
pub(super) fn classify_missing_raw_bits(raw_bits: &[u64]) -> Option<Vec<u64>> {
    type U64x8 = Simd<u64, 8>;

    let exp_mask = U64x8::splat(NUMERIC_EXP_MASK);
    let fraction_mask = U64x8::splat(NUMERIC_FRACTION_MASK);
    let zeros = U64x8::splat(0);
    let mut valid: Option<Vec<u64>> = None;
    let mut processed_words = 0usize;

    // Process in groups of 64 rows, each producing one u64 validity word.
    let mut chunks64 = raw_bits.chunks_exact(64);
    for chunk64 in &mut chunks64 {
        let mut valid_word = 0u64;
        let mut any_missing = false;
        for (i, sub_chunk) in chunk64.chunks_exact(8).enumerate() {
            let lanes = U64x8::from_slice(sub_chunk);
            let missing_mask = ((lanes & exp_mask).simd_eq(exp_mask)
                & (lanes & fraction_mask).simd_ne(zeros))
            .to_bitmask();
            // valid_byte: bit j = 1 if lane j is NOT missing
            let valid_byte = (!missing_mask) & 0xFF;
            valid_word |= valid_byte << (i * 8);
            if missing_mask != 0 {
                any_missing = true;
            }
        }

        if any_missing {
            let valid_vec = valid.get_or_insert_with(|| vec![u64::MAX; processed_words]);
            valid_vec.push(valid_word);
        } else if let Some(valid_vec) = &mut valid {
            valid_vec.push(u64::MAX);
        }
        processed_words += 1;
    }

    // Remainder: fewer than 64 rows, producing one partial validity word.
    let remainder = chunks64.remainder();
    if !remainder.is_empty() {
        let mut valid_word = 0u64;
        let mut any_missing = false;
        let mut bit_offset = 0usize;
        let mut sub_chunks8 = remainder.chunks_exact(8);
        for sub_chunk in &mut sub_chunks8 {
            let lanes = U64x8::from_slice(sub_chunk);
            let missing_mask = ((lanes & exp_mask).simd_eq(exp_mask)
                & (lanes & fraction_mask).simd_ne(zeros))
            .to_bitmask();
            let valid_byte = (!missing_mask) & 0xFF;
            valid_word |= valid_byte << bit_offset;
            if missing_mask != 0 {
                any_missing = true;
            }
            bit_offset += 8;
        }
        for &bits in sub_chunks8.remainder() {
            if numeric_bits_is_missing(bits) {
                any_missing = true;
            } else {
                valid_word |= 1u64 << bit_offset;
            }
            bit_offset += 1;
        }

        if any_missing {
            let valid_vec = valid.get_or_insert_with(|| vec![u64::MAX; processed_words]);
            valid_vec.push(valid_word);
        } else if let Some(valid_vec) = &mut valid {
            valid_vec.push(valid_word);
        }
    }

    valid
}

pub(super) fn staged_numeric_raw_bits_from_planned_cell(cell: PlannedCell<'_>) -> Result<u64> {
    match cell {
        PlannedCell::Null => Ok(SAS_NUMERIC_MISSING_SENTINEL),
        PlannedCell::Int32(value) => Ok(f64::from(value).to_bits()),
        PlannedCell::Int64(value) => {
            #[allow(clippy::cast_precision_loss)]
            let v = value as f64;
            Ok(v.to_bits())
        }
        PlannedCell::Float64(value) => Ok(value.to_bits()),
        PlannedCell::Date(value) => Ok(f64::from(value.days_since_sas_epoch).to_bits()),
        PlannedCell::DateTime(value) => {
            #[allow(clippy::cast_precision_loss)]
            let v = value.seconds_since_sas_epoch as f64;
            Ok(v.to_bits())
        }
        PlannedCell::Time(value) => {
            let v = f64::from(value.seconds_since_midnight);
            Ok(v.to_bits())
        }
        other => Err(unexpected_batch_cell("staged numeric bits", other)),
    }
}

#[inline]
pub(super) fn numeric_bits_scalar_8(slice: &[u8], endianness: Endianness) -> u64 {
    let bytes: [u8; 8] = slice.try_into().expect("len == 8");
    match endianness {
        Endianness::Little => u64::from_le_bytes(bytes),
        Endianness::Big => u64::from_be_bytes(bytes),
    }
}

pub(super) const NUMERIC_EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
pub(super) const NUMERIC_FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
pub(super) const SAS_NUMERIC_MISSING_SENTINEL: u64 = 0x7FF0_0000_0000_0001;

#[inline]
pub(super) const fn numeric_bits_is_missing(raw: u64) -> bool {
    (raw & NUMERIC_EXP_MASK) == NUMERIC_EXP_MASK && (raw & NUMERIC_FRACTION_MASK) != 0
}

pub(super) fn try_i64_from_f64(number: f64) -> Option<i64> {
    // Constants for i64::MIN/MAX as f64. Note that i64::MAX (2^63 - 1)
    // is not exactly representable in f64 (2^63 is).
    #[allow(clippy::cast_precision_loss)]
    const I64_MIN_F64: f64 = i64::MIN as f64;
    #[allow(clippy::cast_precision_loss)]
    const I64_MAX_F64: f64 = i64::MAX as f64;

    if !number.is_finite() {
        return None;
    }

    if !(I64_MIN_F64..=I64_MAX_F64).contains(&number) {
        return None;
    }
    #[allow(clippy::cast_possible_truncation)]
    let value = number as i64;
    #[allow(clippy::cast_precision_loss)]
    if (value as f64 - number).abs() < f64::EPSILON {
        Some(value)
    } else {
        None
    }
}

pub(super) fn try_i32_from_f64(number: f64) -> Option<i32> {
    let value = try_i64_from_f64(number)?;
    i32::try_from(value).ok()
}

pub(super) fn classify_typed_numeric_value(
    number: Option<f64>,
    prefer_i32: bool,
) -> TypedNumericValue {
    let Some(number) = number else {
        return TypedNumericValue::Null;
    };
    let Some(value64) = try_i64_from_f64(number) else {
        return TypedNumericValue::Float64(number);
    };
    if prefer_i32 && let Ok(value32) = i32::try_from(value64) {
        return TypedNumericValue::Int32(value32);
    }
    TypedNumericValue::Int64(value64)
}

pub(super) fn classify_date_numeric_value(number: Option<f64>) -> DateNumericValue {
    number.map_or(DateNumericValue::Null, |number| {
        try_i32_from_f64(number).map_or(DateNumericValue::Float64(number), |days| {
            DateNumericValue::Date(SasDate {
                days_since_sas_epoch: days,
            })
        })
    })
}

pub(super) fn classify_datetime_numeric_value(number: Option<f64>) -> DateTimeNumericValue {
    number.map_or(DateTimeNumericValue::Null, |number| {
        try_i64_from_f64(number).map_or(DateTimeNumericValue::Float64(number), |seconds| {
            DateTimeNumericValue::DateTime(SasDateTime {
                seconds_since_sas_epoch: seconds,
            })
        })
    })
}

pub(super) fn classify_time_numeric_value(number: Option<f64>) -> TimeNumericValue {
    number.map_or(TimeNumericValue::Null, |number| {
        try_i32_from_f64(number).map_or(TimeNumericValue::Float64(number), |seconds| {
            TimeNumericValue::Time(SasTime {
                seconds_since_midnight: seconds,
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DateNumericValue, DateTimeNumericValue, TimeNumericValue, TypedNumericValue,
        classify_date_numeric_value, classify_datetime_numeric_value, classify_time_numeric_value,
        classify_typed_numeric_value, numeric_bits, try_i64_from_f64,
    };
    use crate::Endianness;

    #[test]
    fn try_i64_requires_finite_integral_values() {
        assert_eq!(try_i64_from_f64(42.0), Some(42));
        assert_eq!(try_i64_from_f64(-42.0), Some(-42));
        assert_eq!(try_i64_from_f64(42.5), None);
        assert_eq!(try_i64_from_f64(f64::NAN), None);
        assert_eq!(try_i64_from_f64(f64::INFINITY), None);
        assert_eq!(try_i64_from_f64(f64::NEG_INFINITY), None);
    }

    #[test]
    fn typed_numeric_classification_handles_i32_i64_and_float() {
        assert_eq!(
            classify_typed_numeric_value(None, true),
            TypedNumericValue::Null
        );
        assert_eq!(
            classify_typed_numeric_value(Some(7.0), true),
            TypedNumericValue::Int32(7)
        );
        assert_eq!(
            classify_typed_numeric_value(Some(f64::from(i32::MAX) + 1.0), true),
            TypedNumericValue::Int64(i64::from(i32::MAX) + 1)
        );
        assert_eq!(
            classify_typed_numeric_value(Some(7.25), true),
            TypedNumericValue::Float64(7.25)
        );
    }

    #[test]
    fn date_datetime_time_classification_handles_fractional_fallback() {
        assert_eq!(
            classify_date_numeric_value(Some(12.0)),
            DateNumericValue::Date(crate::metadata::SasDate {
                days_since_sas_epoch: 12,
            })
        );
        assert_eq!(
            classify_date_numeric_value(Some(12.5)),
            DateNumericValue::Float64(12.5)
        );

        assert_eq!(
            classify_datetime_numeric_value(Some(120.0)),
            DateTimeNumericValue::DateTime(crate::metadata::SasDateTime {
                seconds_since_sas_epoch: 120,
            })
        );
        assert_eq!(
            classify_datetime_numeric_value(Some(120.25)),
            DateTimeNumericValue::Float64(120.25)
        );

        assert_eq!(
            classify_time_numeric_value(Some(3600.0)),
            TimeNumericValue::Time(crate::metadata::SasTime {
                seconds_since_midnight: 3600,
            })
        );
        assert_eq!(
            classify_time_numeric_value(Some(3600.5)),
            TimeNumericValue::Float64(3600.5)
        );
    }

    #[test]
    #[allow(clippy::cast_precision_loss, clippy::float_cmp)]
    fn f64_precision_boundary_is_not_recovered_as_unrepresentable_integer() {
        let rounded = 9_007_199_254_740_993_i64 as f64;
        assert_eq!(rounded, 9_007_199_254_740_992.0);
        assert_eq!(try_i64_from_f64(rounded), Some(9_007_199_254_740_992));
    }

    #[test]
    fn numeric_bits_preserves_sas_alignment_for_partial_widths() {
        let bytes = [0x11_u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
        for len in 1..=7 {
            let slice = &bytes[..len];
            let be_expected = slice
                .iter()
                .fold(0_u64, |acc, &b| (acc << 8) | u64::from(b))
                << ((8 - len) * 8);
            let le_expected = slice
                .iter()
                .rev()
                .fold(0_u64, |acc, &b| (acc << 8) | u64::from(b))
                << ((8 - len) * 8);
            assert_eq!(numeric_bits(slice, Endianness::Big), be_expected);
            assert_eq!(numeric_bits(slice, Endianness::Little), le_expected);
        }
    }
}
