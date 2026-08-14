use super::{
    Endianness, NumericTileMode, OwnedColumnBuffer, PlannedCell, Result, SasDate, SasDateTime,
    SasTime, unexpected_batch_cell,
};
pub(super) use crate::simd::{NUMERIC_EXP_MASK, NUMERIC_FRACTION_MASK, classify_missing_raw_bits};
use crate::simd::{convert_column_i32, convert_column_i64, first_non_integral_in_range_index};

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

/// Materialize a staged numeric column as `i64`-backed values, falling back to
/// `f64` when any present cell is not a finite integer in range.
///
/// The four typed materializers below differ only in the range they demand, the
/// width they convert to, and the wrapper struct — so they all route through this
/// one, which owns the chunking and validity handling.
macro_rules! materialize_or_f64 {
    (
        $name:ident,
        min = $min:expr,
        max = $max:expr,
        convert = $convert:ident,
        scalar_ty = $scalar_ty:ty,
        wrap = $wrap:expr,
        buffer = $buffer:ident $(,)?
    ) => {
        pub(super) fn $name(raw_bits: &[u64], valid: Option<Vec<u64>>) -> OwnedColumnBuffer {
            if first_non_integral_in_range_index(raw_bits, valid.as_deref(), $min, $max).is_some() {
                return materialize_staged_f64_column(raw_bits, valid);
            }

            let wrap: fn($scalar_ty) -> _ = $wrap;
            let mut values = Vec::with_capacity(raw_bits.len());
            // One call for the whole column: a dispatching backend selects its
            // implementation once here rather than once per 8 rows.
            $convert(raw_bits, valid.as_deref(), &mut values, wrap);
            OwnedColumnBuffer::$buffer { values, valid }
        }
    };
}

materialize_or_f64!(
    materialize_staged_i64_or_f64_column,
    min = I64_MIN_F64,
    max = I64_MAX_F64,
    convert = convert_column_i64,
    scalar_ty = i64,
    wrap = |x| x,
    buffer = I64,
);

materialize_or_f64!(
    materialize_staged_date_or_f64_column,
    min = I32_MIN_F64,
    max = I32_MAX_F64,
    convert = convert_column_i32,
    scalar_ty = i32,
    wrap = |x| SasDate {
        days_since_sas_epoch: x
    },
    buffer = Date,
);

materialize_or_f64!(
    materialize_staged_datetime_or_f64_column,
    min = I64_MIN_F64,
    max = I64_MAX_F64,
    convert = convert_column_i64,
    scalar_ty = i64,
    wrap = |x| SasDateTime {
        seconds_since_sas_epoch: x
    },
    buffer = DateTime,
);

materialize_or_f64!(
    materialize_staged_time_or_f64_column,
    min = I32_MIN_F64,
    max = I32_MAX_F64,
    convert = convert_column_i32,
    scalar_ty = i32,
    wrap = |x| SasTime {
        seconds_since_midnight: x
    },
    buffer = Time,
);

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
/// Mirrors the acceptance criteria of [`first_non_integral_in_range_index`]
/// with the i64 bounds, so a scalar scan with this predicate locates exactly the
/// value that forced [`materialize_staged_i64_or_f64_column`] to fall back to F64.
pub(super) fn f64_is_i64_representable(value: f64) -> bool {
    // `trunc() == value` is a deliberate exact integer-ness test, not an
    // approximate comparison — an epsilon margin would be wrong here.
    #[allow(clippy::float_cmp)]
    {
        value.is_finite() && value.trunc() == value && (I64_MIN_F64..=I64_MAX_F64).contains(&value)
    }
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
