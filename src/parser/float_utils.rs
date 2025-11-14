use std::convert::TryFrom;

#[inline]
pub(super) fn exact_integer_from_f64(value: f64) -> Option<i128> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some(0);
    }

    let bits = value.to_bits();
    let sign = (bits >> 63) != 0;
    let exponent_bits = ((bits >> 52) & 0x7FF) as i32;

    if exponent_bits == 0 {
        return None;
    }

    let exponent = exponent_bits - 1023;
    if exponent < 0 {
        return None;
    }

    let mut mantissa = bits & ((1_u64 << 52) - 1);
    mantissa |= 1_u64 << 52;

    let magnitude_bits = if exponent >= 52 {
        let shift = u32::try_from(exponent - 52).ok()?;
        mantissa.checked_shl(shift)?
    } else {
        let shift = u32::try_from(52 - exponent).ok()?;
        let mask = (1_u64 << shift) - 1;
        if mantissa & mask != 0 {
            return None;
        }
        mantissa >> shift
    };

    let magnitude = i128::from(magnitude_bits);
    Some(if sign { -magnitude } else { magnitude })
}

#[inline]
pub(super) fn try_int_from_f64<T>(value: f64) -> Option<T>
where
    T: TryFrom<i128>,
{
    exact_integer_from_f64(value).and_then(|int| T::try_from(int).ok())
}
