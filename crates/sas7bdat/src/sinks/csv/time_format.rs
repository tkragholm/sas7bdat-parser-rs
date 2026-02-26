use crate::error::{Error, Result};
use time::{Date, Duration, OffsetDateTime};

/// Write a `YYYY-MM-DD` date directly into `out` without allocating a String.
fn write_date_components(date: Date, out: &mut Vec<u8>) {
    let year = date.year();
    // Fast path for signed four-digit years: `-9999..=9999`.
    // For wider years, fall back to `Date` formatting to preserve semantics.
    if (-9999..=9999).contains(&year) {
        if year < 0 {
            out.push(b'-');
        }
        let y = year.unsigned_abs();
        write_two((y / 100) as u8, out);
        write_two((y % 100) as u8, out);
    } else {
        out.extend_from_slice(date.to_string().as_bytes());
        return;
    }
    out.push(b'-');
    write_two(u8::from(date.month()), out);
    out.push(b'-');
    write_two(date.day(), out);
}

pub fn write_date(dt: &OffsetDateTime, out: &mut Vec<u8>) {
    write_date_components(dt.date(), out);
}

pub fn write_datetime(dt: &OffsetDateTime, out: &mut Vec<u8>) {
    // Round to milliseconds like the integration fixtures and render "YYYY-MM-DD HH:MM:SS[.mmm]"
    let rounded = round_to_millisecond(dt);
    let date = rounded.date();
    let time = rounded.time();
    write_date_components(date, out);
    out.push(b' ');
    write_two(time.hour(), out);
    out.push(b':');
    write_two(time.minute(), out);
    out.push(b':');
    write_two(time.second(), out);
    let nanos = time.nanosecond();
    if nanos != 0 {
        out.push(b'.');
        let millis = nanos / 1_000_000;
        let millis_u16 = u16::try_from(millis).unwrap_or(0);
        write_three(millis_u16, out);
    }
}

pub fn write_time(dur: &Duration, out: &mut Vec<u8>) -> Result<()> {
    // Render HH:MM:SS[.mmm]
    let mut total_seconds = dur.whole_seconds();
    let nanos_total = dur.whole_nanoseconds();
    let nanos = nanos_total - i128::from(total_seconds) * 1_000_000_000;
    let mut millis =
        i64::try_from((nanos.abs() + 500_000) / 1_000_000).map_err(|_| Error::InvalidMetadata {
            details: std::borrow::Cow::from("time millisecond rounding overflow"),
        })?;
    if millis >= 1000 {
        millis = 0;
        total_seconds += if nanos_total >= 0 { 1 } else { -1 };
    }

    let mut remaining = total_seconds;
    let hours = remaining.div_euclid(3600);
    remaining -= hours * 3600;
    let minutes = remaining.div_euclid(60);
    remaining -= minutes * 60;
    let seconds = remaining;

    let hours_u8 = u8::try_from(hours).map_err(|_| Error::InvalidMetadata {
        details: std::borrow::Cow::from("time hours component out of range for CSV formatting"),
    })?;
    let minutes_u8 = u8::try_from(minutes).map_err(|_| Error::InvalidMetadata {
        details: std::borrow::Cow::from("time minutes component out of range for CSV formatting"),
    })?;
    let seconds_u8 = u8::try_from(seconds).map_err(|_| Error::InvalidMetadata {
        details: std::borrow::Cow::from("time seconds component out of range for CSV formatting"),
    })?;

    write_two(hours_u8, out);
    out.push(b':');
    write_two(minutes_u8, out);
    out.push(b':');
    write_two(seconds_u8, out);

    if millis != 0 {
        out.push(b'.');
        let millis_u16 = u16::try_from(millis).map_err(|_| Error::InvalidMetadata {
            details: std::borrow::Cow::from(
                "time milliseconds component out of range for CSV formatting",
            ),
        })?;
        write_three(millis_u16, out);
    }
    Ok(())
}

fn round_to_millisecond(dt: &OffsetDateTime) -> OffsetDateTime {
    use time::Duration as TDuration;
    let nanos = u64::from(dt.time().nanosecond());
    let mut millis = (nanos + 500_000) / 1_000_000;
    let mut adjusted = *dt;
    if millis == 1_000 {
        millis = 0;
        if let Some(next) = adjusted.checked_add(TDuration::seconds(1)) {
            adjusted = next;
        } else {
            return *dt;
        }
    }
    let new_nanos = u32::try_from(millis * 1_000_000).unwrap_or(0);
    adjusted.replace_nanosecond(new_nanos).unwrap_or(*dt)
}

#[inline]
pub fn write_two(v: u8, out: &mut Vec<u8>) {
    out.push(b'0' + (v / 10));
    out.push(b'0' + (v % 10));
}

#[inline]
pub fn write_three(v: u16, out: &mut Vec<u8>) {
    let hundreds = v / 100;
    let tens = (v / 10) % 10;
    let ones = v % 10;
    let hundreds_u8 = u8::try_from(hundreds).unwrap_or(0);
    let tens_u8 = u8::try_from(tens).unwrap_or(0);
    let ones_u8 = u8::try_from(ones).unwrap_or(0);
    out.push(b'0' + hundreds_u8);
    out.push(b'0' + tens_u8);
    out.push(b'0' + ones_u8);
}

#[cfg(test)]
mod tests {
    use super::write_date;
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

    fn utc_date(year: i32, month: Month, day: u8) -> OffsetDateTime {
        PrimitiveDateTime::new(
            Date::from_calendar_date(year, month, day).expect("valid test date"),
            Time::MIDNIGHT,
        )
        .assume_utc()
    }

    #[test]
    fn write_date_preserves_sign_for_negative_years() {
        let dt = utc_date(-1, Month::January, 2);
        let mut out = Vec::new();
        write_date(&dt, &mut out);
        assert_eq!(String::from_utf8(out).expect("utf8"), "-0001-01-02");
    }

    #[test]
    fn write_date_zero_pads_positive_years() {
        let dt = utc_date(42, Month::March, 5);
        let mut out = Vec::new();
        write_date(&dt, &mut out);
        assert_eq!(String::from_utf8(out).expect("utf8"), "0042-03-05");
    }
}
