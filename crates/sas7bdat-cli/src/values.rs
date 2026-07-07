//! Render a borrowed [`CellValue`] as a human-readable display string, used by the
//! `head` preview and the `info` sample. (CSV export has its own zero-alloc path.)

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use sas7bdat::{CellValue, LogicalType};
use std::fmt::Write as _;

/// Group a number with thousands separators (e.g. `70044` -> `70,044`).
#[must_use]
pub fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let len = digits.len();
    let mut out = String::with_capacity(len + len / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Format a byte count as a compact human-readable size (e.g. `1.2 KB`).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// The SAS epoch (`1960-01-01`) as a date and as a midnight datetime.
// `Option::expect` isn't const-stable, so this can't be a `const fn` despite being constant.
#[allow(clippy::missing_const_for_fn)]
fn sas_epochs() -> (NaiveDate, NaiveDateTime) {
    let date = NaiveDate::from_ymd_opt(1960, 1, 1).expect("valid SAS epoch");
    let datetime = date.and_hms_opt(0, 0, 0).expect("valid SAS epoch time");
    (date, datetime)
}

/// Format one cell for display. `kind` is the column's logical type, used to render a
/// temporal value that widened to `Float64` (a sub-second/out-of-range datetime/date/time
/// carries raw SAS-epoch units as a float) as a timestamp rather than a bare number —
/// matching the Parquet/Polars/R output. `null_repr` is substituted for missing values.
// The `expect`s here operate on compile-time constants (the SAS epoch and midnight) that
// cannot fail, so the function does not panic on any input.
#[must_use]
#[allow(clippy::missing_panics_doc)]
pub fn format_cell(cell: &CellValue<'_>, kind: LogicalType, null_repr: &str) -> String {
    let (date_epoch, datetime_epoch) = sas_epochs();
    match cell {
        CellValue::Null => null_repr.to_owned(),
        CellValue::Str(value) => value.trim_end().to_owned(),
        CellValue::Int32(value) => value.to_string(),
        CellValue::Int64(value) => value.to_string(),
        // A temporal column whose cell didn't fit a whole integer unit arrives here as a
        // raw `Float64` of SAS-epoch units; render it as the column's declared type.
        CellValue::Float64(value) => match kind {
            LogicalType::DateTime => format_datetime_f64(datetime_epoch, *value),
            LogicalType::Date => format_date_f64(date_epoch, *value),
            LogicalType::Time => format_time_f64(*value),
            _ => value.to_string(),
        },
        CellValue::Bytes(value) => {
            let mut out = String::with_capacity(2 + value.len() * 2);
            out.push_str("0x");
            for byte in *value {
                let _ = write!(out, "{byte:02x}");
            }
            out
        }
        CellValue::Date(value) => {
            let date = date_epoch + Duration::days(i64::from(value.days_since_sas_epoch));
            date.format("%Y-%m-%d").to_string()
        }
        CellValue::DateTime(value) => {
            let datetime = datetime_epoch + Duration::seconds(value.seconds_since_sas_epoch);
            datetime.format("%Y-%m-%d %H:%M:%S").to_string()
        }
        CellValue::Time(value) => {
            let seconds = u32::try_from(value.seconds_since_midnight).unwrap_or(0);
            NaiveTime::from_num_seconds_from_midnight_opt(seconds, 0)
                .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).expect("valid midnight"))
                .format("%H:%M:%S")
                .to_string()
        }
    }
}

/// Total microseconds for a signed `f64` of seconds. Scaling before rounding (rather than
/// splitting whole/fractional) keeps precision for large-magnitude SAS datetimes and
/// matches the microsecond computation the Parquet/Polars paths use. The SAS range fits
/// `i64` microseconds comfortably (year 9999 ≈ 2.5e17 µs « `i64::MAX`).
#[allow(clippy::cast_possible_truncation)]
fn microseconds(seconds: f64) -> i64 {
    (seconds * 1_000_000.0).round() as i64
}

/// Render a `Float64` datetime (raw seconds since the SAS epoch) as a timestamp,
/// preserving any sub-second part. Falls back to the raw number if out of chrono range.
fn format_datetime_f64(epoch: NaiveDateTime, seconds: f64) -> String {
    let micros = microseconds(seconds);
    let Some(dt) = epoch.checked_add_signed(Duration::microseconds(micros)) else {
        return seconds.to_string();
    };
    if micros % 1_000_000 == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }
}

/// Render a `Float64` date (raw days since the SAS epoch, rounded to a whole day).
fn format_date_f64(epoch: NaiveDate, days: f64) -> String {
    #[allow(clippy::cast_possible_truncation)]
    let whole = days.round() as i64;
    epoch.checked_add_signed(Duration::days(whole)).map_or_else(
        || days.to_string(),
        |date| date.format("%Y-%m-%d").to_string(),
    )
}

/// Render a `Float64` time (raw seconds since midnight) as `HH:MM:SS[.fff]`. Wraps within
/// the day (SAS times are seconds-since-midnight, occasionally outside `[0, 24h)`).
fn format_time_f64(seconds: f64) -> String {
    let micros = microseconds(seconds);
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("valid midnight");
    let (time, _) = midnight.overflowing_add_signed(Duration::microseconds(micros));
    if micros % 1_000_000 == 0 {
        time.format("%H:%M:%S").to_string()
    } else {
        time.format("%H:%M:%S%.3f").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{format_cell, human_bytes};
    use sas7bdat::{CellValue, LogicalType, SasDate, SasDateTime};

    #[test]
    fn thousands_groups_digits() {
        use super::thousands;
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(42), "42");
        assert_eq!(thousands(4041), "4,041");
        assert_eq!(thousands(70044), "70,044");
        assert_eq!(thousands(1_234_567), "1,234,567");
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KB");
        assert_eq!(human_bytes(1536), "1.5 KB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MB");
    }

    #[test]
    fn format_cell_renders_scalars_and_nulls() {
        let f = LogicalType::Float;
        assert_eq!(format_cell(&CellValue::Null, f, "NA"), "NA");
        assert_eq!(format_cell(&CellValue::Null, f, ""), "");
        assert_eq!(format_cell(&CellValue::Int32(5), f, ""), "5");
        assert_eq!(format_cell(&CellValue::Int64(-9), f, ""), "-9");
        assert_eq!(format_cell(&CellValue::Float64(1.5), f, ""), "1.5");
        // Strings have trailing fixed-width padding trimmed.
        assert_eq!(
            format_cell(&CellValue::Str("ab  "), LogicalType::String, ""),
            "ab"
        );
        assert_eq!(
            format_cell(&CellValue::Bytes(&[0x0f, 0xa0]), f, ""),
            "0x0fa0"
        );
    }

    #[test]
    fn format_cell_renders_temporal_values() {
        // SAS day 0 is 1960-01-01; the Unix epoch (1970-01-01) is SAS day 3653.
        assert_eq!(
            format_cell(
                &CellValue::Date(SasDate {
                    days_since_sas_epoch: 3653
                }),
                LogicalType::Date,
                ""
            ),
            "1970-01-01"
        );
        assert_eq!(
            format_cell(
                &CellValue::DateTime(SasDateTime {
                    seconds_since_sas_epoch: 315_619_200
                }),
                LogicalType::DateTime,
                ""
            ),
            "1970-01-01 00:00:00"
        );
    }

    #[test]
    fn format_cell_renders_widened_fractional_temporals() {
        // A sub-second/drifting datetime widens to Float64 (raw SAS seconds). With the
        // column's logical type it must still render as a timestamp, not a bare number.
        // -0.001 SAS seconds is 1959-12-31 23:59:59.999 (just before the 1960 epoch).
        assert_eq!(
            format_cell(&CellValue::Float64(-0.001), LogicalType::DateTime, ""),
            "1959-12-31 23:59:59.999"
        );
        // A whole-second float datetime drops the sub-second suffix.
        assert_eq!(
            format_cell(
                &CellValue::Float64(315_619_200.0),
                LogicalType::DateTime,
                ""
            ),
            "1970-01-01 00:00:00"
        );
        // Fractional time -> HH:MM:SS.fff since midnight.
        assert_eq!(
            format_cell(&CellValue::Float64(3661.5), LogicalType::Time, ""),
            "01:01:01.500"
        );
        // The same float in a plain numeric column stays a number.
        assert_eq!(
            format_cell(&CellValue::Float64(-0.001), LogicalType::Float, ""),
            "-0.001"
        );
    }
}
