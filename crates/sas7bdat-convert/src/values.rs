//! Rendering SAS values as text for the delimited writers.
//!
//! Split out of the CLI alongside the rest of conversion: these turn a decoded cell into
//! the bytes a CSV or TSV row carries, which is library work. The CLI keeps its own
//! display helpers — thousands separators and human-readable byte counts — because those
//! format numbers for a terminal, not for a file.

use chrono::{Duration, NaiveDate, NaiveDateTime};
use sas7bdat::{CellValue, LogicalType};
use std::fmt::Write as _;

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
        CellValue::Float64(value) => {
            let mut out = String::new();
            if write_temporal_f64(&mut out, kind, *value, date_epoch, datetime_epoch) {
                out
            } else {
                value.to_string()
            }
        }
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
            let mut out = String::new();
            write_sas_time(&mut out, f64::from(value.seconds_since_midnight));
            out
        }
    }
}

/// Render a SAS TIME value — a signed count of seconds since midnight — into `out` as
/// `[-]HH:MM:SS[.fff]`.
///
/// Hours are neither wrapped at 24 nor clamped. SAS stores TIME as a plain numeric, and real
/// files carry values outside `[0, 24h)`: 359,280 seconds renders as `99:48:00`, not
/// `03:48:00`, because wrapping would print an instant the file does not contain. In-range
/// values are unaffected — they produce the same two-digit hour as before.
///
/// Both cell shapes of a TIME column come through here — whole seconds arrive as
/// [`CellValue::Time`], values with a fractional part widen to [`CellValue::Float64`] — so a
/// column renders one way down its whole length rather than alternating representations.
///
/// The sub-second part prints as milliseconds when it is a whole number of them, and as
/// microseconds otherwise, so a value is never rounded away to `.000`.
pub fn write_sas_time(out: &mut String, seconds: f64) {
    let micros = microseconds(seconds);
    if micros < 0 {
        out.push('-');
    }
    let magnitude = micros.unsigned_abs();
    let (whole, sub) = (magnitude / 1_000_000, magnitude % 1_000_000);
    let _ = write!(
        out,
        "{:02}:{:02}:{:02}",
        whole / 3600,
        (whole / 60) % 60,
        whole % 60
    );
    if sub % 1000 == 0 {
        if sub != 0 {
            let _ = write!(out, ".{:03}", sub / 1000);
        }
    } else {
        let _ = write!(out, ".{sub:06}");
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

/// Render a `Float64` datetime (raw seconds since the SAS epoch) into `out` as a timestamp,
/// preserving any sub-second part. Falls back to the raw number if out of chrono range.
pub fn write_datetime_f64(out: &mut String, epoch: NaiveDateTime, seconds: f64) {
    let micros = microseconds(seconds);
    let Some(dt) = epoch.checked_add_signed(Duration::microseconds(micros)) else {
        let _ = write!(out, "{seconds}");
        return;
    };
    if micros % 1_000_000 == 0 {
        let _ = write!(out, "{}", dt.format("%Y-%m-%d %H:%M:%S"));
    } else {
        let _ = write!(out, "{}", dt.format("%Y-%m-%d %H:%M:%S%.3f"));
    }
}

/// Render a `Float64` date (raw days since the SAS epoch, rounded to a whole day) into `out`.
pub fn write_date_f64(out: &mut String, epoch: NaiveDate, days: f64) {
    #[allow(clippy::cast_possible_truncation)]
    let whole = days.round() as i64;
    match epoch.checked_add_signed(Duration::days(whole)) {
        Some(date) => {
            let _ = write!(out, "{}", date.format("%Y-%m-%d"));
        }
        None => {
            let _ = write!(out, "{days}");
        }
    }
}

/// Render a temporal cell that widened to `Float64` (raw SAS-epoch units) into `out` as the
/// column's declared type. Returns `false` for a non-temporal `kind`, leaving `out` untouched
/// so the caller can write the value as a plain number.
///
/// Shared by [`format_cell`] and the CSV writer so the two never disagree about a cell.
pub fn write_temporal_f64(
    out: &mut String,
    kind: LogicalType,
    value: f64,
    date_epoch: NaiveDate,
    datetime_epoch: NaiveDateTime,
) -> bool {
    match kind {
        LogicalType::DateTime => write_datetime_f64(out, datetime_epoch, value),
        LogicalType::Date => write_date_f64(out, date_epoch, value),
        LogicalType::Time => write_sas_time(out, value),
        _ => return false,
    }
    true
}

#[cfg(test)]
mod tests {
    use super::format_cell;
    use sas7bdat::{CellValue, LogicalType, SasDate, SasDateTime};

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

    /// A SAS TIME outside `[0, 24h)` must render its real elapsed value, not a wrapped or
    /// clamped clock. These used to collapse to `00:00:00`: `NaiveTime` has no representation
    /// for them, and the fallback silently substituted midnight in both `head` and the CSV.
    #[test]
    fn format_cell_renders_out_of_range_times() {
        use sas7bdat::SasTime;
        let time = |seconds| {
            format_cell(
                &CellValue::Time(SasTime {
                    seconds_since_midnight: seconds,
                }),
                LogicalType::Time,
                "",
            )
        };
        // 359,280s is 99h48m — the value that read back as midnight.
        assert_eq!(time(359_280), "99:48:00");
        assert_eq!(time(86_400), "24:00:00");
        // Negative offsets failed the `u32::try_from` and hit the same fallback.
        assert_eq!(time(-77), "-00:01:17");
        // In-range values are untouched by the change.
        assert_eq!(time(0), "00:00:00");
        assert_eq!(time(69_507), "19:18:27");
    }

    /// The whole-second and widened-`Float64` cells of one TIME column must agree: they used
    /// to print as a clock string and a raw number respectively, in the same column.
    #[test]
    fn time_column_renders_one_way_for_both_cell_shapes() {
        use sas7bdat::SasTime;
        let t = LogicalType::Time;
        assert_eq!(
            format_cell(
                &CellValue::Time(SasTime {
                    seconds_since_midnight: 69_507
                }),
                t,
                ""
            ),
            "19:18:27"
        );
        assert_eq!(
            format_cell(&CellValue::Float64(69_507.95), t, ""),
            "19:18:27.950"
        );
        // Out-of-range on the float side wraps no more than on the integer side.
        assert_eq!(
            format_cell(&CellValue::Float64(359_960.4), t, ""),
            "99:59:20.400"
        );
        // Sub-millisecond precision survives rather than rounding to `.000`.
        assert_eq!(
            format_cell(&CellValue::Float64(1.000_1), t, ""),
            "00:00:01.000100"
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
