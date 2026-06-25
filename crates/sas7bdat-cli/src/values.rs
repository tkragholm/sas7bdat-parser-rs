//! Render a borrowed [`CellValue`] as a human-readable display string, used by the
//! `head` preview and the `info` sample. (CSV export has its own zero-alloc path.)

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use sas7bdat::CellValue;
use std::fmt::Write as _;

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

/// Format one cell for display. `null_repr` is substituted for missing values so the
/// caller can pick a placeholder (e.g. empty, or a dimmed marker).
// The `expect`s here operate on compile-time constants (the SAS epoch and midnight) that
// cannot fail, so the function does not panic on any input.
#[must_use]
#[allow(clippy::missing_panics_doc)]
pub fn format_cell(cell: &CellValue<'_>, null_repr: &str) -> String {
    let (date_epoch, datetime_epoch) = sas_epochs();
    match cell {
        CellValue::Null => null_repr.to_owned(),
        CellValue::Str(value) => value.trim_end().to_owned(),
        CellValue::Int32(value) => value.to_string(),
        CellValue::Int64(value) => value.to_string(),
        CellValue::Float64(value) => value.to_string(),
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

#[cfg(test)]
mod tests {
    use super::{format_cell, human_bytes};
    use sas7bdat::{CellValue, SasDate, SasDateTime};

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KB");
        assert_eq!(human_bytes(1536), "1.5 KB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MB");
    }

    #[test]
    fn format_cell_renders_scalars_and_nulls() {
        assert_eq!(format_cell(&CellValue::Null, "NA"), "NA");
        assert_eq!(format_cell(&CellValue::Null, ""), "");
        assert_eq!(format_cell(&CellValue::Int32(5), ""), "5");
        assert_eq!(format_cell(&CellValue::Int64(-9), ""), "-9");
        assert_eq!(format_cell(&CellValue::Float64(1.5), ""), "1.5");
        // Strings have trailing fixed-width padding trimmed.
        assert_eq!(format_cell(&CellValue::Str("ab  "), ""), "ab");
        assert_eq!(format_cell(&CellValue::Bytes(&[0x0f, 0xa0]), ""), "0x0fa0");
    }

    #[test]
    fn format_cell_renders_temporal_values() {
        // SAS day 0 is 1960-01-01; the Unix epoch (1970-01-01) is SAS day 3653.
        assert_eq!(
            format_cell(&CellValue::Date(SasDate { days_since_sas_epoch: 3653 }), ""),
            "1970-01-01"
        );
        assert_eq!(
            format_cell(
                &CellValue::DateTime(SasDateTime { seconds_since_sas_epoch: 315_619_200 }),
                ""
            ),
            "1970-01-01 00:00:00"
        );
    }
}
