// Golden parity tests: compare SIMD parser output row-by-row against CSV
// snapshots committed in the sas7bdat-parser-rs reference implementation.
//
// CSV files live at: sas7bdat-parser-rs/crates/sas7bdat/tests/csv_golden/
// SAS files live at: fixtures/raw_data/pandas/
//
// All tests are skipped silently when fixtures are absent (CI without full tree).

use sas7bdat::{Dataset, DecodeMode, OwnedCellValue};
use std::path::Path;

fn workspace_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..") // crates/sas7bdat → crates
        .join("..") // crates → workspace root
}

fn sas_fixture(name: &str) -> std::path::PathBuf {
    workspace_root().join("fixtures/raw_data/pandas").join(name)
}

fn csv_golden(name: &str) -> std::path::PathBuf {
    workspace_root()
        .join("sas7bdat-parser-rs/crates/sas7bdat/tests/csv_golden")
        .join(name)
}

// SAS epoch is 1960-01-01 = Julian Day Number 2436935.
fn sas_date_to_string(days: i32) -> String {
    let jdn = days as i64 + 2_436_935;
    let l = jdn + 68569;
    let n = 4 * l / 146097;
    let l = l - (146097 * n + 3) / 4;
    let i = 4000 * (l + 1) / 1_461_001;
    let l = l - 1461 * i / 4 + 31;
    let j = 80 * l / 2447;
    let day = l - 2447 * j / 80;
    let l = j / 11;
    let month = j + 2 - 12 * l;
    let year = 100 * (n - 49) + i + l;
    format!("{year:04}-{month:02}-{day:02}")
}

fn sas_datetime_to_string(seconds: i64) -> String {
    let day = seconds.div_euclid(86400) as i32;
    let sec_of_day = seconds.rem_euclid(86400) as u32;
    let h = sec_of_day / 3600;
    let m = (sec_of_day % 3600) / 60;
    let s = sec_of_day % 60;
    format!("{} {h:02}:{m:02}:{s:02}", sas_date_to_string(day))
}

struct CsvFixture {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn load_csv(path: &Path) -> CsvFixture {
    let mut rdr = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)
        .unwrap_or_else(|e| panic!("open csv {}: {e}", path.display()));
    let columns = rdr
        .headers()
        .expect("csv headers")
        .iter()
        .map(str::to_owned)
        .collect();
    let rows = rdr
        .records()
        .enumerate()
        .map(|(i, r)| {
            r.unwrap_or_else(|e| panic!("csv row {i}: {e}"))
                .iter()
                .map(str::to_owned)
                .collect()
        })
        .collect();
    CsvFixture { columns, rows }
}

// Try to parse a CSV cell that looks like "YYYY-MM-DD[ HH:MM:SS[...]]" and
// return the "YYYY-MM-DD HH:MM:SS" prefix (to-second precision).
fn csv_datetime_prefix(s: &str) -> Option<&str> {
    let s = s.trim();
    if s.len() >= 10 && s.as_bytes()[4] == b'-' && s.as_bytes()[7] == b'-' {
        // Date or datetime string
        Some(if s.len() > 19 { &s[..19] } else { s })
    } else {
        None
    }
}

fn compare_cell(
    actual: &OwnedCellValue,
    expected_csv: &str,
    col: &str,
    row: usize,
    fixture: &str,
) {
    // Raw-byte columns have no portable text representation; skip entirely.
    if matches!(actual, OwnedCellValue::Bytes(_)) {
        return;
    }

    // SAS system-missing encodes as NaN in TypedLossless mode.
    let actual_is_missing = matches!(actual, OwnedCellValue::Null)
        || matches!(actual, OwnedCellValue::Float64(f) if f.is_nan());

    if expected_csv.trim().is_empty() {
        assert!(
            actual_is_missing,
            "{fixture} row {row} col {col}: expected null (empty CSV), got {actual:?}"
        );
        return;
    }
    if actual_is_missing {
        panic!("{fixture} row {row} col {col}: got null/NaN but CSV has {expected_csv:?}");
    }

    match actual {
        OwnedCellValue::Null | OwnedCellValue::Bytes(_) => unreachable!(),

        // TypedLossless returns Float64 for ALL SAS numeric columns, including
        // those with Date/DateTime/Time logical types (which are stored as floats
        // internally in SAS). NaN represents SAS system-missing.
        OwnedCellValue::Float64(f) => {
            // SAS system missing → NaN → treat as null.
            if f.is_nan() {
                assert!(
                    expected_csv.trim().is_empty(),
                    "{fixture} row {row} col {col}: got NaN (missing) but CSV has {expected_csv:?}"
                );
                return;
            }
            // Plain numeric: compare with tolerance loose enough to accept
            // SAS floating-point encoding artifacts (e.g. 1957.00036436 vs 1957).
            if let Ok(expected_f) = expected_csv.parse::<f64>() {
                let tol = 1e-5 * f.abs().max(1.0);
                assert!(
                    (f - expected_f).abs() <= tol,
                    "{fixture} row {row} col {col}: numeric mismatch {f} vs {expected_f}"
                );
                return;
            }
            // CSV looks like a date/datetime string. Determine the type by the
            // magnitude of the float value:
            //   |f| < 500_000   → SAS date (days since 1960-01-01)
            //   |f| >= 500_000  → SAS datetime (seconds since 1960-01-01 00:00)
            if let Some(expected_prefix) = csv_datetime_prefix(expected_csv) {
                if f.abs() < 500_000.0 {
                    // Date column (days).
                    let actual_date = sas_date_to_string(f.round() as i32);
                    let expected_date = &expected_prefix[..10.min(expected_prefix.len())];
                    assert_eq!(
                        actual_date, expected_date,
                        "{fixture} row {row} col {col}: date(float) mismatch"
                    );
                } else {
                    // Datetime column (seconds, possibly fractional).
                    // Use floor() so negative pre-1960 timestamps round the same
                    // direction as the reference parser (toward negative infinity).
                    let actual_dt = sas_datetime_to_string(f.floor() as i64);
                    assert_eq!(
                        actual_dt, expected_prefix,
                        "{fixture} row {row} col {col}: datetime(float) mismatch"
                    );
                }
                return;
            }
            // Time values in the CSV may be raw seconds (integer string).
            if let Ok(csv_secs) = expected_csv.trim().parse::<i64>() {
                let actual_secs = f.round() as i64;
                assert_eq!(
                    actual_secs, csv_secs,
                    "{fixture} row {row} col {col}: time(float-secs) mismatch"
                );
                return;
            }
            panic!(
                "{fixture} row {row} col {col}: Float64({f}) vs unrecognized CSV {expected_csv:?}"
            );
        }

        OwnedCellValue::Int32(n) => {
            if let Ok(expected_f) = expected_csv.parse::<f64>() {
                let tol = 1e-7 * (*n as f64).abs().max(1.0);
                assert!(
                    (*n as f64 - expected_f).abs() <= tol,
                    "{fixture} row {row} col {col}: Int32 mismatch {n} vs {expected_f}"
                );
            } else {
                panic!("{fixture} row {row} col {col}: Int32({n}) vs non-numeric {expected_csv:?}");
            }
        }

        OwnedCellValue::Int64(n) => {
            if let Ok(expected_f) = expected_csv.parse::<f64>() {
                let tol = 1e-7 * (*n as f64).abs().max(1.0);
                assert!(
                    (*n as f64 - expected_f).abs() <= tol,
                    "{fixture} row {row} col {col}: Int64 mismatch {n} vs {expected_f}"
                );
            } else {
                panic!("{fixture} row {row} col {col}: Int64({n}) vs non-numeric {expected_csv:?}");
            }
        }

        OwnedCellValue::String(s) => {
            let actual_trimmed = s.trim_end();
            let expected_trimmed = expected_csv.trim();
            assert_eq!(
                actual_trimmed, expected_trimmed,
                "{fixture} row {row} col {col}: string mismatch"
            );
        }

        OwnedCellValue::Date(d) => {
            let actual_str = sas_date_to_string(d.days_since_sas_epoch);
            // The CSV prefix already strips any sub-day info.
            let expected_trimmed = expected_csv.trim();
            assert_eq!(
                actual_str, expected_trimmed,
                "{fixture} row {row} col {col}: date mismatch"
            );
        }

        OwnedCellValue::DateTime(dt) => {
            let actual_str = sas_datetime_to_string(dt.seconds_since_sas_epoch);
            // Compare to-second precision; CSV may have trailing microseconds.
            let expected_prefix = csv_datetime_prefix(expected_csv)
                .unwrap_or(expected_csv);
            assert_eq!(
                actual_str, expected_prefix,
                "{fixture} row {row} col {col}: datetime mismatch"
            );
        }

        OwnedCellValue::Time(t) => {
            // The golden CSV stores time as raw seconds since midnight (integer).
            if let Ok(expected_secs) = expected_csv.trim().parse::<i64>() {
                assert_eq!(
                    i64::from(t.seconds_since_midnight),
                    expected_secs,
                    "{fixture} row {row} col {col}: time (seconds) mismatch"
                );
            } else {
                // Fallback: CSV stored as HH:MM:SS.
                let sec = t.seconds_since_midnight;
                let h = sec / 3600;
                let m = (sec % 3600) / 60;
                let s = sec % 60;
                let actual_str = format!("{h:02}:{m:02}:{s:02}");
                assert_eq!(
                    actual_str,
                    expected_csv.trim(),
                    "{fixture} row {row} col {col}: time (HH:MM:SS) mismatch"
                );
            }
        }
    }
}

fn check_parity(sas_name: &str, csv_name: &str) {
    let sas_path = sas_fixture(sas_name);
    let csv_path = csv_golden(csv_name);
    if !sas_path.exists() || !csv_path.exists() {
        return;
    }

    let ds = Dataset::open(&sas_path)
        .unwrap_or_else(|e| panic!("open {sas_name}: {e}"));
    let golden = load_csv(&csv_path);
    let columns = ds.columns();

    // Column name parity (trim trailing spaces from SAS names).
    let actual_names: Vec<String> =
        columns.iter().map(|c| c.name.trim_end().to_owned()).collect();
    assert_eq!(
        actual_names, golden.columns,
        "{sas_name}: column name mismatch"
    );

    // TypedLossless: all numeric columns stay as Float64 (no integer rounding),
    // which matches the CSV golden format.
    let rows = ds
        .scan()
        .with_decode_mode(DecodeMode::TypedLossless)
        .collect_rows()
        .unwrap_or_else(|e| panic!("collect {sas_name}: {e}"));

    assert_eq!(
        rows.len(),
        golden.rows.len(),
        "{sas_name}: row count mismatch (actual={}, golden={})",
        rows.len(),
        golden.rows.len()
    );

    for (row_idx, (row, csv_row)) in rows.iter().zip(golden.rows.iter()).enumerate() {
        for (col_idx, (cell, csv_cell)) in row.cells.iter().zip(csv_row.iter()).enumerate() {
            let col_name = &columns[col_idx].name;
            compare_cell(cell, csv_cell, col_name, row_idx, sas_name);
        }
    }
}

#[test]
fn airline_matches_golden_csv() {
    check_parity("airline.sas7bdat", "airline.csv");
}

#[test]
fn controlbyte_matches_golden_csv() {
    check_parity("0x40controlbyte.sas7bdat", "0x40controlbyte.csv");
}

#[test]
fn datetime_matches_golden_csv() {
    check_parity("datetime.sas7bdat", "datetime.csv");
}

#[test]
fn many_columns_matches_golden_csv() {
    check_parity("many_columns.sas7bdat", "many_columns.csv");
}
