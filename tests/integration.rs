use csv::ReaderBuilder;
use sas7bdat_parser_rs::SasFile;
use sas7bdat_parser_rs::value::Value;
use std::path::Path;
use std::sync::OnceLock;
use time::format_description::{self, FormatItem};
use time::{Date, Duration, OffsetDateTime, PrimitiveDateTime, Time};

#[test]
fn fixtures_match_csv_rows() {
    let cases = [
        (
            "0x40controlbyte.sas7bdat",
            "tests/csv_golden/0x40controlbyte.csv",
        ),
        ("many_columns.sas7bdat", "tests/csv_golden/many_columns.csv"),
        ("airline.sas7bdat", "tests/csv_golden/airline.csv"),
        ("datetime.sas7bdat", "tests/csv_golden/datetime.csv"),
    ];

    for (sas_file, csv_path) in cases {
        let sas_path = Path::new("fixtures/raw_data/pandas").join(sas_file);
        let mut sas = SasFile::open(&sas_path)
            .unwrap_or_else(|err| panic!("failed to open {}: {}", sas_path.display(), err));
        let metadata = sas.metadata().clone();
        let csv_fixture = load_csv_fixture(csv_path);

        let variable_names: Vec<_> = metadata
            .variables
            .iter()
            .map(|v| v.name.trim_end().to_string())
            .collect();
        assert_eq!(
            variable_names, csv_fixture.columns,
            "column names mismatch for {}",
            sas_file
        );
        assert_eq!(
            metadata.row_count as usize,
            csv_fixture.rows.len(),
            "row count mismatch for {}",
            sas_file
        );

        let mut rows = sas.rows().unwrap_or_else(|err| {
            panic!("failed to create row iterator for {}: {}", sas_file, err)
        });
        for (index, expected_row) in csv_fixture.rows.iter().enumerate() {
            let actual_row = rows
                .try_next()
                .unwrap_or_else(|err| {
                    panic!("error reading row {} for {}: {}", index, sas_file, err)
                })
                .unwrap_or_else(|| panic!("missing row {} for {}", index, sas_file));
            for ((value, var), expected) in actual_row
                .iter()
                .zip(metadata.variables.iter())
                .zip(expected_row.iter())
            {
                assert_value_matches_csv(value, expected, var.name.trim_end(), index, sas_file);
            }
        }

        if let Some(extra) = rows
            .try_next()
            .unwrap_or_else(|err| panic!("error reading trailing row for {}: {}", sas_file, err))
        {
            panic!("found unexpected extra row {:?} in {}", extra, sas_file);
        }
    }
}

#[derive(Debug)]
struct CsvFixture {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn format_iso_seconds(dt: &OffsetDateTime) -> String {
    let rounded = round_to_millisecond(dt);
    let date = rounded.date();
    let time = rounded.time();
    if time.nanosecond() == 0 {
        format!(
            "{} {:02}:{:02}:{:02}",
            date,
            time.hour(),
            time.minute(),
            time.second()
        )
    } else {
        format!(
            "{} {:02}:{:02}:{:02}.{:03}",
            date,
            time.hour(),
            time.minute(),
            time.second(),
            time.nanosecond() / 1_000_000
        )
    }
}

fn format_iso_date(dt: &OffsetDateTime) -> String {
    dt.date().to_string()
}

fn round_to_millisecond(dt: &OffsetDateTime) -> OffsetDateTime {
    let nanos = dt.time().nanosecond() as u64;
    let mut millis = (nanos + 500_000) / 1_000_000;
    let mut adjusted = *dt;
    if millis == 1_000 {
        millis = 0;
        if let Some(next) = adjusted.checked_add(Duration::seconds(1)) {
            adjusted = next;
        } else {
            return *dt;
        }
    }
    let new_nanos = (millis * 1_000_000) as u32;
    adjusted.replace_nanosecond(new_nanos).unwrap_or(*dt)
}

fn load_csv_fixture(path: &str) -> CsvFixture {
    let mut reader = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)
        .unwrap_or_else(|err| panic!("failed to open csv fixture {}: {}", path, err));

    let headers = reader
        .headers()
        .unwrap_or_else(|err| panic!("failed to read headers in {}: {}", path, err))
        .clone();
    let columns = headers.iter().map(|h| h.to_string()).collect();

    let mut rows = Vec::new();
    for (idx, record) in reader.records().enumerate() {
        let record =
            record.unwrap_or_else(|err| panic!("failed reading row {} in {}: {}", idx, path, err));
        rows.push(record.iter().map(|field| field.to_string()).collect());
    }

    CsvFixture { columns, rows }
}

fn assert_value_matches_csv(
    value: &Value<'_>,
    expected: &str,
    column: &str,
    row_idx: usize,
    file: &str,
) {
    if expected.is_empty() {
        match value {
            Value::Missing(_) => return,
            Value::Str(s) | Value::NumericString(s) => {
                assert!(
                    s.is_empty(),
                    "expected empty string treated as missing for {} in {} row {} but got {:?}",
                    column,
                    file,
                    row_idx,
                    value
                );
            }
            Value::Bytes(bytes) => {
                assert!(
                    bytes.is_empty(),
                    "expected empty bytes treated as missing for {} in {} row {} but got {:?}",
                    column,
                    file,
                    row_idx,
                    value
                );
            }
            _ => panic!(
                "expected missing value for empty csv field {} in {} row {}, got {:?}",
                column, file, row_idx, value
            ),
        }
        return;
    }

    match value {
        Value::Float(actual) => {
            let expected = expected.parse::<f64>().unwrap_or_else(|err| {
                panic!(
                    "csv float parse failed for {} in {} row {}: {} (value {:?})",
                    column, file, row_idx, err, expected
                )
            });
            assert!(
                (actual - expected).abs() <= 1e-6,
                "float mismatch for {} in {} row {}: actual {}, expected {}",
                column,
                file,
                row_idx,
                actual,
                expected
            );
        }
        Value::Int32(actual) => {
            let expected = parse_expected_integer(expected, column, file, row_idx);
            let expected = i32::try_from(expected).unwrap_or_else(|_| {
                panic!(
                    "csv int parse failed for {} in {} row {}: value {:?} out of i32 range",
                    column, file, row_idx, expected
                )
            });
            assert_eq!(
                *actual, expected,
                "int32 mismatch for {} in {} row {}: actual {}, expected {}",
                column, file, row_idx, actual, expected
            );
        }
        Value::Int64(actual) => {
            let expected = parse_expected_integer(expected, column, file, row_idx);
            assert_eq!(
                *actual, expected,
                "int64 mismatch for {} in {} row {}: actual {}, expected {}",
                column, file, row_idx, actual, expected
            );
        }
        Value::NumericString(actual) | Value::Str(actual) => {
            assert_eq!(
                actual.as_ref(),
                expected,
                "string mismatch for {} in {} row {}: actual {:?}, expected {:?}",
                column,
                file,
                row_idx,
                actual,
                expected
            );
        }
        Value::Bytes(actual) => {
            let actual_text = String::from_utf8_lossy(actual);
            assert_eq!(
                actual_text, expected,
                "byte string mismatch for {} in {} row {}: actual {:?}, expected {:?}",
                column, file, row_idx, actual_text, expected
            );
        }
        Value::DateTime(actual) => {
            let expected_dt = parse_csv_datetime(expected).unwrap_or_else(|| {
                panic!(
                    "failed to parse csv datetime for {} in {} row {}: {:?}",
                    column, file, row_idx, expected
                )
            });
            let expected_str = format_iso_seconds(&expected_dt.assume_utc());
            let actual_str = format_iso_seconds(actual);
            assert_eq!(
                actual_str, expected_str,
                "datetime mismatch for {} in {} row {}: actual {}, expected {}",
                column, file, row_idx, actual_str, expected_str
            );
        }
        Value::Date(actual) => {
            let expected_date = parse_csv_date(expected).unwrap_or_else(|| {
                panic!(
                    "failed to parse csv date for {} in {} row {}: {:?}",
                    column, file, row_idx, expected
                )
            });
            let expected_str = format_iso_date(&expected_date.assume_utc());
            let actual_str = format_iso_date(actual);
            assert_eq!(
                actual_str, expected_str,
                "date mismatch for {} in {} row {}: actual {}, expected {}",
                column, file, row_idx, actual_str, expected_str
            );
        }
        Value::Time(actual) => {
            let expected_duration = parse_csv_time(expected).unwrap_or_else(|| {
                panic!(
                    "failed to parse csv time for {} in {} row {}: {:?}",
                    column, file, row_idx, expected
                )
            });
            assert!(
                (*actual - expected_duration).abs() <= Duration::microseconds(1),
                "time mismatch for {} in {} row {}: actual {:?}, expected {:?}",
                column,
                file,
                row_idx,
                actual,
                expected_duration
            );
        }
        Value::Missing(_) => panic!(
            "unexpected missing value for {} in {} row {} while csv had {:?}",
            column, file, row_idx, expected
        ),
    }
}

fn parse_expected_integer(expected: &str, column: &str, file: &str, row_idx: usize) -> i64 {
    if let Ok(value) = expected.parse::<i64>() {
        return value;
    }

    let float_value = expected.parse::<f64>().unwrap_or_else(|err| {
        panic!(
            "csv int parse failed for {} in {} row {}: {} (value {:?})",
            column, file, row_idx, err, expected
        )
    });

    const INTEGER_TOLERANCE: f64 = 1e-3;

    let rounded = float_value.round();
    if (float_value - rounded).abs() > INTEGER_TOLERANCE {
        panic!(
            "csv int parse failed for {} in {} row {}: non-integer float {} (value {:?})",
            column, file, row_idx, float_value, expected
        );
    }

    if rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
        panic!(
            "csv int parse failed for {} in {} row {}: value {:?} out of i64 range",
            column, file, row_idx, expected
        );
    }

    rounded as i64
}

fn parse_csv_time(field: &str) -> Option<Duration> {
    if field.contains(':') {
        let parts: Vec<_> = field.split(':').collect();
        if parts.len() == 3 {
            let hours: i64 = parts[0].parse().ok()?;
            let minutes: i64 = parts[1].parse().ok()?;
            let seconds: f64 = parts[2].parse().ok()?;
            let total = hours * 3600 + minutes * 60;
            let nanos = (seconds.fract() * 1_000_000_000.0).round() as i64;
            return Some(
                Duration::seconds(total + seconds.trunc() as i64) + Duration::nanoseconds(nanos),
            );
        }
    }
    field.parse::<f64>().ok().map(Duration::seconds_f64)
}

fn parse_csv_datetime(field: &str) -> Option<PrimitiveDateTime> {
    static BASE_FORMAT: OnceLock<Vec<FormatItem<'static>>> = OnceLock::new();
    let format = BASE_FORMAT.get_or_init(|| {
        format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
            .expect("valid datetime format")
    });

    let (base, fraction) = match field.split_once('.') {
        Some((base, frac)) => (base, Some(frac)),
        None => (field, None),
    };

    let mut datetime = PrimitiveDateTime::parse(base, format).ok()?;

    if let Some(fraction) = fraction {
        let digits: String = fraction
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if !digits.is_empty() {
            let parsed = digits.parse::<u64>().ok()?;
            let len = digits.len() as u32;
            let nanos = if len >= 9 {
                (parsed / 10u64.pow(len - 9)) as u32
            } else {
                (parsed * 10u64.pow(9 - len)) as u32
            };
            datetime = datetime.replace_nanosecond(nanos).ok()?;
        }
    }

    Some(datetime)
}

fn parse_csv_date(field: &str) -> Option<PrimitiveDateTime> {
    static DATE_FORMAT: OnceLock<Vec<FormatItem<'static>>> = OnceLock::new();
    let format = DATE_FORMAT.get_or_init(|| {
        format_description::parse("[year]-[month]-[day]").expect("valid date format")
    });
    Date::parse(field, format)
        .ok()
        .map(|date| PrimitiveDateTime::new(date, Time::MIDNIGHT))
}
