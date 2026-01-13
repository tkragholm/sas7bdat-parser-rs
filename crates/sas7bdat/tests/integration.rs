use csv::ReaderBuilder;
use num_traits::ToPrimitive;
use sas7bdat::{CellValue, SasReader};
use sas7bdat_test_support::common;
use std::sync::OnceLock;
use time::{
    Date, Duration, PrimitiveDateTime, Time,
    format_description::{self, FormatItem},
};

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
        let sas_path = common::fixture_path("fixtures/raw_data/pandas").join(sas_file);
        let mut sas = SasReader::open(&sas_path)
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
            "column names mismatch for {sas_file}"
        );
        let row_count =
            usize::try_from(metadata.row_count).expect("metadata row count should fit in usize");
        assert_eq!(
            row_count,
            csv_fixture.rows.len(),
            "row count mismatch for {sas_file}"
        );

        let mut rows = sas
            .rows()
            .unwrap_or_else(|err| panic!("failed to create row iterator for {sas_file}: {err}"));
        for (index, expected_row) in csv_fixture.rows.iter().enumerate() {
            let actual_row = rows
                .try_next()
                .unwrap_or_else(|err| panic!("error reading row {index} for {sas_file}: {err}"))
                .unwrap_or_else(|| panic!("missing row {index} for {sas_file}"));
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
            .unwrap_or_else(|err| panic!("error reading trailing row for {sas_file}: {err}"))
        {
            panic!("found unexpected extra row {extra:?} in {sas_file}");
        }
    }
}

#[derive(Debug)]
struct CsvFixture {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn load_csv_fixture(path: &str) -> CsvFixture {
    let mut reader = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)
        .unwrap_or_else(|err| panic!("failed to open csv fixture {path}: {err}"));

    let headers = reader
        .headers()
        .unwrap_or_else(|err| panic!("failed to read headers in {path}: {err}"))
        .clone();
    let columns = headers
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let mut rows = Vec::new();
    for (idx, record) in reader.records().enumerate() {
        let record =
            record.unwrap_or_else(|err| panic!("failed reading row {idx} in {path}: {err}"));
        rows.push(
            record
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
        );
    }

    CsvFixture { columns, rows }
}

#[allow(clippy::too_many_lines)]
fn assert_value_matches_csv(
    value: &CellValue<'_>,
    expected: &str,
    column: &str,
    row_idx: usize,
    file: &str,
) {
    if expected.is_empty() {
        match value {
            CellValue::Missing(_) => return,
            CellValue::Str(s) | CellValue::NumericString(s) => {
                assert!(
                    s.is_empty(),
                    "expected empty string treated as missing for {column} in {file} row {row_idx} but got {value:?}"
                );
            }
            CellValue::Bytes(bytes) => {
                assert!(
                    bytes.is_empty(),
                    "expected empty bytes treated as missing for {column} in {file} row {row_idx} but got {value:?}"
                );
            }
            _ => panic!(
                "expected missing value for empty csv field {column} in {file} row {row_idx}, got {value:?}"
            ),
        }
        return;
    }

    match value {
        CellValue::Float(actual) => {
            let expected = expected.parse::<f64>().unwrap_or_else(|err| {
                panic!(
                    "csv float parse failed for {column} in {file} row {row_idx}: {err} (value {expected:?})"
                )
            });
            assert!(
                (actual - expected).abs() <= 1e-6,
                "float mismatch for {column} in {file} row {row_idx}: actual {actual}, expected {expected}"
            );
        }
        CellValue::Int32(actual) => {
            let expected = parse_expected_integer(expected, column, file, row_idx);
            let expected = i32::try_from(expected).unwrap_or_else(|_| {
                panic!(
                    "csv int parse failed for {column} in {file} row {row_idx}: value {expected:?} out of i32 range"
                )
            });
            assert_eq!(
                *actual, expected,
                "int32 mismatch for {column} in {file} row {row_idx}: actual {actual}, expected {expected}"
            );
        }
        CellValue::Int64(actual) => {
            let expected = parse_expected_integer(expected, column, file, row_idx);
            assert_eq!(
                *actual, expected,
                "int64 mismatch for {column} in {file} row {row_idx}: actual {actual}, expected {expected}"
            );
        }
        CellValue::NumericString(actual) | CellValue::Str(actual) => {
            assert_eq!(
                actual.as_ref(),
                expected,
                "string mismatch for {column} in {file} row {row_idx}: actual {actual:?}, expected {expected:?}"
            );
        }
        CellValue::Bytes(actual) => {
            let actual_text = String::from_utf8_lossy(actual);
            assert_eq!(
                actual_text, expected,
                "byte string mismatch for {column} in {file} row {row_idx}: actual {actual_text:?}, expected {expected:?}"
            );
        }
        CellValue::DateTime(actual) => {
            let expected_dt = parse_csv_datetime(expected).unwrap_or_else(|| {
                panic!(
                    "failed to parse csv datetime for {column} in {file} row {row_idx}: {expected:?}"
                )
            });
            let expected_str = common::format_iso_seconds(&expected_dt.assume_utc());
            let actual_str = common::format_iso_seconds(actual);
            assert_eq!(
                actual_str, expected_str,
                "datetime mismatch for {column} in {file} row {row_idx}: actual {actual_str}, expected {expected_str}"
            );
        }
        CellValue::Date(actual) => {
            let expected_date = parse_csv_date(expected).unwrap_or_else(|| {
                panic!(
                    "failed to parse csv date for {column} in {file} row {row_idx}: {expected:?}"
                )
            });
            let expected_str = common::format_iso_date(&expected_date.assume_utc());
            let actual_str = common::format_iso_date(actual);
            assert_eq!(
                actual_str, expected_str,
                "date mismatch for {column} in {file} row {row_idx}: actual {actual_str}, expected {expected_str}"
            );
        }
        CellValue::Time(actual) => {
            let expected_duration = parse_csv_time(expected).unwrap_or_else(|| {
                panic!(
                    "failed to parse csv time for {column} in {file} row {row_idx}: {expected:?}"
                )
            });
            assert!(
                (*actual - expected_duration).abs() <= Duration::microseconds(1),
                "time mismatch for {column} in {file} row {row_idx}: actual {actual:?}, expected {expected_duration:?}"
            );
        }
        CellValue::Missing(_) => panic!(
            "unexpected missing value for {column} in {file} row {row_idx} while csv had {expected:?}"
        ),
    }
}

fn parse_expected_integer(expected: &str, column: &str, file: &str, row_idx: usize) -> i64 {
    const INTEGER_TOLERANCE: f64 = 1e-3;
    if let Ok(value) = expected.parse::<i64>() {
        return value;
    }

    let float_value = expected.parse::<f64>().unwrap_or_else(|err| {
        panic!(
            "csv int parse failed for {column} in {file} row {row_idx}: {err} (value {expected:?})"
        )
    });

    let rounded = float_value.round();
    assert!(
        (float_value - rounded).abs() <= INTEGER_TOLERANCE,
        "csv int parse failed for {column} in {file} row {row_idx}: non-integer float {float_value} (value {expected:?})"
    );

    let min = i64::MIN.to_f64().unwrap();
    let max = i64::MAX.to_f64().unwrap();
    assert!(
        !(rounded < min || rounded > max),
        "csv int parse failed for {column} in {file} row {row_idx}: value {expected:?} out of i64 range"
    );

    rounded.to_i64().unwrap_or_else(|| {
        panic!(
            "csv int parse failed for {column} in {file} row {row_idx}: value {expected:?} out of i64 range"
        )
    })
}

fn parse_csv_time(field: &str) -> Option<Duration> {
    if field.contains(':') {
        let parts: Vec<_> = field.split(':').collect();
        if parts.len() == 3 {
            let hours: i64 = parts[0].parse().ok()?;
            let minutes: i64 = parts[1].parse().ok()?;
            let seconds: f64 = parts[2].parse().ok()?;
            let total = hours * 3600 + minutes * 60;
            let nanos = (seconds.fract() * 1_000_000_000.0).round().to_i64()?;
            let whole = seconds.trunc().to_i64()?;
            return Some(Duration::seconds(total + whole) + Duration::nanoseconds(nanos));
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
        let digits: String = fraction.chars().take_while(char::is_ascii_digit).collect();
        if !digits.is_empty() {
            let parsed = digits.parse::<u64>().ok()?;
            let len = u32::try_from(digits.len()).ok()?;
            let nanos = if len >= 9 {
                u32::try_from(parsed / 10u64.pow(len - 9)).ok()?
            } else {
                u32::try_from(parsed * 10u64.pow(9 - len)).ok()?
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
