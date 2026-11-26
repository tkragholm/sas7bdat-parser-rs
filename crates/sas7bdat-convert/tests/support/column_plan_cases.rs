//! End-to-end cases for the column plan, shared by the parallel and the serial test
//! binaries. Each binary is its own process, so each sizes the Parquet encode pool
//! once, from the threads it passes, and so exercises one of the two write paths.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, DurationNanosecondType, Time32MillisecondType, TimestampMicrosecondType,
    TimestampMillisecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sas7bdat_convert::columns::{ColumnPlan, ConstantValue, NameCase, StringType, TimeType};
use sas7bdat_convert::{ConvertOptions, NoObserver, OutputLayout, SinkKind, convert_file};
use std::fs::File;
use std::path::{Path, PathBuf};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sas7bdat/tests/fixtures/corpus")
        .join(name)
}

fn convert(
    input: &Path,
    output: &Path,
    plan: ColumnPlan,
    sink: SinkKind,
    threads: Option<usize>,
) -> anyhow::Result<()> {
    let options = ConvertOptions {
        layout: OutputLayout {
            sink,
            ..OutputLayout::default()
        },
        overwrite: true,
        parse_threads: threads,
        column_plan: plan,
        ..ConvertOptions::default()
    };
    convert_file(
        input.parent().unwrap(),
        input,
        Some(output),
        &options,
        None,
        &NoObserver,
    )
    .map(|_| ())
}

fn read(path: &Path) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    arrow_select_concat(&batches)
}

fn arrow_select_concat(batches: &[RecordBatch]) -> RecordBatch {
    assert_eq!(batches.len(), 1, "fixtures are small enough for one batch");
    batches[0].clone()
}

pub fn renames_retypes_and_adds_a_source_column(threads: Option<usize>) {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture("pandas/datetime.sas7bdat");
    let plain = dir.path().join("plain.parquet");
    let shaped = dir.path().join("shaped.parquet");
    convert(
        &input,
        &plain,
        ColumnPlan::default(),
        SinkKind::Parquet,
        threads,
    )
    .unwrap();
    let plan = ColumnPlan {
        case: NameCase::Lower,
        rename: vec![("datetime".into(), "Stamp".into())],
        constants: vec![("source_file".into(), ConstantValue::InputPath)],
        strings: StringType::LargeUtf8,
        timestamps: Some(TimeUnit::Millisecond),
        times: TimeType::default(),
    };
    convert(&input, &shaped, plan, SinkKind::Parquet, threads).unwrap();

    let plain = read(&plain);
    let shaped = read(&shaped);
    let schema = shaped.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "date1",
            "date2",
            "Stamp",
            "datetimehi",
            "taiw",
            "source_file"
        ]
    );
    assert_eq!(shaped.num_rows(), plain.num_rows());

    // Dates pass through untouched.
    assert_eq!(
        shaped.column(0).as_primitive::<Date32Type>(),
        plain.column(0).as_primitive::<Date32Type>()
    );
    // Timestamps are the same instants in milliseconds, floored.
    assert_eq!(
        *shaped.schema().field(2).data_type(),
        DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    let micros = plain.column(2).as_primitive::<TimestampMicrosecondType>();
    let millis = shaped.column(2).as_primitive::<TimestampMillisecondType>();
    for row in 0..micros.len() {
        assert_eq!(millis.is_null(row), micros.is_null(row));
        if micros.is_valid(row) {
            assert_eq!(millis.value(row), micros.value(row).div_euclid(1_000));
        }
    }
    // The constant column holds the input path as given, as a large string.
    assert_eq!(*shaped.schema().field(5).data_type(), DataType::LargeUtf8);
    let source = shaped.column(5).as_string::<i64>();
    assert!((0..source.len()).all(|row| source.value(row) == input.to_string_lossy()));
}

pub fn times_of_day(threads: Option<usize>) {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture("parso/time_formats.sas7bdat");
    let plain = dir.path().join("plain.parquet");
    let shaped = dir.path().join("shaped.parquet");
    convert(
        &input,
        &plain,
        ColumnPlan {
            times: TimeType::Duration(TimeUnit::Nanosecond),
            ..ColumnPlan::default()
        },
        SinkKind::Parquet,
        threads,
    )
    .unwrap();
    let plan = ColumnPlan {
        times: TimeType::TimeOfDay(TimeUnit::Millisecond),
        ..ColumnPlan::default()
    };
    convert(&input, &shaped, plan, SinkKind::Parquet, threads).unwrap();
    let default = dir.path().join("default.parquet");
    convert(
        &input,
        &default,
        ColumnPlan::default(),
        SinkKind::Parquet,
        threads,
    )
    .unwrap();
    let plain = read(&plain);
    let shaped = read(&shaped);
    let default = read(&default);
    for column in 0..plain.num_columns() {
        assert_eq!(
            *plain.schema().field(column).data_type(),
            DataType::Duration(TimeUnit::Nanosecond),
            "durations are the opt-in"
        );
        assert_eq!(
            *default.schema().field(column).data_type(),
            DataType::Time64(TimeUnit::Microsecond),
            "times of day in microseconds are the default"
        );
        assert_eq!(
            *shaped.schema().field(column).data_type(),
            DataType::Time32(TimeUnit::Millisecond)
        );
        let nanos = plain
            .column(column)
            .as_primitive::<DurationNanosecondType>();
        let millis = shaped
            .column(column)
            .as_primitive::<Time32MillisecondType>();
        for row in 0..nanos.len() {
            assert_eq!(millis.is_null(row), nanos.is_null(row));
            if nanos.is_valid(row) {
                assert_eq!(
                    i64::from(millis.value(row)),
                    nanos.value(row).div_euclid(1_000_000)
                );
            }
        }
    }
}

pub fn csv_takes_names_and_constants(threads: Option<usize>) {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture("pandas/datetime.sas7bdat");
    let output = dir.path().join("out.csv");
    let plan = ColumnPlan {
        case: NameCase::Lower,
        constants: vec![("source_file".into(), ConstantValue::InputPath)],
        ..ColumnPlan::default()
    };
    convert(&input, &output, plan, SinkKind::Csv, threads).unwrap();
    let text = std::fs::read_to_string(&output).unwrap();
    let mut lines = text.lines();
    assert_eq!(
        lines.next(),
        Some("date1,date2,datetime,datetimehi,taiw,source_file")
    );
    let suffix = format!(",{}", input.to_string_lossy());
    let rows: Vec<&str> = lines.collect();
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| row.ends_with(&suffix)), "{rows:?}");
}

pub fn bad_plans_fail_before_any_output(threads: Option<usize>) {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture("pandas/datetime.sas7bdat");
    let output = dir.path().join("out.parquet");
    let unknown = ColumnPlan {
        rename: vec![("nope".into(), "x".into())],
        ..ColumnPlan::default()
    };
    let err = convert(&input, &output, unknown, SinkKind::Parquet, threads).unwrap_err();
    assert!(err.to_string().contains("`nope`"), "{err}");
    let typed = ColumnPlan {
        strings: StringType::LargeUtf8,
        ..ColumnPlan::default()
    };
    let csv = dir.path().join("out.csv");
    let err = convert(&input, &csv, typed, SinkKind::Csv, threads).unwrap_err();
    assert!(err.to_string().contains("only apply to Parquet"), "{err}");
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert!(
        entries.is_empty(),
        "no output or staging file is left behind"
    );
}
