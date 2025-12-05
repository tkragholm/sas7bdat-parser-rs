#![allow(clippy::pedantic)]
use std::path::Path;

use sas7bdat::value::Value;
use sas7bdat::{Error, ReadOptions, SasFile};

#[test]
fn rows_with_options_respects_skip_and_limit() {
    let path = Path::new("fixtures/raw_data/pandas/datetime.sas7bdat");
    let mut sas = SasFile::open(path).expect("failed to open datetime fixture");

    let column_count = sas.metadata().column_count as usize;

    let options = ReadOptions::new().with_skip_rows(1).with_max_rows(2);
    let mut rows = sas
        .rows_with_options(&options)
        .expect("failed to build windowed iterator");

    let mut seen = 0usize;
    while let Some(row) = rows.try_next().expect("row iteration failed") {
        assert_eq!(row.len(), column_count, "row should contain every column");
        seen += 1;
    }

    assert_eq!(seen, 2);
    assert!(
        rows.try_next().expect("final advance failed").is_none(),
        "iterator should end after returning the maximum rows"
    );
}

#[test]
fn project_rows_with_options_supports_name_projection() {
    let path = Path::new("fixtures/raw_data/pandas/datetime.sas7bdat");
    let mut sas = SasFile::open(path).expect("failed to open datetime fixture");

    let metadata = sas.metadata().clone();
    let column_indices = [0usize, 2usize];
    let column_names: Vec<String> = column_indices
        .iter()
        .map(|&idx| metadata.variables[idx].name.trim_end().to_string())
        .collect();

    let first_full_row: Vec<Value<'static>> = {
        let mut iter = sas.rows().expect("failed to build full iterator");
        iter.try_next()
            .expect("row iteration failed")
            .expect("expected at least one row")
            .into_iter()
            .map(Value::into_owned)
            .collect()
    };

    let options = ReadOptions::new()
        .with_column_names(column_names.clone())
        .with_max_rows(1);
    let mut rows = sas
        .project_rows_with_options(&options)
        .expect("failed to build projected iterator");

    let first = rows
        .try_next()
        .expect("row iteration failed")
        .expect("expected first row");
    assert_eq!(first.len(), column_names.len());
    for (value, (&index, name)) in first
        .iter()
        .zip(column_indices.iter().zip(column_names.iter()))
    {
        assert_eq!(
            value, &first_full_row[index],
            "projected value for column '{name}' did not match reference row"
        );
    }
    assert!(
        rows.try_next().expect("final advance failed").is_none(),
        "iterator should respect max_rows limit"
    );
}

#[test]
fn project_rows_with_options_rejects_duplicate_names() {
    let path = Path::new("fixtures/raw_data/pandas/datetime.sas7bdat");
    let mut sas = SasFile::open(path).expect("failed to open datetime fixture");

    let options = ReadOptions::new().with_column_names(["DATE1", "DATE1"]);
    let err = match sas.project_rows_with_options(&options) {
        Ok(_) => panic!("expected duplicate projection to fail"),
        Err(err) => err,
    };
    match err {
        Error::InvalidMetadata { .. } => {}
        other => panic!("expected InvalidMetadata error, got {other:?}"),
    }
}
