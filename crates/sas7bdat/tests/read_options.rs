use sas7bdat::{CellValue, Error, RowSelection, SasReader};
use sas7bdat_test_support::common;

#[test]
fn rows_windowed_respects_skip_and_limit() {
    let mut sas = open_datetime_fixture();

    let column_count =
        usize::try_from(sas.metadata().column_count).expect("column count fits in usize");

    let options = RowSelection::new().skip_rows(1).max_rows(2);
    let mut rows = sas
        .rows_windowed(&options)
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
fn select_with_supports_name_projection() {
    let mut sas = open_datetime_fixture();

    let metadata = sas.metadata().clone();
    let column_indices = [0usize, 2usize];
    let column_names: Vec<String> = column_indices
        .iter()
        .map(|&idx| metadata.variables[idx].name.trim_end().to_string())
        .collect();

    let first_full_row: Vec<CellValue<'static>> = {
        let mut iter = sas.rows().expect("failed to build full iterator");
        iter.try_next()
            .expect("row iteration failed")
            .expect("expected at least one row")
            .into_iter()
            .map(CellValue::into_owned)
            .collect()
    };

    let options = RowSelection::new()
        .column_names(column_names.clone())
        .max_rows(1);
    let mut rows = sas
        .select_with(&options)
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
fn select_with_rejects_duplicate_names() {
    let mut sas = open_datetime_fixture();

    let options = RowSelection::new().column_names(["DATE1", "DATE1"]);
    let Err(err) = sas.select_with(&options) else {
        panic!("expected duplicate projection to fail");
    };
    match err {
        Error::InvalidMetadata { .. } => {}
        other => panic!("expected InvalidMetadata error, got {other:?}"),
    }
}

fn open_datetime_fixture() -> SasReader<std::fs::File> {
    let path = common::fixture_path("fixtures/raw_data/pandas/datetime.sas7bdat");
    SasReader::open(path).expect("failed to open datetime fixture")
}
