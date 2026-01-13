use sas7bdat::{CellValue, SasReader};
use sas7bdat_test_support::common;

#[test]
fn parse_test1_metadata_smoke() {
    let mut sas = open_test1();
    let metadata = sas.metadata();

    assert_eq!(metadata.column_count, 100);
    assert_eq!(metadata.row_count, 10);
    assert_eq!(metadata.variables.len(), 100);
    assert_eq!(metadata.variables[0].name, "Column1");

    let mut row_iter = sas.rows().expect("create row iterator");
    let row = row_iter.next().expect("row result").expect("row data");
    match &row[0] {
        CellValue::Float(v) => assert!((v - 0.636).abs() < 1e-6),
        other => panic!("unexpected value for Column1: {other:?}"),
    }
    match &row[1] {
        CellValue::Str(s) => assert_eq!(s.as_ref(), "pear"),
        other => panic!("unexpected value for Column2: {other:?}"),
    }
    assert_numeric_84(&row[2], "Column3");
}

#[test]
fn project_test1_subset() {
    let mut sas = open_test1();
    let mut projected = sas
        .select_columns(&[0, 2, 4])
        .expect("create projected iterator");
    let row = projected.next().expect("row result").expect("row data");
    assert_eq!(row.len(), 3);
    match &row[0] {
        CellValue::Float(v) => assert!((v - 0.636).abs() < 1e-6),
        other => panic!("unexpected value in projection for Column1: {other:?}"),
    }
    assert_numeric_84(&row[1], "Column3");
    match &row[2] {
        CellValue::Float(v) => assert!((v - 0.103).abs() < 1e-6),
        other => panic!("unexpected value in projection for Column5: {other:?}"),
    }
}

fn assert_numeric_84(value: &CellValue<'_>, label: &str) {
    match value {
        CellValue::Float(v) => assert!((v - 84.0).abs() < 1e-6),
        CellValue::Int64(v) => assert_eq!(*v, 84),
        CellValue::Int32(v) => assert_eq!(i64::from(*v), 84),
        other => panic!("unexpected value for {label}: {other:?}"),
    }
}

fn open_test1() -> SasReader<std::fs::File> {
    let path = common::fixture_path("fixtures/raw_data/pandas/test1.sas7bdat");
    SasReader::open(path).expect("sas open")
}
