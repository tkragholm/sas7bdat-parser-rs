#![allow(clippy::pedantic)]
use sas7bdat::CellValue;
use sas7bdat::SasReader;

#[test]
fn parse_test1_metadata_smoke() {
    let mut sas = SasReader::open("fixtures/raw_data/pandas/test1.sas7bdat").expect("sas open");
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
    match &row[2] {
        CellValue::Float(v) => assert_eq!(*v, 84.0),
        CellValue::Int64(v) => assert_eq!(*v, 84),
        CellValue::Int32(v) => assert_eq!(*v as i64, 84),
        other => panic!("unexpected value for Column3: {other:?}"),
    }
}

#[test]
fn project_test1_subset() {
    let mut sas = SasReader::open("fixtures/raw_data/pandas/test1.sas7bdat").expect("sas open");
    let mut projected = sas
        .select_columns(&[0, 2, 4])
        .expect("create projected iterator");
    let row = projected.next().expect("row result").expect("row data");
    assert_eq!(row.len(), 3);
    match &row[0] {
        CellValue::Float(v) => assert!((*v - 0.636).abs() < 1e-6),
        other => panic!("unexpected value in projection for Column1: {other:?}"),
    }
    match &row[1] {
        CellValue::Float(v) => assert_eq!(*v, 84.0),
        CellValue::Int64(v) => assert_eq!(*v, 84),
        CellValue::Int32(v) => assert_eq!(*v as i64, 84),
        other => panic!("unexpected value in projection for Column3: {other:?}"),
    }
    match &row[2] {
        CellValue::Float(v) => assert!((*v - 0.103).abs() < 1e-6),
        other => panic!("unexpected value in projection for Column5: {other:?}"),
    }
}
