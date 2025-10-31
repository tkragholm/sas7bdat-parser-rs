use sas7bdat_parser_rs::api::SasFile;
use sas7bdat_parser_rs::value::Value;

#[test]
fn parse_test1_metadata_smoke() {
    let mut sas = SasFile::open("fixtures/raw_data/pandas/test1.sas7bdat").expect("sas open");
    let metadata = sas.metadata();

    assert_eq!(metadata.column_count, 100);
    assert_eq!(metadata.row_count, 10);
    assert_eq!(metadata.variables.len(), 100);
    assert_eq!(metadata.variables[0].name, "Column1");

    let mut row_iter = sas.rows().expect("create row iterator");
    let row = row_iter.next().expect("row result").expect("row data");
    match &row[0] {
        Value::Float(v) => assert!((v - 0.636).abs() < 1e-6),
        other => panic!("unexpected value for Column1: {other:?}"),
    }
    match &row[1] {
        Value::Str(s) => assert_eq!(s.as_ref(), "pear"),
        other => panic!("unexpected value for Column2: {other:?}"),
    }
    match &row[2] {
        Value::Float(v) => assert_eq!(*v, 84.0),
        Value::Int64(v) => assert_eq!(*v, 84),
        Value::Int32(v) => assert_eq!(*v as i64, 84),
        other => panic!("unexpected value for Column3: {other:?}"),
    }
}

#[test]
fn project_test1_subset() {
    let mut sas = SasFile::open("fixtures/raw_data/pandas/test1.sas7bdat").expect("sas open");
    let mut projected = sas
        .project_rows(&[0, 2, 4])
        .expect("create projected iterator");
    let row = projected.next().expect("row result").expect("row data");
    assert_eq!(row.len(), 3);
    match &row[0] {
        Value::Float(v) => assert!((*v - 0.636).abs() < 1e-6),
        other => panic!("unexpected value in projection for Column1: {other:?}"),
    }
    match &row[1] {
        Value::Float(v) => assert_eq!(*v, 84.0),
        Value::Int64(v) => assert_eq!(*v, 84),
        Value::Int32(v) => assert_eq!(*v as i64, 84),
        other => panic!("unexpected value in projection for Column3: {other:?}"),
    }
    match &row[2] {
        Value::Float(v) => assert!((*v - 0.103).abs() < 1e-6),
        other => panic!("unexpected value in projection for Column5: {other:?}"),
    }
}
