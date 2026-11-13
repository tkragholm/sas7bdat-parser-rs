//! `OpenOptions::encoding` replaces the encoding a file declares. The tracked
//! `people_nonascii.sas7bdat` declares windows-1252 and holds the byte 0xE9 in its first
//! `GENDER` cell: `é` there, `ι` in ISO-8859-7.

use sas7bdat::{Dataset, OpenOptions, OwnedCellValue};
use std::path::PathBuf;

fn people() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/people_nonascii.sas7bdat")
}

fn first_gender(options: OpenOptions) -> String {
    let ds = Dataset::open_with(people(), options).expect("open");
    let gender = ds
        .columns()
        .iter()
        .position(|c| c.name == "GENDER")
        .expect("GENDER");
    let rows = ds.scan().limit(1).collect_rows().expect("rows");
    match &rows[0].cells[gender] {
        OwnedCellValue::String(text) => text.clone(),
        other => panic!("GENDER is not text: {other:?}"),
    }
}

#[test]
fn the_declared_encoding_is_used_by_default() {
    assert_eq!(first_gender(OpenOptions::default()), "é");
}

#[test]
fn an_override_decodes_the_same_bytes_differently() {
    let options = OpenOptions::builder()
        .encoding("iso-8859-7")
        .expect("label")
        .build();
    assert_eq!(first_gender(options), "ι");
    let ds = Dataset::open_with(people(), options).expect("open");
    assert_eq!(ds.metadata().encoding.as_deref(), Some("ISO-8859-7"));
}

#[test]
fn sas_names_for_windows_code_pages_are_accepted() {
    let options = OpenOptions::builder()
        .encoding("wlatin1")
        .expect("label")
        .build();
    assert_eq!(first_gender(options), "é");
}

#[test]
fn an_unknown_label_is_an_error() {
    assert!(OpenOptions::builder().encoding("not-an-encoding").is_err());
}
