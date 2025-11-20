#![allow(clippy::pedantic)]
use std::io::Cursor;

use sas7bdat_parser_rs::{Error, parse_layout};

#[test]
fn rejects_non_sas_streams() {
    let data = b"not a sas dataset";
    let mut cursor = Cursor::new(&data[..]);
    match parse_layout(&mut cursor).unwrap_err() {
        Error::Corrupted { .. } | Error::InvalidMetadata { .. } => {}
        Error::Io(_) => {} // short buffer also acceptable at this stage
        other => panic!("unexpected error: {other}"),
    }
}
