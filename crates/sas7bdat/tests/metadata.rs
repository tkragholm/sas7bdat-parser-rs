use sas7bdat::{Error, decode_layout};
use std::io::Cursor;

#[test]
fn rejects_non_sas_streams() {
    let data = b"not a sas dataset";
    let mut cursor = Cursor::new(&data[..]);
    match decode_layout(&mut cursor).unwrap_err() {
        Error::Corrupted { .. } | Error::InvalidMetadata { .. } | Error::Io(_) => {
            // short buffer also acceptable at this stage
        }
        other => panic!("unexpected error: {other}"),
    }
}
