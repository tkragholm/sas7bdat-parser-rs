use csv::ByteRecord;
use itoa::Buffer as ItoaBuffer;
use ryu::Buffer as RyuBuffer;

use crate::cell::CellValue;
use crate::error::Result;

use super::time_format::{write_date, write_datetime, write_time};

pub fn encode_value(
    value: &CellValue<'_>,
    out: &mut Vec<u8>,
    ryu: &mut RyuBuffer,
    itoa: &mut ItoaBuffer,
) -> Result<()> {
    out.clear();
    match value {
        CellValue::Missing(_) => {}
        CellValue::Float(v) => {
            let s = ryu.format(*v);
            out.extend_from_slice(s.as_bytes());
        }
        CellValue::Int32(v) => {
            let s = itoa.format(*v);
            out.extend_from_slice(s.as_bytes());
        }
        CellValue::Int64(v) => {
            let s = itoa.format(*v);
            out.extend_from_slice(s.as_bytes());
        }
        CellValue::NumericString(s) | CellValue::Str(s) => {
            out.extend_from_slice(s.as_bytes());
        }
        CellValue::Bytes(bytes) => out.extend_from_slice(bytes),
        CellValue::DateTime(dt) => write_datetime(dt, out),
        CellValue::Date(dt) => write_date(dt, out),
        CellValue::Time(dur) => write_time(dur, out)?,
    }
    Ok(())
}

pub fn flush_record<W: std::io::Write>(
    writer: &mut csv::Writer<W>,
    record: &ByteRecord,
) -> Result<()> {
    writer
        .write_byte_record(record)
        .map_err(|e| crate::error::Error::InvalidMetadata {
            details: std::borrow::Cow::Owned(format!("csv write failed: {e}")),
        })
}
