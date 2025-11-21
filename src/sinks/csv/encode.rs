use csv::ByteRecord;
use itoa::Buffer as ItoaBuffer;
use ryu::Buffer as RyuBuffer;

use crate::error::Result;
use crate::value::Value;

use super::time_format::{write_date, write_datetime, write_time};

pub fn encode_value(
    value: &Value<'_>,
    out: &mut Vec<u8>,
    ryu: &mut RyuBuffer,
    itoa: &mut ItoaBuffer,
) -> Result<()> {
    out.clear();
    match value {
        Value::Missing(_) => {}
        Value::Float(v) => {
            let s = ryu.format(*v);
            out.extend_from_slice(s.as_bytes());
        }
        Value::Int32(v) => {
            let s = itoa.format(*v);
            out.extend_from_slice(s.as_bytes());
        }
        Value::Int64(v) => {
            let s = itoa.format(*v);
            out.extend_from_slice(s.as_bytes());
        }
        Value::NumericString(s) | Value::Str(s) => {
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(bytes) => out.extend_from_slice(bytes),
        Value::DateTime(dt) => write_datetime(dt, out),
        Value::Date(dt) => write_date(dt, out),
        Value::Time(dur) => write_time(dur, out)?,
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
