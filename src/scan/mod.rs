use crate::{
    columnar::{ColumnBuffer, ColumnarBatch, OwnedColumnBuffer, OwnedColumnarBatch},
    compression::decompress_row,
    dataset::Dataset,
    encoding::resolve_encoding,
    error::{Error, Result},
    internal::{FileSource, PageDescriptor, ProjectedColumnPlan, RowSpan, RowSpanKind},
    metadata::{ColumnMeta, Endianness, LogicalType, SasDate, SasDateTime, SasTime},
    options::{
        BatchHint, DecodeMode, MojibakePolicy, OrderingMode, Parallelism, RowSelection,
        StringDecodeOptions, TemporalDecodeOptions, Utf8ValidationMode,
    },
    projection::Projection,
    row::{OwnedRow, RawRow, RowView},
};
use encoding_rs::{Encoding, UTF_8};
use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    ops::ControlFlow,
    simd::{Simd, cmp::SimdPartialEq, num::SimdUint},
    sync::Arc,
};

mod batch;
mod builder;
mod numeric;
mod plan;
mod raw;
mod row_decode;
mod string;

pub use builder::ScanBuilder;

use batch::*;
use numeric::*;
use plan::*;
use raw::*;
use row_decode::*;
use string::*;

#[derive(Debug, Clone, Default)]
pub struct ScanStats {
    pub rows_seen: u64,
    pub rows_emitted: u64,
    pub pages_seen: u64,
    pub fused_pages: u64,
    pub indexed_pages: u64,
    pub compressed_pages: u64,
    pub raw_bytes_read: u64,
    pub row_bytes_materialized: u64,
    pub decode_batches: u64,
}

pub trait RawRowSink {
    fn push(&mut self, row: RawRow<'_>) -> Result<ControlFlow<()>>;
}

pub trait RowSink {
    fn push(&mut self, row: RowView<'_>) -> Result<ControlFlow<()>>;
}

pub trait BatchSink {
    fn push(&mut self, batch: ColumnarBatch<'_>) -> Result<ControlFlow<()>>;
}

#[cfg(test)]
mod tests;
