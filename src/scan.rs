use crate::{
    columnar::{ColumnBuffer, ColumnarBatch, OwnedColumnarBatch},
    dataset::Dataset,
    error::{Error, Result},
    options::{
        BatchHint, DecodeMode, OrderingMode, Parallelism, RowSelection, StringDecodeOptions,
        TemporalDecodeOptions,
    },
    projection::Projection,
    row::{OwnedRow, RawRow, RowView},
};
use std::ops::ControlFlow;

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

#[derive(Debug, Clone)]
pub struct ScanBuilder<'a> {
    #[allow(dead_code)]
    pub(crate) ds: &'a Dataset,
    pub(crate) projection: Option<&'a Projection>,
    pub(crate) decode: DecodeMode,
    pub(crate) string_options: StringDecodeOptions,
    pub(crate) temporal_options: TemporalDecodeOptions,
    pub(crate) ordering: OrderingMode,
    pub(crate) parallelism: Parallelism,
    pub(crate) batch_hint: BatchHint,
    pub(crate) row_limit: Option<u64>,
    pub(crate) row_selection: RowSelection,
}

impl<'a> ScanBuilder<'a> {
    pub(crate) fn new(ds: &'a Dataset) -> Self {
        Self {
            ds,
            projection: None,
            decode: DecodeMode::Typed,
            string_options: StringDecodeOptions::default(),
            temporal_options: TemporalDecodeOptions::default(),
            ordering: OrderingMode::Stable,
            parallelism: Parallelism::Auto,
            batch_hint: BatchHint::Auto,
            row_limit: None,
            row_selection: RowSelection::All,
        }
    }

    #[must_use]
    pub fn with_projection(mut self, projection: &'a Projection) -> Self {
        self.projection = Some(projection);
        self
    }

    #[must_use]
    pub fn with_decode_mode(mut self, mode: DecodeMode) -> Self {
        self.decode = mode;
        self
    }

    #[must_use]
    pub fn with_string_options(mut self, options: StringDecodeOptions) -> Self {
        self.string_options = options;
        self
    }

    #[must_use]
    pub fn with_temporal_options(mut self, options: TemporalDecodeOptions) -> Self {
        self.temporal_options = options;
        self
    }

    #[must_use]
    pub fn with_ordering(mut self, mode: OrderingMode) -> Self {
        self.ordering = mode;
        self
    }

    #[must_use]
    pub fn with_parallelism(mut self, parallelism: Parallelism) -> Self {
        self.parallelism = parallelism;
        self
    }

    #[must_use]
    pub fn with_batch_hint(mut self, hint: BatchHint) -> Self {
        self.batch_hint = hint;
        self
    }

    #[must_use]
    pub fn limit(mut self, rows: u64) -> Self {
        self.row_limit = Some(rows);
        self
    }

    #[must_use]
    pub fn select(mut self, selection: RowSelection) -> Self {
        self.row_selection = selection;
        self
    }

    pub fn visit_raw_rows<F>(self, _f: F) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
    {
        Err(Error::unsupported(
            "raw row scanning is not implemented yet",
        ))
    }

    pub fn visit_rows<F>(self, _f: F) -> Result<ScanStats>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
    {
        Err(Error::unsupported("row scanning is not implemented yet"))
    }

    pub fn visit_batches<F>(self, _f: F) -> Result<ScanStats>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
    {
        Err(Error::unsupported(
            "columnar scanning is not implemented yet",
        ))
    }

    pub fn collect_rows(self) -> Result<Vec<OwnedRow>> {
        Err(Error::unsupported("row collection is not implemented yet"))
    }

    pub fn collect_batches(self) -> Result<Vec<OwnedColumnarBatch>> {
        Err(Error::unsupported(
            "batch collection is not implemented yet",
        ))
    }

    pub fn write_raw_rows(self, _sink: &mut impl RawRowSink) -> Result<ScanStats> {
        Err(Error::unsupported(
            "raw row sink writing is not implemented yet",
        ))
    }

    pub fn write_rows(self, _sink: &mut impl RowSink) -> Result<ScanStats> {
        Err(Error::unsupported(
            "row sink writing is not implemented yet",
        ))
    }

    pub fn write_batches(self, _sink: &mut impl BatchSink) -> Result<ScanStats> {
        Err(Error::unsupported(
            "batch sink writing is not implemented yet",
        ))
    }
}

#[allow(dead_code)]
fn _keep_type_imports_alive<'a>(_columns: &'a [ColumnBuffer<'a>], _dataset: &'a Dataset) {}
