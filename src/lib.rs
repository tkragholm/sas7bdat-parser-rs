#![feature(portable_simd)]

mod columnar;
mod compression;
mod dataset;
mod encoding;
mod error;
#[doc(hidden)]
pub mod fixture_catalog;
mod internal;
mod layout;
mod metadata;
mod options;
mod pages;
mod probe;
mod projection;
mod row;
mod scan;

pub use columnar::{
    BytesBuffer, ColumnBuffer, ColumnarBatch, OwnedColumnBuffer, OwnedColumnarBatch,
    PrimitiveBuffer, Utf8Buffer, Utf8Dictionary,
};
pub use dataset::Dataset;
pub use error::{
    CompressionError, CorruptionError, DecodeError, Error, HeaderError, IoError, MetadataError,
    ProjectionError, Result, UnsupportedError,
};
pub use metadata::{
    ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType, SasDate, SasDateTime,
    SasTime, Timestamp,
};
pub use options::{
    BatchHint, DecodeMode, DictionaryStaging, IoBackendPreference, MojibakePolicy, OpenOptions,
    OrderingMode, PageCachePolicy, Parallelism, PrefetchPolicy, RowSelection, StringDecodeOptions,
    TemporalDecodeOptions, Utf8ValidationMode, ValidationMode,
};
pub use projection::{ProjectedColumnMeta, Projection, ProjectionBuilder};
pub use row::{CellValue, OwnedCellValue, OwnedRow, RawRow, RowView};
pub use scan::{BatchSink, RawRowSink, RowSink, ScanBuilder, ScanProgress, ScanStats};

#[allow(unused_imports)]
pub(crate) use internal::{
    FileInner, KernelSet, LayoutPlan, PageDescriptorTable, PageExecClass, PageSource,
    ProjectionPlan, SmallCommandBlock,
};
