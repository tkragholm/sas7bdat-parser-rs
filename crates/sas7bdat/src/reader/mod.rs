mod frame;
mod labels;
mod missing;
mod projection;
mod query;
mod row;
mod selection;
mod window;

use crate::{
    dataset::{DatasetMetadata, MissingValuePolicy},
    error::{Error, Result},
    parser::{
        DatasetLayout, DecodePolicy, MetadataReadOptions, ParallelScanConfig, RawRowBatch,
        RawScanStats, RowIterator, parse_catalog, parse_metadata, parse_metadata_with_options,
        scan_file_projected_rows_with_decode_policy,
        scan_file_projected_rows_with_decode_policy_unordered, scan_file_raw_rows,
        scan_file_raw_rows_unordered_batched_with_stats, scan_file_raw_rows_unordered_with_stats,
        scan_file_rows_with_decode_policy, scan_file_rows_with_decode_policy_unordered,
    },
    sinks::{RowSink, SinkContext},
};
use labels::{build_label_lookup, normalize_label_name};
use missing::{dedup_missing_ranges, dedup_tagged_missing, merge_label_set_missing};
use row::RowProjection;
use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct SasReader<R: Read + Seek> {
    reader: R,
    layout: DatasetLayout,
    missing_policies_stale: bool,
    source_path: Option<PathBuf>,
}

pub use frame::{
    BinaryCol, FrameBatch, FrameColumn, FrameColumnType, FrameSchema, FrameSchemaField,
    MissingSummary, PrimitiveCol, Utf8Col,
};
pub use projection::ProjectedRowIter;
pub use query::{OrderingMode, Query, QueryPlan, QueryStream, Shape, SourceKind};
pub use row::{Row, RowIter, RowLookup, RowValue, RowView, RowViewIter};
pub use selection::{RowSelection, resolve_column_name_projection};
pub use window::{ProjectedRowWindow, RowWindow};

/// Controls when missing-value policies are refreshed after attaching a catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CatalogScanPolicy {
    /// Scan immediately after attaching the catalog.
    #[default]
    Eager,
    /// Defer scanning until a row-consuming API is called.
    Deferred,
}

#[allow(deprecated)]
impl SasReader<File> {
    /// Opens a SAS7BDAT file from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or if the metadata
    /// cannot be parsed.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = File::open(&path_buf)?;
        let mut reader = Self::from_reader(file)?;
        reader.source_path = Some(path_buf);
        Ok(reader)
    }

    /// Opens a SAS7BDAT file from disk with custom metadata read options.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or if the metadata
    /// cannot be parsed.
    pub fn open_with_options<P: AsRef<Path>>(
        path: P,
        options: MetadataReadOptions,
    ) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = File::open(&path_buf)?;
        let mut reader = Self::from_reader_with_options(file, options)?;
        reader.source_path = Some(path_buf);
        Ok(reader)
    }

    fn parallel_scan_config(parse_threads: usize) -> ParallelScanConfig {
        let mut config = ParallelScanConfig::new(parse_threads);
        if let Ok(raw) = std::env::var("SAS7BDAT_PARSE_PAGE_CHUNK")
            && let Ok(value) = raw.parse::<u64>()
            && value > 0
        {
            config.page_chunk = value;
        }
        if let Ok(raw) = std::env::var("SAS7BDAT_PARSE_ROW_BATCH_SIZE")
            && let Ok(value) = raw.parse::<usize>()
            && value > 0
        {
            config.row_batch_size = value;
        }
        config
    }

    /// Scans all rows in parallel (unordered by default for throughput).
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).parallel(...).ordering(...).decode(...).scan_*()`."
    )]
    pub fn scan_rows_parallel_with_decode_policy<F>(
        &mut self,
        parse_threads: usize,
        policy: DecodePolicy,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.scan_rows_parallel_unordered_with_decode_policy(parse_threads, policy, f)
    }

    /// Scans all rows in parallel (unordered by default) using default decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).parallel(...).ordering(OrderingMode::Unordered).scan_unordered(...)`."
    )]
    pub fn scan_rows_parallel<F>(&mut self, parse_threads: usize, f: F) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.scan_rows_parallel_with_decode_policy(parse_threads, DecodePolicy::default(), f)
    }

    /// Scans all rows in parallel without preserving row order.
    ///
    /// This path runs the callback on parser worker threads and avoids ordered
    /// chunk assembly overhead, typically improving high-thread throughput.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).parallel(...).ordering(OrderingMode::Unordered).decode(...).scan_unordered(...)`."
    )]
    pub fn scan_rows_parallel_unordered_with_decode_policy<F>(
        &mut self,
        parse_threads: usize,
        policy: DecodePolicy,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.ensure_missing_policies_fresh()?;
        if parse_threads <= 1 {
            self.reader.seek(SeekFrom::Start(0))?;
            let mut iterator = self.layout.row_iterator(&mut self.reader)?;
            iterator.set_decode_policy(policy);
            let result = (|| -> Result<u64> {
                let mut rows = 0u64;
                while let Some(row) = iterator.try_next()? {
                    f(&row)?;
                    rows = rows.saturating_add(1);
                }
                Ok(rows)
            })();
            self.reader.seek(SeekFrom::Start(0))?;
            return result;
        }

        let Some(path) = self.source_path.as_deref() else {
            return self.scan_rows_parallel_unordered_with_decode_policy(1, policy, f);
        };

        scan_file_rows_with_decode_policy_unordered(
            path,
            &self.layout,
            policy,
            Self::parallel_scan_config(parse_threads),
            f,
        )
    }

    /// Scans all rows in unordered parallel mode using default decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).parallel(...).ordering(OrderingMode::Unordered).scan_unordered(...)`."
    )]
    pub fn scan_rows_parallel_unordered<F>(&mut self, parse_threads: usize, f: F) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.scan_rows_parallel_unordered_with_decode_policy(
            parse_threads,
            DecodePolicy::default(),
            f,
        )
    }

    /// Scans all rows in parallel while preserving row order.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).parallel(...).ordering(OrderingMode::Ordered).decode(...).scan_ordered(...)`."
    )]
    pub fn scan_rows_parallel_ordered_with_decode_policy<F>(
        &mut self,
        parse_threads: usize,
        policy: DecodePolicy,
        mut f: F,
    ) -> Result<u64>
    where
        F: FnMut(&[crate::cell::CellValue<'static>]) -> Result<()>,
    {
        self.ensure_missing_policies_fresh()?;
        if parse_threads <= 1 {
            self.reader.seek(SeekFrom::Start(0))?;
            let mut iterator = self.layout.row_iterator(&mut self.reader)?;
            iterator.set_decode_policy(policy);
            let result = (|| -> Result<u64> {
                let mut rows = 0u64;
                while let Some(row) = iterator.try_next_owned()? {
                    f(&row)?;
                    rows = rows.saturating_add(1);
                }
                Ok(rows)
            })();
            self.reader.seek(SeekFrom::Start(0))?;
            return result;
        }

        let Some(path) = self.source_path.as_deref() else {
            return self.scan_rows_parallel_ordered_with_decode_policy(1, policy, f);
        };

        scan_file_rows_with_decode_policy(
            path,
            &self.layout,
            policy,
            Self::parallel_scan_config(parse_threads),
            f,
        )
    }

    /// Scans all rows in ordered parallel mode using default decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).parallel(...).ordering(OrderingMode::Ordered).scan_ordered(...)`."
    )]
    pub fn scan_rows_parallel_ordered<F>(&mut self, parse_threads: usize, f: F) -> Result<u64>
    where
        F: FnMut(&[crate::cell::CellValue<'static>]) -> Result<()>,
    {
        self.scan_rows_parallel_ordered_with_decode_policy(
            parse_threads,
            DecodePolicy::default(),
            f,
        )
    }

    /// Scans projected columns in parallel (unordered by default for throughput).
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row decoding fails, or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).parallel(...).ordering(...).decode(...).scan_*()`."
    )]
    pub fn scan_projected_columns_parallel_with_decode_policy<F>(
        &mut self,
        indices: &[usize],
        parse_threads: usize,
        policy: DecodePolicy,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.scan_projected_columns_parallel_unordered_with_decode_policy(
            indices,
            parse_threads,
            policy,
            f,
        )
    }

    /// Scans projected columns in parallel (unordered by default) with default decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row decoding fails, or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).parallel(...).ordering(OrderingMode::Unordered).scan_unordered(...)`."
    )]
    pub fn scan_projected_columns_parallel<F>(
        &mut self,
        indices: &[usize],
        parse_threads: usize,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.scan_projected_columns_parallel_with_decode_policy(
            indices,
            parse_threads,
            DecodePolicy::default(),
            f,
        )
    }

    /// Scans projected columns in parallel without preserving row order.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row decoding fails, or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).parallel(...).ordering(OrderingMode::Unordered).decode(...).scan_unordered(...)`."
    )]
    pub fn scan_projected_columns_parallel_unordered_with_decode_policy<F>(
        &mut self,
        indices: &[usize],
        parse_threads: usize,
        policy: DecodePolicy,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.ensure_missing_policies_fresh()?;
        let normalized = self.normalize_projection(indices)?;
        if parse_threads <= 1 {
            self.reader.seek(SeekFrom::Start(0))?;
            let mut iterator = self.layout.row_iterator(&mut self.reader)?;
            iterator.set_decode_policy(policy);
            let result = (|| -> Result<u64> {
                let mut rows = 0u64;
                while let Some(row) = iterator.try_next_projected(&normalized)? {
                    f(&row)?;
                    rows = rows.saturating_add(1);
                }
                Ok(rows)
            })();
            self.reader.seek(SeekFrom::Start(0))?;
            return result;
        }

        let Some(path) = self.source_path.as_deref() else {
            return self.scan_projected_columns_parallel_unordered_with_decode_policy(
                &normalized,
                1,
                policy,
                f,
            );
        };

        let projection_legacy = std::env::var("SAS7BDAT_PROJECTION_DECODE_PLAN")
            .map(|value| !value.eq_ignore_ascii_case("compiled"))
            .unwrap_or(false);
        scan_file_projected_rows_with_decode_policy_unordered(
            path,
            &self.layout,
            &normalized,
            policy,
            Self::parallel_scan_config(parse_threads),
            projection_legacy,
            f,
        )
    }

    /// Scans projected columns in unordered parallel mode with default decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row decoding fails, or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).parallel(...).ordering(OrderingMode::Unordered).scan_unordered(...)`."
    )]
    pub fn scan_projected_columns_parallel_unordered<F>(
        &mut self,
        indices: &[usize],
        parse_threads: usize,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.scan_projected_columns_parallel_unordered_with_decode_policy(
            indices,
            parse_threads,
            DecodePolicy::default(),
            f,
        )
    }

    /// Scans projected columns in parallel while preserving row order.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row decoding fails, or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).parallel(...).ordering(OrderingMode::Ordered).decode(...).scan_ordered(...)`."
    )]
    pub fn scan_projected_columns_parallel_ordered_with_decode_policy<F>(
        &mut self,
        indices: &[usize],
        parse_threads: usize,
        policy: DecodePolicy,
        mut f: F,
    ) -> Result<u64>
    where
        F: FnMut(&[crate::cell::CellValue<'static>]) -> Result<()>,
    {
        self.ensure_missing_policies_fresh()?;
        let normalized = self.normalize_projection(indices)?;
        if parse_threads <= 1 {
            self.reader.seek(SeekFrom::Start(0))?;
            let mut iterator = self.layout.row_iterator(&mut self.reader)?;
            iterator.set_decode_policy(policy);
            let result = (|| -> Result<u64> {
                let mut rows = 0u64;
                while let Some(row) = iterator.try_next_projected(&normalized)? {
                    let owned: Vec<crate::cell::CellValue<'static>> = row
                        .into_iter()
                        .map(crate::cell::CellValue::into_owned)
                        .collect();
                    f(&owned)?;
                    rows = rows.saturating_add(1);
                }
                Ok(rows)
            })();
            self.reader.seek(SeekFrom::Start(0))?;
            return result;
        }

        let Some(path) = self.source_path.as_deref() else {
            return self.scan_projected_columns_parallel_ordered_with_decode_policy(
                &normalized,
                1,
                policy,
                f,
            );
        };

        let projection_legacy = std::env::var("SAS7BDAT_PROJECTION_DECODE_PLAN")
            .map(|value| !value.eq_ignore_ascii_case("compiled"))
            .unwrap_or(false);
        scan_file_projected_rows_with_decode_policy(
            path,
            &self.layout,
            &normalized,
            policy,
            Self::parallel_scan_config(parse_threads),
            projection_legacy,
            f,
        )
    }

    /// Scans projected columns in ordered parallel mode with default decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row decoding fails, or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).parallel(...).ordering(OrderingMode::Ordered).scan_ordered(...)`."
    )]
    pub fn scan_projected_columns_parallel_ordered<F>(
        &mut self,
        indices: &[usize],
        parse_threads: usize,
        f: F,
    ) -> Result<u64>
    where
        F: FnMut(&[crate::cell::CellValue<'static>]) -> Result<()>,
    {
        self.scan_projected_columns_parallel_ordered_with_decode_policy(
            indices,
            parse_threads,
            DecodePolicy::default(),
            f,
        )
    }

    /// Scans raw row bytes in parallel (unordered by default for throughput).
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Raw).parallel(...).ordering(OrderingMode::Unordered).scan_raw_unordered(...)`."
    )]
    pub fn scan_raw_rows_parallel<F>(&mut self, parse_threads: usize, f: F) -> Result<u64>
    where
        F: Fn(&[u8]) -> Result<()> + Send + Sync,
    {
        Ok(self
            .scan_raw_rows_parallel_with_stats(parse_threads, f)?
            .rows)
    }

    /// Scans raw row bytes in parallel (unordered by default) and returns row/byte stats.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Raw).parallel(...).ordering(OrderingMode::Unordered).scan_raw_unordered(...)`."
    )]
    pub fn scan_raw_rows_parallel_with_stats<F>(
        &mut self,
        parse_threads: usize,
        f: F,
    ) -> Result<RawScanStats>
    where
        F: Fn(&[u8]) -> Result<()> + Send + Sync,
    {
        self.scan_raw_rows_parallel_unordered_with_stats(parse_threads, f)
    }

    /// Scans raw row bytes in parallel and invokes a callback per owned row batch.
    ///
    /// Batches are contiguous owned byte buffers to reduce callback overhead compared
    /// to per-row callbacks when consumers ingest rows into memory.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Raw).parallel(...).ordering(OrderingMode::Unordered).batch_rows(...).collect_raw_batches(...)`."
    )]
    pub fn scan_raw_rows_parallel_batched_with_stats<F>(
        &mut self,
        parse_threads: usize,
        batch_rows: usize,
        f: F,
    ) -> Result<RawScanStats>
    where
        F: Fn(&RawRowBatch) -> Result<()> + Send + Sync,
    {
        self.scan_raw_rows_parallel_unordered_batched_with_stats(parse_threads, batch_rows, f)
    }

    /// Scans raw row bytes in parallel without preserving row order.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Raw).parallel(...).ordering(OrderingMode::Unordered).scan_raw_unordered(...)`."
    )]
    pub fn scan_raw_rows_parallel_unordered<F>(&mut self, parse_threads: usize, f: F) -> Result<u64>
    where
        F: Fn(&[u8]) -> Result<()> + Send + Sync,
    {
        Ok(self
            .scan_raw_rows_parallel_unordered_with_stats(parse_threads, f)?
            .rows)
    }

    /// Scans raw row bytes in unordered parallel mode and returns row/byte stats.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Raw).parallel(...).ordering(OrderingMode::Unordered).scan_raw_unordered(...)`."
    )]
    pub fn scan_raw_rows_parallel_unordered_with_stats<F>(
        &mut self,
        parse_threads: usize,
        f: F,
    ) -> Result<RawScanStats>
    where
        F: Fn(&[u8]) -> Result<()> + Send + Sync,
    {
        self.ensure_missing_policies_fresh()?;
        if parse_threads <= 1 {
            return self.scan_raw_rows_with_stats(|row| f(row));
        }
        let Some(path) = self.source_path.as_deref() else {
            return self.scan_raw_rows_with_stats(|row| f(row));
        };
        scan_file_raw_rows_unordered_with_stats(
            path,
            &self.layout,
            Self::parallel_scan_config(parse_threads),
            f,
        )
    }

    /// Scans raw row bytes in unordered parallel mode and invokes a callback per owned row batch.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Raw).parallel(...).ordering(OrderingMode::Unordered).batch_rows(...).collect_raw_batches(...)`."
    )]
    pub fn scan_raw_rows_parallel_unordered_batched_with_stats<F>(
        &mut self,
        parse_threads: usize,
        batch_rows: usize,
        f: F,
    ) -> Result<RawScanStats>
    where
        F: Fn(&RawRowBatch) -> Result<()> + Send + Sync,
    {
        self.ensure_missing_policies_fresh()?;
        if parse_threads <= 1 {
            return self.scan_raw_rows_batched_with_stats(batch_rows, |batch| f(batch));
        }
        let Some(path) = self.source_path.as_deref() else {
            return self.scan_raw_rows_batched_with_stats(batch_rows, |batch| f(batch));
        };
        scan_file_raw_rows_unordered_batched_with_stats(
            path,
            &self.layout,
            Self::parallel_scan_config(parse_threads),
            batch_rows,
            f,
        )
    }

    /// Scans raw row bytes in parallel while preserving row order.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Raw).parallel(...).ordering(OrderingMode::Ordered).scan_raw_ordered(...)`."
    )]
    pub fn scan_raw_rows_parallel_ordered<F>(&mut self, parse_threads: usize, f: F) -> Result<u64>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.ensure_missing_policies_fresh()?;
        if parse_threads <= 1 {
            return self.scan_raw_rows(f);
        }
        let Some(path) = self.source_path.as_deref() else {
            return self.scan_raw_rows(f);
        };
        scan_file_raw_rows(
            path,
            &self.layout,
            Self::parallel_scan_config(parse_threads),
            f,
        )
    }
}

#[allow(deprecated)]
impl<R: Read + Seek> SasReader<R> {
    /// Builds a reader from any `Read + Seek` implementor.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata parsing fails.
    pub fn from_reader(mut reader: R) -> Result<Self> {
        let layout = parse_metadata(&mut reader)?;
        reader.seek(SeekFrom::Start(0))?;
        Ok(Self {
            reader,
            layout,
            missing_policies_stale: false,
            source_path: None,
        })
    }

    /// Builds a reader from any `Read + Seek` implementor with custom metadata read options.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata parsing fails.
    pub fn from_reader_with_options(mut reader: R, options: MetadataReadOptions) -> Result<Self> {
        let layout = parse_metadata_with_options(&mut reader, options)?;
        reader.seek(SeekFrom::Start(0))?;
        Ok(Self {
            reader,
            layout,
            missing_policies_stale: false,
            source_path: None,
        })
    }

    pub const fn metadata(&self) -> &DatasetMetadata {
        &self.layout.header.metadata
    }

    #[must_use]
    pub fn query(&mut self) -> Query<'_, R> {
        Query::new(self)
    }

    /// Loads value-label catalog metadata from a companion file.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be opened or parsed.
    pub fn attach_catalog<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let mut file = File::open(path)?;
        self.attach_catalog_reader_with_policy(&mut file, CatalogScanPolicy::Eager)
    }

    /// Loads value-label catalog metadata with configurable missing-policy scan behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be opened or parsed.
    pub fn attach_catalog_with_policy<P: AsRef<Path>>(
        &mut self,
        path: P,
        scan_policy: CatalogScanPolicy,
    ) -> Result<()> {
        let mut file = File::open(path)?;
        self.attach_catalog_reader_with_policy(&mut file, scan_policy)
    }

    /// Loads value-label catalog metadata from the provided reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be parsed.
    pub fn attach_catalog_reader<C: Read + Seek>(&mut self, reader: &mut C) -> Result<()> {
        self.attach_catalog_reader_with_policy(reader, CatalogScanPolicy::Eager)
    }

    /// Loads value-label catalog metadata from the provided reader with configurable scan behavior.
    ///
    /// `CatalogScanPolicy::Deferred` keeps merged policies marked as stale until a row-consuming
    /// method is invoked or [`scan_missing_policies`](Self::scan_missing_policies) is called.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be parsed.
    pub fn attach_catalog_reader_with_policy<C: Read + Seek>(
        &mut self,
        reader: &mut C,
        scan_policy: CatalogScanPolicy,
    ) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        let catalog = parse_catalog(reader)?;

        {
            let metadata = &mut self.layout.header.metadata;

            for set in catalog.label_sets {
                metadata.label_sets.insert(set.name.clone(), set);
            }

            let lookup = build_label_lookup(&metadata.label_sets);
            for variable in &mut metadata.variables {
                if let Some(format) = &variable.format {
                    let normalized = normalize_label_name(&format.name);
                    if let Some(matched) = lookup.get(&normalized) {
                        variable.value_labels = Some(matched.clone());
                    } else if !normalized.starts_with('$') {
                        let prefixed = format!("${normalized}");
                        if let Some(matched) = lookup.get(&prefixed) {
                            variable.value_labels = Some(matched.clone());
                        }
                    }
                }

                if let Some(label_name) = &variable.value_labels
                    && let Some(set) = metadata.label_sets.get(label_name)
                {
                    merge_label_set_missing(&mut variable.missing, set);
                }
            }
        }

        match scan_policy {
            CatalogScanPolicy::Eager => self.scan_missing_policies(),
            CatalogScanPolicy::Deferred => {
                self.missing_policies_stale = true;
                Ok(())
            }
        }
    }

    /// Populates missing-value policies by scanning the dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails.
    pub fn scan_missing_policies(&mut self) -> Result<()> {
        let variable_count = self.layout.header.metadata.variables.len();
        if variable_count == 0 {
            self.missing_policies_stale = false;
            return Ok(());
        }

        let mut policies: Vec<MissingValuePolicy> = self
            .layout
            .header
            .metadata
            .variables
            .iter()
            .map(|var| var.missing.clone())
            .collect();

        self.reader.seek(SeekFrom::Start(0))?;
        {
            let mut rows = self.layout.row_iterator(&mut self.reader)?;
            for row in rows.by_ref() {
                let row = row?;
                for (idx, value) in row.iter().enumerate() {
                    if let crate::cell::CellValue::Missing(missing) = value {
                        missing::record_missing_observation(&mut policies[idx], missing);
                    }
                }
            }
        }
        self.reader.seek(SeekFrom::Start(0))?;

        for (variable, policy) in self
            .layout
            .header
            .metadata
            .variables
            .iter_mut()
            .zip(policies.into_iter())
        {
            let mut normalized_policy = policy;
            dedup_tagged_missing(&mut normalized_policy.tagged_missing);
            dedup_missing_ranges(&mut normalized_policy.ranges);
            variable.missing = normalized_policy;
        }
        self.missing_policies_stale = false;

        Ok(())
    }

    /// Creates a row iterator over the dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).stream_ordered()`."
    )]
    pub fn rows(&mut self) -> Result<RowIterator<'_, R>> {
        self.rows_with_decode_policy(DecodePolicy::default())
    }

    /// Creates a row iterator over the dataset with explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(policy).stream_ordered()`."
    )]
    pub fn rows_with_decode_policy(&mut self, policy: DecodePolicy) -> Result<RowIterator<'_, R>> {
        self.ensure_missing_policies_fresh()?;
        self.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.layout.row_iterator(&mut self.reader)?;
        iterator.set_decode_policy(policy);
        Ok(iterator)
    }

    /// Creates a row iterator using the fast-scan decode policy.
    ///
    /// Fast scan disables temporal conversion and mojibake repair and preserves
    /// trailing string whitespace for maximum throughput.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(DecodePolicy::FAST_SCAN).stream_ordered()`."
    )]
    pub fn rows_fast(&mut self) -> Result<RowIterator<'_, R>> {
        self.rows_with_decode_policy(DecodePolicy::FAST_SCAN)
    }

    /// Creates a row iterator that yields owned rows with column-name lookup.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).stream_ordered()` and map rows by metadata names."
    )]
    pub fn rows_named(&mut self) -> Result<RowIter<'_, R>> {
        self.rows_named_with_decode_policy(DecodePolicy::default())
    }

    /// Creates a row iterator that yields owned rows with column-name lookup and explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(policy).stream_ordered()` and map rows by metadata names."
    )]
    pub fn rows_named_with_decode_policy(
        &mut self,
        policy: DecodePolicy,
    ) -> Result<RowIter<'_, R>> {
        self.ensure_missing_policies_fresh()?;
        let lookup = Arc::new(row::RowLookup::from_metadata(self.metadata()));
        let iterator = self.rows_with_decode_policy(policy)?;
        Ok(RowIter::new(iterator, lookup))
    }

    /// Creates a row iterator that yields owned rows with column-name lookup using fast-scan policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(DecodePolicy::FAST_SCAN).stream_ordered()`."
    )]
    pub fn rows_named_fast(&mut self) -> Result<RowIter<'_, R>> {
        self.rows_named_with_decode_policy(DecodePolicy::FAST_SCAN)
    }

    /// Creates a streaming iterator that yields borrowed row views.
    ///
    /// Row views borrow internal buffers and are only valid until the next call to `try_next`.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).stream_ordered()`."
    )]
    pub fn stream_rows(&mut self) -> Result<RowViewIter<'_, R>> {
        self.stream_rows_with_decode_policy(DecodePolicy::default())
    }

    /// Creates a streaming iterator that yields borrowed row views with explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(policy).stream_ordered()`."
    )]
    pub fn stream_rows_with_decode_policy(
        &mut self,
        policy: DecodePolicy,
    ) -> Result<RowViewIter<'_, R>> {
        self.ensure_missing_policies_fresh()?;
        let lookup = Arc::new(row::RowLookup::from_metadata(self.metadata()));
        let iterator = self.rows_with_decode_policy(policy)?;
        Ok(RowViewIter::new(iterator, lookup, None))
    }

    /// Creates a streaming iterator that yields borrowed row views using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(DecodePolicy::FAST_SCAN).stream_ordered()`."
    )]
    pub fn stream_rows_fast(&mut self) -> Result<RowViewIter<'_, R>> {
        self.stream_rows_with_decode_policy(DecodePolicy::FAST_SCAN)
    }

    /// Creates a streaming iterator that yields borrowed row views for the named columns.
    ///
    /// Row views borrow internal buffers and are only valid until the next call to `try_next`.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name cannot be resolved.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).columns_by_name(...).stream_ordered()`."
    )]
    pub fn stream_rows_with_projection(&mut self, names: &[&str]) -> Result<RowViewIter<'_, R>> {
        self.stream_rows_with_projection_with_decode_policy(names, DecodePolicy::default())
    }

    /// Creates a streaming iterator that yields borrowed row views for named columns with explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name cannot be resolved.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).columns_by_name(...).decode(policy).stream_ordered()`."
    )]
    pub fn stream_rows_with_projection_with_decode_policy(
        &mut self,
        names: &[&str],
        policy: DecodePolicy,
    ) -> Result<RowViewIter<'_, R>> {
        self.ensure_missing_policies_fresh()?;
        let selection = RowSelection::new().columns(names);
        let metadata = &self.layout.header.metadata;
        let indices =
            selection
                .resolve_projection(metadata)?
                .ok_or_else(|| Error::InvalidMetadata {
                    details: "column projection not specified".into(),
                })?;
        let normalized = self.normalize_projection(&indices)?;
        let projection = RowProjection::new(&normalized, metadata.column_count as usize);
        let lookup = Arc::new(row::RowLookup::from_metadata(metadata));
        let iterator = self.rows_with_decode_policy(policy)?;
        Ok(RowViewIter::new(iterator, lookup, Some(projection)))
    }

    /// Creates a streaming iterator for named columns using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name cannot be resolved.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).columns_by_name(...).decode(DecodePolicy::FAST_SCAN).stream_ordered()`."
    )]
    pub fn stream_rows_with_projection_fast(
        &mut self,
        names: &[&str],
    ) -> Result<RowViewIter<'_, R>> {
        self.stream_rows_with_projection_with_decode_policy(names, DecodePolicy::FAST_SCAN)
    }

    /// Streams raw row bytes and invokes the callback for each row without decoding cells.
    ///
    /// This is the highest-throughput parser path when callers only need row bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    pub fn scan_raw_rows<F>(&mut self, mut f: F) -> Result<u64>
    where
        F: for<'row> FnMut(&'row [u8]) -> Result<()>,
    {
        Ok(self.scan_raw_rows_with_stats(&mut f)?.rows)
    }

    /// Streams raw row bytes and invokes the callback for each row without decoding cells.
    ///
    /// Returns row and raw-byte counters alongside callback processing.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    pub fn scan_raw_rows_with_stats<F>(&mut self, mut f: F) -> Result<RawScanStats>
    where
        F: for<'row> FnMut(&'row [u8]) -> Result<()>,
    {
        self.ensure_missing_policies_fresh()?;
        self.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.layout.row_iterator(&mut self.reader)?;

        let result = (|| -> Result<RawScanStats> {
            let mut stats = RawScanStats::default();
            let mut visit = |row: &[u8]| -> Result<()> {
                f(row)?;
                stats.rows = stats.rows.saturating_add(1);
                stats.raw_bytes = stats.raw_bytes.saturating_add(row.len() as u64);
                Ok(())
            };
            while iterator.try_next_raw_row_visit(&mut visit)?.is_some() {}
            Ok(stats)
        })();
        self.reader.seek(SeekFrom::Start(0))?;
        result
    }

    /// Streams raw row bytes and invokes the callback per owned row batch.
    ///
    /// Batches are contiguous owned byte buffers to reduce callback overhead when
    /// callers ingest rows into memory.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails or the callback errors.
    pub fn scan_raw_rows_batched_with_stats<F>(
        &mut self,
        batch_rows: usize,
        mut f: F,
    ) -> Result<RawScanStats>
    where
        F: FnMut(&RawRowBatch) -> Result<()>,
    {
        self.ensure_missing_policies_fresh()?;
        self.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.layout.row_iterator(&mut self.reader)?;

        let result = (|| -> Result<RawScanStats> {
            let batch_rows = batch_rows.max(1);
            let row_length = usize::try_from(self.layout.row_info.row_length).unwrap_or(0);
            let mut batch =
                RawRowBatch::with_capacity(batch_rows, row_length.saturating_mul(batch_rows));
            let mut stats = RawScanStats::default();
            while let Some(row) = iterator.try_next_raw_row()? {
                batch.push_row(row);
                stats.rows = stats.rows.saturating_add(1);
                stats.raw_bytes = stats.raw_bytes.saturating_add(row.len() as u64);
                if batch.row_count() >= batch_rows {
                    f(&batch)?;
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                f(&batch)?;
            }
            Ok(stats)
        })();
        self.reader.seek(SeekFrom::Start(0))?;
        result
    }

    /// Streams selected columns and invokes the callback with borrowed decoded values per row.
    ///
    /// This path avoids owned row materialization and is intended for high-throughput scans.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row iteration fails, or the callback errors.
    pub fn scan_projected_columns_with_decode_policy<F>(
        &mut self,
        indices: &[usize],
        policy: DecodePolicy,
        mut f: F,
    ) -> Result<u64>
    where
        F: for<'row> FnMut(&[crate::cell::CellValue<'row>]) -> Result<()>,
    {
        self.ensure_missing_policies_fresh()?;
        let normalized = self.normalize_projection(indices)?;

        self.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.layout.row_iterator(&mut self.reader)?;
        iterator.set_decode_policy(policy);
        let compiled_columns = iterator.resolve_compiled_runtime_columns(&normalized)?;

        let result = (|| -> Result<u64> {
            let mut rows = 0u64;
            while iterator
                .try_next_projected_compiled_columns_visit(&compiled_columns, &mut f)?
                .is_some()
            {
                rows = rows.saturating_add(1);
            }
            Ok(rows)
        })();
        self.reader.seek(SeekFrom::Start(0))?;
        result
    }

    /// Streams selected columns and invokes the callback with borrowed decoded values per row.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row iteration fails, or the callback errors.
    pub fn scan_projected_columns<F>(&mut self, indices: &[usize], f: F) -> Result<u64>
    where
        F: for<'row> FnMut(&[crate::cell::CellValue<'row>]) -> Result<()>,
    {
        self.scan_projected_columns_with_decode_policy(indices, DecodePolicy::default(), f)
    }

    /// Streams selected columns with fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row iteration fails, or the callback errors.
    pub fn scan_projected_columns_fast<F>(&mut self, indices: &[usize], f: F) -> Result<u64>
    where
        F: for<'row> FnMut(&[crate::cell::CellValue<'row>]) -> Result<()>,
    {
        self.scan_projected_columns_with_decode_policy(indices, DecodePolicy::FAST_SCAN, f)
    }

    /// Streams selected numeric columns as raw SAS numeric values (`f64`) with missing values as `None`.
    ///
    /// This bypasses `CellValue` materialization and temporal conversion for maximum scan throughput.
    ///
    /// # Errors
    ///
    /// Returns an error if any projected column is non-numeric or row iteration fails.
    pub fn scan_numeric_columns<F>(&mut self, indices: &[usize], mut f: F) -> Result<u64>
    where
        F: FnMut(&[Option<f64>]) -> Result<()>,
    {
        self.ensure_missing_policies_fresh()?;
        let normalized = self.normalize_projection(indices)?;

        self.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.layout.row_iterator(&mut self.reader)?;
        iterator.set_decode_policy(DecodePolicy::FAST_SCAN);
        let selected_columns = iterator.resolve_numeric_runtime_columns(&normalized)?;

        let mut values = Vec::with_capacity(selected_columns.len());
        let result = (|| -> Result<u64> {
            let mut rows = 0u64;
            while iterator
                .try_next_numeric_projected_columns(&selected_columns, &mut values)?
                .is_some()
            {
                f(&values)?;
                rows = rows.saturating_add(1);
            }
            Ok(rows)
        })();
        self.reader.seek(SeekFrom::Start(0))?;
        result
    }

    /// Streams selected numeric columns by name as raw SAS numeric values (`f64`) with missing as `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if projection resolution fails, any column is non-numeric, or row iteration fails.
    pub fn scan_numeric_columns_by_name<F>(&mut self, names: &[&str], f: F) -> Result<u64>
    where
        F: FnMut(&[Option<f64>]) -> Result<()>,
    {
        let selection = RowSelection::new().columns(names);
        let metadata = &self.layout.header.metadata;
        let indices =
            selection
                .resolve_projection(metadata)?
                .ok_or_else(|| Error::InvalidMetadata {
                    details: "column projection not specified".into(),
                })?;
        self.scan_numeric_columns(&indices, f)
    }

    /// Creates a row iterator configured by the provided selection.
    ///
    /// This method is intended for pagination without column projection. Use
    /// [`select_with`] when selecting a subset of columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the selection specifies a projection, if the reader
    /// cannot be positioned, or if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).skip_rows(...).max_rows(...).stream_ordered()`."
    )]
    pub fn rows_windowed(&mut self, selection: &RowSelection) -> Result<RowWindow<'_, R>> {
        self.rows_windowed_with_decode_policy(selection, DecodePolicy::default())
    }

    /// Creates a row iterator configured by the provided selection with explicit decode policy.
    ///
    /// This method is intended for pagination without column projection. Use
    /// [`select_with_decode_policy`] when selecting a subset of columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the selection specifies a projection, if the reader
    /// cannot be positioned, or if row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(policy).skip_rows(...).max_rows(...).stream_ordered()`."
    )]
    pub fn rows_windowed_with_decode_policy(
        &mut self,
        selection: &RowSelection,
        policy: DecodePolicy,
    ) -> Result<RowWindow<'_, R>> {
        self.ensure_missing_policies_fresh()?;
        if selection.has_projection() {
            return Err(Error::InvalidMetadata {
                details: "rows_windowed does not accept column projection; use select_with instead"
                    .into(),
            });
        }
        let iterator = self.rows_with_decode_policy(policy)?;
        Ok(RowWindow::new(
            iterator,
            selection.skip_count(),
            selection.max_count(),
        ))
    }

    /// Creates a row window iterator using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the selection specifies a projection or row iteration cannot be initialised.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Rows).decode(DecodePolicy::FAST_SCAN).skip_rows(...).max_rows(...).stream_ordered()`."
    )]
    pub fn rows_windowed_fast(&mut self, selection: &RowSelection) -> Result<RowWindow<'_, R>> {
        self.rows_windowed_with_decode_policy(selection, DecodePolicy::FAST_SCAN)
    }

    /// Creates an iterator that yields a subset of columns for each row.
    ///
    /// # Errors
    ///
    /// Returns an error if any requested column index is invalid or if row
    /// decoding fails.
    pub fn select_columns(&mut self, indices: &[usize]) -> Result<ProjectedRowIter<'_, R>> {
        self.select_columns_with_decode_policy(indices, DecodePolicy::default())
    }

    /// Creates an iterator that yields a subset of columns for each row with explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if any requested column index is invalid or if row
    /// decoding fails.
    pub fn select_columns_with_decode_policy(
        &mut self,
        indices: &[usize],
        policy: DecodePolicy,
    ) -> Result<ProjectedRowIter<'_, R>> {
        self.ensure_missing_policies_fresh()?;
        let normalized = self.normalize_projection(indices)?;
        let inner = self.rows_with_decode_policy(policy)?;
        let compiled_columns = inner.resolve_compiled_runtime_columns(&normalized)?;
        Ok(ProjectedRowIter {
            inner,
            compiled_columns,
            exhausted: false,
        })
    }

    /// Creates a projected iterator using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if any requested column index is invalid or row decoding fails.
    pub fn select_columns_fast(&mut self, indices: &[usize]) -> Result<ProjectedRowIter<'_, R>> {
        self.select_columns_with_decode_policy(indices, DecodePolicy::FAST_SCAN)
    }

    /// Creates an iterator configured by selection with column projection.
    ///
    /// # Errors
    ///
    /// Returns an error when projection cannot be resolved or row decoding fails.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).skip_rows(...).max_rows(...).stream_ordered()`."
    )]
    pub fn select_with(&mut self, selection: &RowSelection) -> Result<ProjectedRowWindow<'_, R>> {
        self.select_with_decode_policy(selection, DecodePolicy::default())
    }

    /// Creates an iterator configured by selection with column projection and explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error when projection cannot be resolved or row decoding fails.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).decode(policy).skip_rows(...).max_rows(...).stream_ordered()`."
    )]
    pub fn select_with_decode_policy(
        &mut self,
        selection: &RowSelection,
        policy: DecodePolicy,
    ) -> Result<ProjectedRowWindow<'_, R>> {
        let metadata = &self.layout.header.metadata;
        let indices =
            selection
                .resolve_projection(metadata)?
                .ok_or_else(|| Error::InvalidMetadata {
                    details: "column projection not specified".into(),
                })?;
        let projected = self.select_columns_with_decode_policy(&indices, policy)?;
        Ok(ProjectedRowWindow::new(
            projected,
            selection.skip_count(),
            selection.max_count(),
        ))
    }

    /// Creates a projected row window using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error when projection cannot be resolved or row decoding fails.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).projection(...).decode(DecodePolicy::FAST_SCAN).skip_rows(...).max_rows(...).stream_ordered()`."
    )]
    pub fn select_with_fast(
        &mut self,
        selection: &RowSelection,
    ) -> Result<ProjectedRowWindow<'_, R>> {
        self.select_with_decode_policy(selection, DecodePolicy::FAST_SCAN)
    }

    /// Creates an iterator that yields only the named columns.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name cannot be resolved.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).columns_by_name(...).stream_ordered()`."
    )]
    pub fn rows_with_projection(&mut self, names: &[&str]) -> Result<ProjectedRowIter<'_, R>> {
        self.rows_with_projection_with_decode_policy(names, DecodePolicy::default())
    }

    /// Creates an iterator that yields only the named columns with explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name cannot be resolved.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).columns_by_name(...).decode(policy).stream_ordered()`."
    )]
    pub fn rows_with_projection_with_decode_policy(
        &mut self,
        names: &[&str],
        policy: DecodePolicy,
    ) -> Result<ProjectedRowIter<'_, R>> {
        let selection = RowSelection::new().columns(names);
        let metadata = &self.layout.header.metadata;
        let indices =
            selection
                .resolve_projection(metadata)?
                .ok_or_else(|| Error::InvalidMetadata {
                    details: "column projection not specified".into(),
                })?;
        self.select_columns_with_decode_policy(&indices, policy)
    }

    /// Creates an iterator that yields named columns using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name cannot be resolved.
    #[deprecated(
        since = "0.2.0",
        note = "Use `query().shape(Shape::Projection).columns_by_name(...).decode(DecodePolicy::FAST_SCAN).stream_ordered()`."
    )]
    pub fn rows_with_projection_fast(&mut self, names: &[&str]) -> Result<ProjectedRowIter<'_, R>> {
        self.rows_with_projection_with_decode_policy(names, DecodePolicy::FAST_SCAN)
    }

    /// Streams the full dataset into a custom sink implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or if the sink reports a failure.
    pub fn stream_into<S: RowSink>(&mut self, sink: &mut S) -> Result<()> {
        self.stream_into_with_decode_policy(sink, DecodePolicy::default())
    }

    /// Collects all rows into a frame-oriented in-memory batch.
    ///
    /// # Errors
    ///
    /// Returns an error if frame materialization fails.
    pub fn collect_frame(&mut self) -> Result<FrameBatch> {
        self.query().shape(Shape::Frame).collect_frame()
    }

    /// Reads all rows into a single frame-oriented in-memory batch.
    ///
    /// This is an alias for [`Self::collect_frame`].
    ///
    /// # Errors
    ///
    /// Returns an error if frame materialization fails.
    pub fn read_frame(&mut self) -> Result<FrameBatch> {
        self.collect_frame()
    }

    /// Collects rows into multiple frame batches with at most `batch_rows` rows each.
    ///
    /// # Errors
    ///
    /// Returns an error if frame materialization fails.
    pub fn collect_frame_batches(&mut self, batch_rows: usize) -> Result<Vec<FrameBatch>> {
        self.query()
            .shape(Shape::Frame)
            .collect_frame_batches(batch_rows)
    }

    /// Reads rows into frame batches with at most `batch_rows` rows each.
    ///
    /// This is an alias for [`Self::collect_frame_batches`].
    ///
    /// # Errors
    ///
    /// Returns an error if frame materialization fails.
    pub fn read_frame_batches(&mut self, batch_rows: usize) -> Result<Vec<FrameBatch>> {
        self.collect_frame_batches(batch_rows)
    }

    /// Throughput-first raw scan shortcut using unordered parallel execution.
    ///
    /// # Errors
    ///
    /// Returns an error if raw scanning fails or the callback errors.
    pub fn scan_raw_fast_with_stats<F>(
        &mut self,
        parse_threads: usize,
        f: F,
    ) -> Result<RawScanStats>
    where
        F: Fn(&[u8]) -> Result<()> + Send + Sync,
    {
        self.query()
            .shape(Shape::Raw)
            .parallel(parse_threads)
            .ordering(OrderingMode::Unordered)
            .scan_raw_unordered(f)
    }

    /// Throughput-first projected scan shortcut using unordered parallel execution and fast decode.
    ///
    /// # Errors
    ///
    /// Returns an error if projection validation fails, row decoding fails, or the callback errors.
    pub fn scan_projected_fast<F>(
        &mut self,
        indices: &[usize],
        parse_threads: usize,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[crate::cell::CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.query()
            .shape(Shape::Projection)
            .projection(indices)
            .decode(DecodePolicy::FAST_SCAN)
            .parallel(parse_threads)
            .ordering(OrderingMode::Unordered)
            .scan_unordered(f)
    }

    /// Throughput-first raw ingestion shortcut that materializes owned raw row batches.
    ///
    /// # Errors
    ///
    /// Returns an error if raw scanning fails.
    pub fn collect_raw_batches_fast(
        &mut self,
        parse_threads: usize,
        batch_rows: usize,
    ) -> Result<Vec<RawRowBatch>> {
        self.query()
            .shape(Shape::Raw)
            .parallel(parse_threads)
            .ordering(OrderingMode::Unordered)
            .collect_raw_batches(batch_rows)
    }

    /// Streams the full dataset into a custom sink implementation with explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or if the sink reports a failure.
    pub fn stream_into_with_decode_policy<S: RowSink>(
        &mut self,
        sink: &mut S,
        policy: DecodePolicy,
    ) -> Result<()> {
        self.ensure_missing_policies_fresh()?;
        self.reader.seek(SeekFrom::Start(0))?;
        let context = SinkContext::new(&self.layout);
        sink.begin(context)?;
        let mut iterator = self.layout.row_iterator(&mut self.reader)?;
        iterator.set_decode_policy(policy);
        iterator.stream_all(|row| sink.write_streaming_row(row))?;
        sink.finish()?;
        self.reader.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    /// Streams the full dataset into a sink using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or if the sink reports a failure.
    pub fn stream_into_fast<S: RowSink>(&mut self, sink: &mut S) -> Result<()> {
        self.stream_into_with_decode_policy(sink, DecodePolicy::FAST_SCAN)
    }

    /// Consumes the reader and returns a row iterator yielding owned rows.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    #[allow(clippy::should_implement_trait)]
    pub fn into_iter(self) -> Result<crate::parser::OwnedRowIterator<R>> {
        self.into_iter_with_decode_policy(DecodePolicy::default())
    }

    /// Consumes the reader and returns a row iterator yielding owned rows with explicit decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    pub fn into_iter_with_decode_policy(
        mut self,
        policy: DecodePolicy,
    ) -> Result<crate::parser::OwnedRowIterator<R>> {
        self.ensure_missing_policies_fresh()?;
        let layout = Box::new(self.layout);
        let mut reader = self.reader;
        reader.seek(SeekFrom::Start(0))?;
        let mut iterator = crate::parser::RowIteratorCore::new(reader, layout)?;
        iterator.set_decode_policy(policy);
        Ok(iterator)
    }

    /// Consumes the reader and returns an owned row iterator using fast-scan decode policy.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    pub fn into_iter_fast(self) -> Result<crate::parser::OwnedRowIterator<R>> {
        self.into_iter_with_decode_policy(DecodePolicy::FAST_SCAN)
    }

    pub fn into_parts(self) -> (R, DatasetLayout) {
        (self.reader, self.layout)
    }

    fn ensure_missing_policies_fresh(&mut self) -> Result<()> {
        if self.missing_policies_stale {
            self.scan_missing_policies()?;
        }
        Ok(())
    }

    fn normalize_projection(&self, indices: &[usize]) -> Result<Vec<usize>> {
        let column_count = self.layout.header.metadata.column_count as usize;
        if indices.is_empty() {
            return Err(Error::InvalidMetadata {
                details: "projected column list may not be empty".into(),
            });
        }
        let mut normalized = Vec::with_capacity(indices.len());
        let mut seen = HashSet::with_capacity(indices.len());
        for &idx in indices {
            if idx >= column_count {
                return Err(Error::InvalidMetadata {
                    details: format!(
                        "column projection index {idx} exceeds column count {column_count}"
                    )
                    .into(),
                });
            }
            if !seen.insert(idx) {
                return Err(Error::InvalidMetadata {
                    details: format!("duplicate column projection index {idx}").into(),
                });
            }
            normalized.push(idx);
        }
        Ok(normalized)
    }
}
