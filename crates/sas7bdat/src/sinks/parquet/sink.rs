use super::{
    constants::{
        DEFAULT_ROW_GROUP_SIZE, DEFAULT_TARGET_ROW_GROUP_BYTES, MAX_AUTO_ROW_GROUP_ROWS,
        MIN_AUTO_ROW_GROUP_ROWS,
    },
    plan::ColumnPlan,
};
use crate::{
    cell::CellValue,
    error::{Error, Result},
    parser::ColumnarBatch,
    sinks::{ColumnarSink, RowSink, SinkContext, validate_sink_begin},
};
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::{Type, TypePtr},
};
use std::{borrow::Cow, io::Write, sync::Arc};

/// Writes decoded SAS rows into a Parquet file.
pub struct ParquetSink<W: Write + Send> {
    output: Option<W>,
    writer: Option<SerializedFileWriter<W>>,
    row_group_size: usize,
    columns: Vec<ColumnPlan>,
    rows_buffered: usize,
    auto_row_group_size: bool,
    target_row_group_bytes: usize,
    streaming_columnar: bool,
    lenient_dates: bool,
}

impl<W: Write + Send> ParquetSink<W> {
    /// Creates a new sink that writes to the supplied writer.
    #[must_use]
    pub const fn new(writer: W) -> Self {
        Self {
            output: Some(writer),
            writer: None,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            columns: Vec::new(),
            rows_buffered: 0,
            auto_row_group_size: true,
            target_row_group_bytes: DEFAULT_TARGET_ROW_GROUP_BYTES,
            streaming_columnar: false,
            lenient_dates: true,
        }
    }

    /// Configures the number of rows buffered per Parquet row group.
    #[must_use]
    pub const fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self.auto_row_group_size = false;
        self
    }

    /// Sets the target byte size used when automatically estimating row group size.
    #[must_use]
    pub const fn with_target_row_group_bytes(mut self, bytes: usize) -> Self {
        self.target_row_group_bytes = if bytes == 0 { 1 } else { bytes };
        self.auto_row_group_size = true;
        self
    }

    /// Toggles streaming columnar mode where batches map directly to row groups.
    #[must_use]
    pub const fn with_streaming_columnar(mut self, enabled: bool) -> Self {
        self.streaming_columnar = enabled;
        self
    }

    /// Controls whether date/time columns are downgraded to numeric on invalid data.
    #[must_use]
    pub const fn with_lenient_dates(mut self, enabled: bool) -> Self {
        self.lenient_dates = enabled;
        self
    }

    fn estimate_row_group_size(&self, context: &SinkContext<'_>) -> usize {
        let mut approx_row_bytes = context
            .columns
            .iter()
            .map(|column| usize::try_from(column.offsets.width).unwrap_or(0))
            .sum::<usize>();
        if approx_row_bytes == 0 {
            approx_row_bytes = 1;
        }

        let mut rows = self.target_row_group_bytes.saturating_div(approx_row_bytes);
        if rows == 0 {
            rows = 1;
        }

        if rows > MAX_AUTO_ROW_GROUP_ROWS {
            rows = MAX_AUTO_ROW_GROUP_ROWS;
        } else if rows < MIN_AUTO_ROW_GROUP_ROWS {
            rows = rows.max(1);
        }

        if context.metadata.row_count > 0
            && let Ok(total_rows) = usize::try_from(context.metadata.row_count)
        {
            rows = rows.min(total_rows.max(1));
        }

        rows
    }

    /// Returns the underlying writer once the sink has been finalised.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer has not been finished or if the
    /// internal output has already been taken.
    pub fn into_inner(mut self) -> Result<W> {
        if self.writer.is_some() {
            return Err(Error::Unsupported {
                feature: Cow::from("attempted to take Parquet writer before sink was finished"),
            });
        }
        self.output.take().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink output already consumed"),
        })
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows_buffered == 0 {
            return Ok(());
        }

        let selection: Vec<usize> = (0..self.columns.len()).collect();
        self.with_selection_row_group(&selection, |plan, column_writer, _| {
            plan.flush(column_writer)
        })
    }
}

impl<W: Write + Send> RowSink for ParquetSink<W> {
    fn begin(&mut self, context: SinkContext<'_>) -> Result<()> {
        validate_sink_begin(&context, self.writer.is_some(), "Parquet")?;

        let mut plans = Vec::with_capacity(context.columns.len());
        let mut fields: Vec<TypePtr> = Vec::with_capacity(context.columns.len());

        if self.auto_row_group_size {
            self.row_group_size = self.estimate_row_group_size(&context);
            self.auto_row_group_size = false;
        }

        for (variable, column) in context
            .metadata
            .variables
            .iter()
            .zip(context.columns.iter())
        {
            let (plan, field) = ColumnPlan::new(
                variable,
                column,
                self.lenient_dates,
                context.source_path.as_deref(),
            )?;
            fields.push(field);
            plans.push(plan);
        }
        for plan in &mut plans {
            plan.reserve_capacity(self.row_group_size);
        }

        let schema = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?;
        let schema = Arc::new(schema);

        let props = WriterProperties::builder().build();
        let output = self.output.take().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink output already taken"),
        })?;
        let writer = SerializedFileWriter::new(output, schema, props.into())?;

        self.columns = plans;
        self.writer = Some(writer);
        self.rows_buffered = 0;
        Ok(())
    }

    fn write_row(&mut self, row: &[CellValue<'_>]) -> Result<()> {
        if self.writer.is_none() {
            return Err(Error::Unsupported {
                feature: Cow::from("rows written before Parquet sink initialised"),
            });
        }

        if row.len() != self.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "row length {} does not match column count {}",
                    row.len(),
                    self.columns.len()
                )),
            });
        }

        for (value, plan) in row.iter().zip(self.columns.iter_mut()) {
            plan.push(value)?;
        }

        self.rows_buffered = self.rows_buffered.saturating_add(1);

        if self.row_group_size > 0 && self.rows_buffered >= self.row_group_size {
            self.flush()?;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.writer.is_none() {
            return Ok(());
        }

        if self.rows_buffered > 0 {
            self.flush()?;
        }

        if let Some(writer) = self.writer.take() {
            let output = writer.into_inner()?;
            self.output = Some(output);
        }
        self.columns.clear();
        self.rows_buffered = 0;
        Ok(())
    }
}

impl<W: Write + Send> ColumnarSink for ParquetSink<W> {
    fn write_columnar_batch(
        &mut self,
        batch: &ColumnarBatch<'_>,
        selection: &[usize],
    ) -> Result<()> {
        if self.writer.is_none() {
            return Err(Error::Unsupported {
                feature: Cow::from("rows written before Parquet sink initialised"),
            });
        }
        if selection.len() != self.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::from("column selection length does not match sink columns"),
            });
        }

        if self.streaming_columnar {
            self.write_columnar_batch_streaming(batch, selection)?;
            return Ok(());
        }

        for (plan, &source_idx) in self.columns.iter_mut().zip(selection.iter()) {
            let column = batch
                .column(source_idx)
                .ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column selection index {source_idx} exceeds available columns"
                    )),
                })?;
            plan.extend_columnar(&column)?;
        }

        self.rows_buffered = self.rows_buffered.saturating_add(batch.row_count);
        if self.row_group_size > 0 && self.rows_buffered >= self.row_group_size {
            self.flush()?;
        }
        Ok(())
    }
}

impl<W: Write + Send> ParquetSink<W> {
    #[inline]
    const fn streaming_chunk_rows(&self) -> usize {
        // Align streaming chunking to the configured row group size to reduce write_batch calls.
        let rows = self.row_group_size;
        if rows == 0 { 1 } else { rows }
    }

    fn write_columnar_batch_streaming(
        &mut self,
        batch: &ColumnarBatch<'_>,
        selection: &[usize],
    ) -> Result<()> {
        let chunk_rows = self.streaming_chunk_rows().max(1);
        self.with_selection_row_group(selection, |plan, column_writer, source_idx| {
            let column = batch
                .column(source_idx)
                .ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column selection index {source_idx} exceeds available columns"
                    )),
                })?;
            match plan.encoder {
                super::plan::ColumnValueEncoder::Utf8 => {
                    if let Some(materialized) = batch.materialize_utf8(source_idx)? {
                        plan.stream_columnar_materialized_utf8(column_writer, &materialized)?;
                    } else {
                        plan.stream_columnar(column_writer, &column, chunk_rows)?;
                    }
                }
                _ => {
                    plan.stream_columnar(column_writer, &column, chunk_rows)?;
                }
            }
            Ok(())
        })
    }

    fn ensure_writer_initialised(&self) -> Result<()> {
        if self.writer.is_none() {
            return Err(Error::Unsupported {
                feature: Cow::from("rows written before Parquet sink initialised"),
            });
        }
        Ok(())
    }

    fn ensure_selection_valid(&self, len: usize) -> Result<()> {
        if len != self.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::from("column selection length does not match sink columns"),
            });
        }
        Ok(())
    }

    fn with_selection_row_group<F>(&mut self, selection: &[usize], mut f: F) -> Result<()>
    where
        F: FnMut(
            &mut super::plan::ColumnPlan,
            parquet::file::writer::SerializedColumnWriter<'_>,
            usize,
        ) -> Result<()>,
    {
        self.ensure_writer_initialised()?;
        self.ensure_selection_valid(selection.len())?;

        let writer = self.writer.as_mut().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink has not been initialised"),
        })?;
        let mut row_group = writer.next_row_group()?;

        for (plan, &source_idx) in self.columns.iter_mut().zip(selection.iter()) {
            let column_writer = row_group.next_column()?.ok_or_else(|| Error::Parquet {
                details: Cow::from("writer returned fewer columns than metadata described"),
            })?;
            f(plan, column_writer, source_idx)?;
        }

        if row_group.next_column()?.is_some() {
            return Err(Error::Parquet {
                details: Cow::from("writer returned more columns than metadata described"),
            });
        }
        row_group.close()?;
        self.rows_buffered = 0;
        Ok(())
    }
}
