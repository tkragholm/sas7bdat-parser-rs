use crate::catalog::Catalog;
use anyhow::Result;
use arrow_schema::{Schema, SchemaRef};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use csv::WriterBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use sas7bdat::{
    BatchHint, CellValue, Dataset, Error, Parallelism, Projection, RowSelection, ScanBuilder,
};
use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanOptions<'a> {
    pub selection: Option<RowSelection>,
    pub projection: Option<&'a Projection>,
    pub parse_threads: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub struct WriteOptions<'a> {
    pub row_group_rows: Option<usize>,
    pub batch_rows: Option<usize>,
    pub scan: ScanOptions<'a>,
    pub catalog: Option<&'a Catalog>,
}

impl WriteOptions<'_> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            row_group_rows: None,
            batch_rows: None,
            scan: ScanOptions {
                selection: None,
                projection: None,
                parse_threads: None,
            },
            catalog: None,
        }
    }
}

impl Default for WriteOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DelimitedWriteOptions<'a> {
    pub delimiter: u8,
    pub headers: bool,
    pub scan: ScanOptions<'a>,
}

/// # Errors
///
/// Returns an error if parquet writing or Arrow conversion fails.
pub fn write_parquet(dataset: &Dataset, output: &Path, options: WriteOptions<'_>) -> Result<()> {
    let file = File::create(output)?;
    let mut scan = apply_scan_options(dataset, options.scan);
    if let Some(rows) = options.batch_rows {
        scan = scan.with_batch_hint(BatchHint::Rows(rows.max(1)));
    }
    let schema = apply_catalog_metadata(scan.arrow_schema()?, dataset, options.catalog)?;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(options.row_group_rows.unwrap_or(65_536).max(1)))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    scan.visit_arrow_batches(|batch| {
        writer
            .write(&batch)
            .map_err(|err| Error::arrow(err.to_string()))?;
        Ok(ControlFlow::Continue(()))
    })?;
    writer.close()?;
    Ok(())
}

/// # Errors
///
/// Returns an error if CSV or TSV writing fails.
pub fn write_csv_or_tsv(
    dataset: &Dataset,
    output: &Path,
    options: DelimitedWriteOptions<'_>,
) -> Result<()> {
    let scan = apply_scan_options(dataset, options.scan);

    let file = File::create(output)?;
    let mut writer = WriterBuilder::new()
        .delimiter(options.delimiter)
        .has_headers(false)
        .from_writer(file);

    let header_names: Vec<String> = options.scan.projection.map_or_else(
        || {
            dataset
                .columns()
                .iter()
                .map(|column| column.name.clone())
                .collect()
        },
        |projection| {
            projection
                .columns()
                .iter()
                .map(|column| column.name.clone())
                .collect()
        },
    );

    // Precompute the SAS epoch once instead of rebuilding it per date/time cell.
    let (date_epoch, datetime_epoch) = sas_epochs();

    let mut wrote_header = false;
    // Reused across every cell so formatting numerics/dates allocates no per-cell String.
    let mut scratch = String::new();
    scan.visit_rows(|row| {
        if options.headers && !wrote_header {
            writer
                .write_record(header_names.iter())
                .map_err(|err| Error::unsupported(format!("csv write failed: {err}")))?;
            wrote_header = true;
        }
        for cell in row.iter() {
            write_cell_field(&mut writer, &mut scratch, cell, date_epoch, datetime_epoch)
                .map_err(|err| Error::unsupported(format!("csv write failed: {err}")))?;
        }
        // Terminate the record after the field sequence.
        writer
            .write_record(None::<&[u8]>)
            .map_err(|err| Error::unsupported(format!("csv write failed: {err}")))?;
        Ok(ControlFlow::Continue(()))
    })?;
    writer.flush()?;
    Ok(())
}

fn apply_scan_options<'a>(dataset: &'a Dataset, options: ScanOptions<'a>) -> ScanBuilder<'a> {
    let mut scan = dataset.scan();
    if let Some(selection) = options.selection {
        scan = scan.select(selection);
    }
    if let Some(projection) = options.projection {
        scan = scan.with_projection(projection);
    }
    if let Some(threads) = options.parse_threads {
        scan = scan.with_parallelism(Parallelism::Threads(threads.max(1)));
    }
    scan
}

fn apply_catalog_metadata(
    schema: SchemaRef,
    dataset: &Dataset,
    catalog: Option<&Catalog>,
) -> Result<SchemaRef> {
    let Some(catalog) = catalog else {
        return Ok(schema);
    };

    let mut fields = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        let mut field = field.as_ref().clone();
        if let Some(column) = dataset.columns().get(idx)
            && let Some(format_name) = column.format.as_deref()
            && let Some(label_set) = catalog.label_set_for_format(format_name)
        {
            let mut metadata = field.metadata().clone();
            metadata.insert(
                "sas.value_labels".to_string(),
                serde_json::to_string(label_set)?,
            );
            field = field.with_metadata(metadata);
        }
        fields.push(Arc::new(field));
    }

    let mut schema = Schema::new(fields);
    let metadata = schema.metadata().clone();
    schema = schema.with_metadata(metadata);
    Ok(Arc::new(schema))
}

/// The SAS epoch (`1960-01-01`) as a date and as a midnight datetime, computed once per export.
fn sas_epochs() -> (NaiveDate, NaiveDateTime) {
    let date_epoch = NaiveDate::from_ymd_opt(1960, 1, 1).expect("valid SAS epoch");
    let datetime_epoch = date_epoch
        .and_hms_opt(0, 0, 0)
        .expect("valid SAS epoch time");
    (date_epoch, datetime_epoch)
}

/// Formats a single cell into `scratch` (reused across cells) and writes it as one CSV field.
///
/// String and null cells are written straight from their borrowed bytes; everything else is
/// formatted into the shared `scratch` buffer, so no per-cell `String` is allocated. The SAS
/// epoch is passed in precomputed rather than rebuilt per cell.
fn write_cell_field<W: Write>(
    writer: &mut csv::Writer<W>,
    scratch: &mut String,
    cell: &CellValue<'_>,
    date_epoch: NaiveDate,
    datetime_epoch: NaiveDateTime,
) -> csv::Result<()> {
    match cell {
        CellValue::Null => writer.write_field(""),
        CellValue::Str(value) => writer.write_field(value.as_bytes()),
        CellValue::Int32(value) => {
            scratch.clear();
            let _ = write!(scratch, "{value}");
            writer.write_field(scratch.as_bytes())
        }
        CellValue::Int64(value) => {
            scratch.clear();
            let _ = write!(scratch, "{value}");
            writer.write_field(scratch.as_bytes())
        }
        CellValue::Float64(value) => {
            scratch.clear();
            let _ = write!(scratch, "{value}");
            writer.write_field(scratch.as_bytes())
        }
        CellValue::Bytes(value) => {
            scratch.clear();
            scratch.push_str("0x");
            for byte in *value {
                let _ = write!(scratch, "{byte:02x}");
            }
            writer.write_field(scratch.as_bytes())
        }
        CellValue::Date(value) => {
            scratch.clear();
            let date = date_epoch + Duration::days(i64::from(value.days_since_sas_epoch));
            let _ = write!(scratch, "{}", date.format("%Y-%m-%d"));
            writer.write_field(scratch.as_bytes())
        }
        CellValue::DateTime(value) => {
            scratch.clear();
            let datetime = datetime_epoch + Duration::seconds(value.seconds_since_sas_epoch);
            let _ = write!(scratch, "{}", datetime.format("%Y-%m-%d %H:%M:%S"));
            writer.write_field(scratch.as_bytes())
        }
        CellValue::Time(value) => {
            scratch.clear();
            let seconds = u32::try_from(value.seconds_since_midnight).unwrap_or(0);
            let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds, 0)
                .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).expect("valid midnight"));
            let _ = write!(scratch, "{}", time.format("%H:%M:%S"));
            writer.write_field(scratch.as_bytes())
        }
    }
}
