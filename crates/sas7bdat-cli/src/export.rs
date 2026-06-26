use crate::catalog::{Catalog, LabelSet};
use crate::cli::CompressionCodec;
use crate::sas_metadata::{DatasetMetaJson, PARQUET_METADATA_KEY};
use anyhow::Result;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use csv::WriterBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

/// Column-statistics min/max longer than this (bytes) are truncated, keeping footers
/// small. Matches the parquet-linter `string_statistics` recommendation and parquet-rs's
/// own page-index default.
const STATISTICS_TRUNCATE_LENGTH: usize = 64;

/// Map the user-facing codec choice to a parquet compression setting.
// Zstd level 3 is a compile-time constant and always valid, so this never panics.
#[must_use]
#[allow(clippy::missing_panics_doc)]
pub fn resolve_compression(codec: CompressionCodec) -> Compression {
    match codec {
        CompressionCodec::Zstd => {
            Compression::ZSTD(ZstdLevel::try_new(3).expect("zstd level 3 is valid"))
        }
        CompressionCodec::Lz4 => Compression::LZ4_RAW,
        CompressionCodec::Snappy => Compression::SNAPPY,
        CompressionCodec::None => Compression::UNCOMPRESSED,
    }
}

/// Set the per-column *fallback* encoding for the types where it pays off, matching the
/// parquet-linter `float_encoding` and `timestamp_encoding` rules. Dictionary encoding
/// stays enabled, so low-cardinality columns still use the dictionary and only high-
/// cardinality columns (which would otherwise spill to PLAIN) pick up the better encoding.
fn apply_column_encodings(
    mut builder: WriterPropertiesBuilder,
    schema: &Schema,
) -> WriterPropertiesBuilder {
    for field in schema.fields() {
        let encoding = match field.data_type() {
            // BYTE_STREAM_SPLIT typically compresses continuous floats 2-4x better.
            DataType::Float32 | DataType::Float64 => Some(Encoding::BYTE_STREAM_SPLIT),
            // DELTA_BINARY_PACKED suits monotonic-ish temporal integers.
            DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_) => Some(Encoding::DELTA_BINARY_PACKED),
            _ => None,
        };
        if let Some(encoding) = encoding {
            let path = ColumnPath::from(field.name().as_str());
            builder = builder.set_column_encoding(path, encoding);
        }
    }
    builder
}
use sas7bdat::{
    BatchHint, CellValue, ColumnMeta, Dataset, Error, Parallelism, Projection, RowSelection,
    ScanBuilder,
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
    /// Embed SAS dataset/column metadata into the Parquet file's key-value metadata.
    pub embed_metadata: bool,
    /// Compression codec for the written Parquet data.
    pub compression: Compression,
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
            embed_metadata: false,
            compression: Compression::UNCOMPRESSED,
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
///
/// Returns the number of rows written.
pub fn write_parquet(dataset: &Dataset, output: &Path, options: WriteOptions<'_>) -> Result<u64> {
    let file = File::create(output)?;
    let mut scan = apply_scan_options(dataset, options.scan);
    if let Some(rows) = options.batch_rows {
        scan = scan.with_batch_hint(BatchHint::Rows(rows.max(1)));
    }
    let schema = apply_catalog_metadata(
        scan.arrow_schema()?,
        dataset,
        options.catalog,
        options.scan.projection,
    )?;
    let mut builder = WriterProperties::builder()
        .set_max_row_group_row_count(Some(options.row_group_rows.unwrap_or(65_536).max(1)))
        .set_compression(options.compression)
        // Keep column-chunk statistics small for long strings (parquet-linter
        // `string_statistics`); page-index stats are already truncated by parquet-rs.
        .set_statistics_truncate_length(Some(STATISTICS_TRUNCATE_LENGTH));
    builder = apply_column_encodings(builder, &schema);
    let props = builder.build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    if options.embed_metadata {
        // Write as a Parquet file-level key-value pair (not Arrow schema metadata), so it is
        // visible to plain Parquet readers — e.g. DuckDB's `parquet_kv_metadata()`.
        let json = sas_metadata_json(dataset, options.scan.projection)?;
        writer.append_key_value_metadata(KeyValue::new(PARQUET_METADATA_KEY.to_string(), json));
    }
    let stats = scan.visit_arrow_batches(|batch| {
        writer
            .write(&batch)
            .map_err(|err| Error::arrow(err.to_string()))?;
        Ok(ControlFlow::Continue(()))
    })?;
    writer.close()?;
    Ok(stats.rows_emitted)
}

/// # Errors
///
/// Returns an error if CSV or TSV writing fails.
///
/// Returns the number of rows written (excluding the header).
pub fn write_csv_or_tsv(
    dataset: &Dataset,
    output: &Path,
    options: DelimitedWriteOptions<'_>,
) -> Result<u64> {
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
    let stats = scan.visit_rows(|row| {
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
    Ok(stats.rows_emitted)
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
    projection: Option<&Projection>,
) -> Result<SchemaRef> {
    let Some(catalog) = catalog else {
        return Ok(schema);
    };

    // Pair each output field with its source column. Under projection the schema is a
    // subset/reorder of the dataset, so the field's position is NOT the source column index —
    // `written_columns` resolves that mapping (and is identity without projection).
    let columns = written_columns(dataset, projection);
    let fields = attach_value_labels(schema.fields(), &columns, |format_name| {
        catalog.label_set_for_format(format_name)
    })?;

    let metadata = schema.metadata().clone();
    Ok(Arc::new(Schema::new(fields).with_metadata(metadata)))
}

/// Attach `sas.value_labels` metadata to each output field whose source column has a format
/// that `lookup` resolves to a value-label set.
///
/// `fields` and `columns` are paired **positionally**, so `columns` MUST already be in output
/// order (use [`written_columns`]) — not raw dataset order. Pairing by field position against
/// raw dataset columns is exactly the projection bug this indirection prevents.
fn attach_value_labels<'a>(
    fields: &Fields,
    columns: &[&ColumnMeta],
    lookup: impl Fn(&str) -> Option<&'a LabelSet>,
) -> Result<Vec<Arc<Field>>> {
    let mut out = Vec::with_capacity(fields.len());
    for (field, column) in fields.iter().zip(columns) {
        let mut field = field.as_ref().clone();
        if let Some(format_name) = column.format.as_deref()
            && let Some(label_set) = lookup(format_name)
        {
            let mut metadata = field.metadata().clone();
            metadata.insert(
                "sas.value_labels".to_string(),
                serde_json::to_string(label_set)?,
            );
            field = field.with_metadata(metadata);
        }
        out.push(Arc::new(field));
    }
    Ok(out)
}

/// Serialize SAS dataset/column metadata (name, label, kind, format, width) for the columns
/// actually written, in output order — so the payload stays correct under projection.
fn sas_metadata_json(dataset: &Dataset, projection: Option<&Projection>) -> Result<String> {
    let columns = written_columns(dataset, projection);
    let payload = DatasetMetaJson::new(dataset, &columns);
    Ok(serde_json::to_string(&payload)?)
}

/// The columns actually written, in output order: the projected columns when a projection is
/// set (resolved back to their full `ColumnMeta` via the source index), otherwise every column.
fn written_columns<'a>(
    dataset: &'a Dataset,
    projection: Option<&Projection>,
) -> Vec<&'a ColumnMeta> {
    projection.map_or_else(
        || dataset.columns().iter().collect(),
        |projection| {
            projection
                .columns()
                .iter()
                .filter_map(|column| dataset.columns().get(column.index))
                .collect()
        },
    )
}

/// The SAS epoch (`1960-01-01`) as a date and as a midnight datetime, computed once per export.
const fn sas_epochs() -> (NaiveDate, NaiveDateTime) {
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

#[cfg(test)]
mod tests {
    use super::{apply_column_encodings, attach_value_labels, resolve_compression};
    use crate::catalog::{LabelSet, ValueKey, ValueLabel, ValueType};
    use crate::cli::CompressionCodec;
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
    use parquet::basic::{Compression, Encoding};
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::ColumnPath;
    use sas7bdat::{ColumnMeta, LogicalType};

    #[test]
    fn resolve_compression_maps_each_codec() {
        assert!(matches!(
            resolve_compression(CompressionCodec::Zstd),
            Compression::ZSTD(_)
        ));
        assert_eq!(
            resolve_compression(CompressionCodec::Lz4),
            Compression::LZ4_RAW
        );
        assert_eq!(
            resolve_compression(CompressionCodec::Snappy),
            Compression::SNAPPY
        );
        assert_eq!(
            resolve_compression(CompressionCodec::None),
            Compression::UNCOMPRESSED
        );
    }

    #[test]
    fn column_encodings_target_floats_and_temporals_only() {
        let schema = Schema::new(vec![
            Field::new("flt", DataType::Float64, false),
            Field::new("dt", DataType::Date32, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("txt", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
        ]);
        let props = apply_column_encodings(WriterProperties::builder(), &schema).build();
        // Set as the per-column fallback encoding; dictionary stays enabled globally.
        assert_eq!(
            props.encoding(&ColumnPath::from("flt")),
            Some(Encoding::BYTE_STREAM_SPLIT)
        );
        assert_eq!(
            props.encoding(&ColumnPath::from("dt")),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert_eq!(
            props.encoding(&ColumnPath::from("ts")),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        // Strings and integers keep parquet-rs defaults.
        assert_eq!(props.encoding(&ColumnPath::from("txt")), None);
        assert_eq!(props.encoding(&ColumnPath::from("int")), None);
        // Dictionary is still on, so low-cardinality columns use it rather than the fallback.
        assert!(props.dictionary_enabled(&ColumnPath::from("flt")));
    }

    fn column(index: usize, name: &str, format: &str) -> ColumnMeta {
        ColumnMeta {
            index,
            name: name.to_owned(),
            logical_type: LogicalType::String,
            physical_width: 1,
            offset: 0,
            label: None,
            format: Some(format.to_owned()),
        }
    }

    fn label_set(name: &str, label: &str) -> LabelSet {
        LabelSet {
            name: name.to_owned(),
            value_type: ValueType::String,
            labels: vec![ValueLabel {
                key: ValueKey::String("1".to_owned()),
                label: label.to_owned(),
            }],
        }
    }

    // Regression: under a reordered column projection the output field at position i does NOT
    // correspond to dataset column i. Labels must follow the (output-ordered) source columns,
    // not the field position. Mirrors `convert --catalog --columns SEXB,SEXA` on a file laid
    // out as [ID(0), SEXA(1, fmt $A), SEXB(2, fmt $B)].
    #[test]
    fn value_labels_follow_source_columns_not_field_position() {
        let fields = Fields::from(vec![
            Field::new("SEXB", DataType::Utf8, true),
            Field::new("SEXA", DataType::Utf8, true),
        ]);
        // Output order, as `written_columns` would resolve it: SEXB first (source index 2).
        let first_col = column(2, "SEXB", "$B");
        let second_col = column(1, "SEXA", "$A");
        let columns = [&first_col, &second_col];

        let labels_b = label_set("$B", "B-Male");
        let labels_a = label_set("$A", "A-Male");
        let lookup = |format: &str| match format {
            "$B" => Some(&labels_b),
            "$A" => Some(&labels_a),
            _ => None,
        };

        let out = attach_value_labels(&fields, &columns, lookup).expect("attach");
        let labels_of = |name: &str| -> String {
            out.iter()
                .find(|f| f.name() == name)
                .and_then(|f| f.metadata().get("sas.value_labels").cloned())
                .unwrap_or_default()
        };

        assert!(
            labels_of("SEXB").contains("B-Male"),
            "SEXB must get $B labels"
        );
        assert!(
            labels_of("SEXA").contains("A-Male"),
            "SEXA must get $A labels"
        );
        // And not cross-contaminated.
        assert!(!labels_of("SEXB").contains("A-Male"));
        assert!(!labels_of("SEXA").contains("B-Male"));
    }

    // A column with a format unknown to the catalog (or no format) gets no metadata.
    #[test]
    fn unmatched_format_attaches_no_metadata() {
        let fields = Fields::from(vec![Field::new("X", DataType::Utf8, true)]);
        let col = column(0, "X", "$NOPE");
        let out = attach_value_labels(&fields, &[&col], |_| None).expect("attach");
        assert!(out[0].metadata().get("sas.value_labels").is_none());
    }
}
