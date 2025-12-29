use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};

use ahash::AHasher;
use clap::Parser;
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedColumnWriter;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{Type, TypePtr};
use sas7bdat::logger::{log_warn, set_log_file};
use sas7bdat::metadata::{DatasetMetadata, Variable, VariableKind};
use sas7bdat::value::Value;
use sas7bdat::SasFile;
use serde_json::json;
use time::{Date, Month, Time};

const BEF_SCHEMA_COLUMNS: &[&str] = &[
    "ADRESSE_ID",
    "AEGTE_ID",
    "ALDER",
    "ANTBOERNF",
    "ANTBOERNH",
    "ANTEFAM",
    "ANTPERSF",
    "ANTPERSH",
    "BETALINGSKOM",
    "BOP_VFRA",
    "CIVST",
    "CIV_VFRA",
    "CPRTJEK",
    "CPRTYPE",
    "E_FAELLE_ID",
    "FAMILIE_ID",
    "FAMILIE_TYPE",
    "FAM_KOEN",
    "FAR_ID",
    "FKIRK",
    "FM_MARK",
    "FOEDREG_KODE",
    "FOED_DAG",
    "FOERSTE_INDVANDRING",
    "HUSTYPE",
    "IE_TYPE",
    "KOEN",
    "KOM",
    "MOR_ID",
    "OPHOLDMD_DK",
    "OPR_LAND",
    "PLADS",
    "PNR",
    "REFERENCETID",
    "REG",
    "SENESTE_INDVANDRING",
    "STATSB",
    "VAN_VTIL",
];

type AnyError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "bef-scd",
    version,
    about = "Build BEF SCD tables and indices from SAS7BDAT snapshots"
)]
struct Cli {
    /// Root directory containing BEF SAS7BDAT snapshots.
    #[arg(long = "bef-path", value_name = "DIR")]
    bef_path: PathBuf,

    /// Output directory for parquet outputs.
    #[arg(long = "output-dir", value_name = "DIR")]
    output_dir: PathBuf,

    /// Datacens date used as open-ended `valid_to` (YYYY-MM-DD).
    #[arg(long, default_value = "9999-12-31", value_name = "YYYY-MM-DD")]
    datacens: String,

    /// Limit the number of files processed (useful for testing).
    #[arg(long, value_name = "N")]
    limit: Option<usize>,

    /// Write warnings and errors to a log file in addition to stderr.
    #[arg(long, value_name = "FILE")]
    log_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeriodKind {
    Year,
    Month,
}

impl PeriodKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Month => "month",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputType {
    Utf8,
    Double,
    Date32,
    TimestampMicros,
    TimeMicros,
}

#[derive(Clone, Debug)]
struct FileInfo {
    path: PathBuf,
    ref_date: Date,
    period_kind: PeriodKind,
    schema_hash: String,
    columns_present: Vec<bool>,
    columns_present_count: usize,
    columns_missing_count: usize,
    column_mapping: Vec<Option<usize>>,
    file_id: i32,
    row_count: u64,
}

#[derive(Clone, Debug)]
struct PnrState {
    values: Vec<Option<Value<'static>>>,
    columns_present: Vec<bool>,
    valid_from: Date,
    source_file_id: i32,
}

#[derive(Clone, Debug)]
enum CellValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Double(f64),
    Utf8(String),
}

enum ColumnValues {
    Bool(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Double(Vec<f64>),
    ByteArray(Vec<ByteArray>),
}

struct ColumnBuffer {
    name: String,
    physical: PhysicalType,
    logical: Option<LogicalType>,
    values: ColumnValues,
    def_levels: Vec<i16>,
}

impl ColumnBuffer {
    const fn new(name: String, physical: PhysicalType, logical: Option<LogicalType>) -> Self {
        let values = match physical {
            PhysicalType::BOOLEAN => ColumnValues::Bool(Vec::new()),
            PhysicalType::INT32 => ColumnValues::Int32(Vec::new()),
            PhysicalType::INT64 => ColumnValues::Int64(Vec::new()),
            PhysicalType::DOUBLE => ColumnValues::Double(Vec::new()),
            _ => ColumnValues::ByteArray(Vec::new()),
        };
        Self {
            name,
            physical,
            logical,
            values,
            def_levels: Vec::new(),
        }
    }

    fn push_null(&mut self) {
        self.def_levels.push(0);
    }

    fn push_value(&mut self, value: CellValue) -> Result<(), AnyError> {
        self.def_levels.push(1);
        match (&mut self.values, value) {
            (ColumnValues::Bool(values), CellValue::Bool(v)) => values.push(v),
            (ColumnValues::Int32(values), CellValue::Int32(v)) => values.push(v),
            (ColumnValues::Int64(values), CellValue::Int64(v)) => values.push(v),
            (ColumnValues::Double(values), CellValue::Double(v)) => values.push(v),
            (ColumnValues::ByteArray(values), CellValue::Utf8(v)) => {
                values.push(ByteArray::from(v.into_bytes()));
            }
            _ => {
                return Err(format!(
                    "column '{}' value type mismatch for {:?}",
                    self.name, self.physical
                )
                .into());
            }
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.def_levels.clear();
        match &mut self.values {
            ColumnValues::Bool(values) => values.clear(),
            ColumnValues::Int32(values) => values.clear(),
            ColumnValues::Int64(values) => values.clear(),
            ColumnValues::Double(values) => values.clear(),
            ColumnValues::ByteArray(values) => values.clear(),
        }
    }
}

struct ParquetTableWriter<W: Write + Send> {
    writer: SerializedFileWriter<W>,
    columns: Vec<ColumnBuffer>,
    row_group_size: usize,
    rows_buffered: usize,
}

impl<W: Write + Send> ParquetTableWriter<W> {
    fn new(writer: W, columns: Vec<ColumnBuffer>, row_group_size: usize) -> Result<Self, AnyError> {
        let fields: Vec<TypePtr> = columns
            .iter()
            .map(|column| {
                let field = Type::primitive_type_builder(&column.name, column.physical)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_logical_type(column.logical.clone())
                    .build()?;
                Ok(std::sync::Arc::new(field))
            })
            .collect::<Result<Vec<_>, parquet::errors::ParquetError>>()?;

        let schema = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?;
        let props = WriterProperties::builder().build();
        let writer =
            SerializedFileWriter::new(writer, std::sync::Arc::new(schema), props.into())?;
        Ok(Self {
            writer,
            columns,
            row_group_size: row_group_size.max(1),
            rows_buffered: 0,
        })
    }

    fn write_row(&mut self, row: &[Option<CellValue>]) -> Result<(), AnyError> {
        if row.len() != self.columns.len() {
            return Err(format!(
                "row length {} does not match column count {}",
                row.len(),
                self.columns.len()
            )
            .into());
        }

        for (column, value) in self.columns.iter_mut().zip(row.iter()) {
            match value {
                Some(cell) => column.push_value(cell.clone())?,
                None => column.push_null(),
            }
        }

        self.rows_buffered += 1;
        if self.rows_buffered >= self.row_group_size {
            self.flush_row_group()?;
        }
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<(), AnyError> {
        if self.rows_buffered == 0 {
            return Ok(());
        }

        let mut row_group_writer = self.writer.next_row_group()?;
        for column in &mut self.columns {
            let Some(mut col_writer) = row_group_writer.next_column()? else {
                return Err("unexpected end of row group columns".into());
            };
            Self::write_column(&mut col_writer, column)?;
            col_writer.close()?;
        }
        row_group_writer.close()?;

        for column in &mut self.columns {
            column.clear();
        }
        self.rows_buffered = 0;
        Ok(())
    }

    fn write_column(
        col_writer: &mut SerializedColumnWriter<'_>,
        column: &ColumnBuffer,
    ) -> Result<(), AnyError> {
        match (col_writer.untyped(), &column.values) {
            (ColumnWriter::BoolColumnWriter(writer), ColumnValues::Bool(values)) => {
                writer.write_batch(values, Some(&column.def_levels), None)?;
            }
            (ColumnWriter::Int32ColumnWriter(writer), ColumnValues::Int32(values)) => {
                writer.write_batch(values, Some(&column.def_levels), None)?;
            }
            (ColumnWriter::Int64ColumnWriter(writer), ColumnValues::Int64(values)) => {
                writer.write_batch(values, Some(&column.def_levels), None)?;
            }
            (ColumnWriter::DoubleColumnWriter(writer), ColumnValues::Double(values)) => {
                writer.write_batch(values, Some(&column.def_levels), None)?;
            }
            (ColumnWriter::ByteArrayColumnWriter(writer), ColumnValues::ByteArray(values)) => {
                writer.write_batch(values, Some(&column.def_levels), None)?;
            }
            _ => {
                return Err(format!(
                    "column '{}' writer type mismatch",
                    column.name
                )
                .into());
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<W, AnyError> {
        self.flush_row_group()?;
        Ok(self.writer.into_inner()?)
    }
}

struct PreparedInputs {
    datacens: Date,
    canonical_columns: Vec<String>,
    pnr_index: usize,
    file_infos: Vec<FileInfo>,
    resolved_column_types: Vec<OutputType>,
}

struct Writers {
    scd: ParquetTableWriter<File>,
    index: ParquetTableWriter<File>,
    coverage: ParquetTableWriter<File>,
}

impl Writers {
    fn finish(self) -> Result<(), AnyError> {
        self.scd.finish()?;
        self.index.finish()?;
        self.coverage.finish()?;
        Ok(())
    }
}

struct FileProcessStats {
    row_count: u64,
    scd_rows: u64,
}

fn main() -> Result<(), AnyError> {
    let cli = Cli::parse();

    if let Some(path) = &cli.log_file {
        set_log_file(path)?;
    }

    fs::create_dir_all(&cli.output_dir)?;
    report_event(
        "start",
        &[
            ("input", json!(cli.bef_path.display().to_string())),
            ("output", json!(cli.output_dir.display().to_string())),
        ],
    );
    let mut prepared = prepare_inputs(&cli)?;
    let mut writers = init_writers(
        &cli.output_dir,
        &prepared.canonical_columns,
        &prepared.resolved_column_types,
        prepared.pnr_index,
    )?;
    process_files(
        &mut writers,
        &mut prepared.file_infos,
        &prepared.canonical_columns,
        &prepared.resolved_column_types,
        prepared.pnr_index,
        prepared.datacens,
    )?;
    writers.finish()
}

fn prepare_inputs(cli: &Cli) -> Result<PreparedInputs, AnyError> {
    let datacens = parse_date(&cli.datacens)?;
    let canonical_columns: Vec<String> =
        BEF_SCHEMA_COLUMNS.iter().map(|name| (*name).to_owned()).collect();
    report_event("schema_loaded", &[("column_count", json!(canonical_columns.len()))]);
    let pnr_index = canonical_columns
        .iter()
        .position(|name| name == "PNR")
        .ok_or("PNR column not found in BEF schema")?;

    let mut column_types: Vec<Option<OutputType>> = vec![None; canonical_columns.len()];
    column_types[pnr_index] = Some(OutputType::Utf8);

    let mut files = discover_bef_files(&cli.bef_path)?;
    if let Some(limit) = cli.limit {
        files.truncate(limit);
    }
    report_event(
        "scan_complete",
        &[
            ("root", json!(cli.bef_path.display().to_string())),
            ("files_selected", json!(files.len())),
            ("limit", json!(cli.limit)),
        ],
    );

    let mut file_infos = Vec::with_capacity(files.len());
    for path in files {
        report_event(
            "inspect_file_start",
            &[("path", json!(path.display().to_string()))],
        );
        let ref_info = parse_ref_date_from_path(&path)?;
        let sas = SasFile::open(&path)?;
        let metadata = sas.metadata().clone();

        let (columns_present, column_mapping) =
            map_columns(&metadata, &canonical_columns, &path)?;
        let columns_present_count = columns_present.iter().filter(|present| **present).count();
        let columns_missing_count = canonical_columns
            .len()
            .saturating_sub(columns_present_count);
        if !columns_present
            .get(pnr_index)
            .copied()
            .unwrap_or(false)
        {
            return Err(format!("PNR column missing in {}", path.display()).into());
        }
        update_column_types(&metadata, &column_mapping, &canonical_columns, &mut column_types);
        let schema_hash = compute_schema_hash(&metadata);
        report_event(
            "inspect_file_done",
            &[
                ("path", json!(path.display().to_string())),
                ("ref_date", json!(ref_info.0.to_string())),
                ("period_kind", json!(ref_info.1.as_str())),
                ("columns_present", json!(columns_present_count)),
                ("columns_missing", json!(columns_missing_count)),
                ("schema_hash", json!(schema_hash)),
            ],
        );

        file_infos.push(FileInfo {
            path,
            ref_date: ref_info.0,
            period_kind: ref_info.1,
            schema_hash,
            columns_present,
            columns_present_count,
            columns_missing_count,
            column_mapping,
            file_id: 0,
            row_count: 0,
        });
    }

    file_infos.sort_by(|a, b| a.ref_date.cmp(&b.ref_date).then_with(|| a.path.cmp(&b.path)));
    for (idx, info) in file_infos.iter_mut().enumerate() {
        info.file_id = i32::try_from(idx + 1)?;
    }

    let resolved_column_types: Vec<OutputType> = column_types
        .iter()
        .map(|ty| ty.unwrap_or(OutputType::Utf8))
        .collect();

    Ok(PreparedInputs {
        datacens,
        canonical_columns,
        pnr_index,
        file_infos,
        resolved_column_types,
    })
}

fn init_writers(
    output_dir: &Path,
    canonical_columns: &[String],
    column_types: &[OutputType],
    pnr_index: usize,
) -> Result<Writers, AnyError> {
    let scd_columns = build_scd_columns(canonical_columns, column_types, pnr_index);
    let scd_path = output_dir.join("bef_scd.parquet");
    let scd_file = File::create(&scd_path)?;
    let scd_writer = ParquetTableWriter::new(scd_file, scd_columns, 16_384)?;

    let index_columns = build_file_index_columns();
    let index_path = output_dir.join("bef_file_index.parquet");
    let index_file = File::create(&index_path)?;
    let index_writer = ParquetTableWriter::new(index_file, index_columns, 1024)?;

    let coverage_columns = build_schema_coverage_columns();
    let coverage_path = output_dir.join("bef_schema_coverage.parquet");
    let coverage_file = File::create(&coverage_path)?;
    let coverage_writer = ParquetTableWriter::new(coverage_file, coverage_columns, 4096)?;

    Ok(Writers {
        scd: scd_writer,
        index: index_writer,
        coverage: coverage_writer,
    })
}

fn process_files(
    writers: &mut Writers,
    file_infos: &mut [FileInfo],
    canonical_columns: &[String],
    column_types: &[OutputType],
    pnr_index: usize,
    datacens: Date,
) -> Result<(), AnyError> {
    write_schema_coverage_all(&mut writers.coverage, file_infos, canonical_columns)?;

    let mut state_map: HashMap<String, PnrState> = HashMap::new();
    let mut total_rows: u64 = 0;
    let mut scd_rows: u64 = 0;

    for info in file_infos.iter_mut() {
        let stats = process_one_file(
            writers,
            info,
            canonical_columns,
            column_types,
            pnr_index,
            &mut state_map,
        )?;
        total_rows = total_rows.saturating_add(stats.row_count);
        scd_rows = scd_rows.saturating_add(stats.scd_rows);
    }

    let distinct_pnr = state_map.len();
    scd_rows = scd_rows.saturating_add(flush_state_map(
        &mut writers.scd,
        state_map,
        datacens,
        column_types,
        pnr_index,
    )?);

    report_event(
        "summary",
        &[
            ("files_processed", json!(file_infos.len())),
            ("rows_total", json!(total_rows)),
            ("distinct_pnr", json!(distinct_pnr)),
            ("scd_rows", json!(scd_rows)),
        ],
    );

    Ok(())
}

fn write_schema_coverage_all(
    writer: &mut ParquetTableWriter<File>,
    file_infos: &[FileInfo],
    canonical_columns: &[String],
) -> Result<(), AnyError> {
    for info in file_infos {
        write_schema_coverage(writer, info.file_id, canonical_columns, &info.columns_present)?;
    }
    Ok(())
}

fn process_one_file(
    writers: &mut Writers,
    info: &mut FileInfo,
    canonical_columns: &[String],
    column_types: &[OutputType],
    pnr_index: usize,
    state_map: &mut HashMap<String, PnrState>,
) -> Result<FileProcessStats, AnyError> {
    report_event(
        "process_file_start",
        &[
            ("file_id", json!(info.file_id)),
            ("path", json!(info.path.display().to_string())),
            ("ref_date", json!(info.ref_date.to_string())),
        ],
    );

    let mut sas = SasFile::open(&info.path)?;
    let mut rows = sas.rows()?;
    let mut row_count: u64 = 0;
    let mut scd_rows: u64 = 0;

    while let Some(row) = rows.try_next()? {
        row_count += 1;
        let mut values: Vec<Option<Value<'static>>> = vec![None; canonical_columns.len()];
        for (idx, value) in row.into_iter().enumerate() {
            if let Some(Some(target)) = info.column_mapping.get(idx) {
                values[*target] = Some(value.into_owned());
            }
        }

        let pnr_value = values
            .get(pnr_index)
            .ok_or("PNR index out of bounds")?
            .clone()
            .ok_or_else(|| format!("missing PNR value in {}", info.path.display()))?;
        let pnr_key = value_to_string(&pnr_value)
            .ok_or_else(|| format!("invalid PNR value in {}", info.path.display()))?;

        match state_map.get_mut(&pnr_key) {
            Some(state) => {
                let prev_sig = signature(
                    &state.values,
                    &state.columns_present,
                    &info.columns_present,
                );
                let curr_sig = signature(&values, &info.columns_present, &state.columns_present);
                let has_new = has_new_columns(&info.columns_present, &state.columns_present);
                if prev_sig != curr_sig || has_new {
                    write_scd_row(
                        &mut writers.scd,
                        &pnr_key,
                        state,
                        info.ref_date,
                        column_types,
                        pnr_index,
                    )?;
                    scd_rows += 1;
                    *state = PnrState {
                        values,
                        columns_present: info.columns_present.clone(),
                        valid_from: info.ref_date,
                        source_file_id: info.file_id,
                    };
                }
            }
            None => {
                state_map.insert(
                    pnr_key,
                    PnrState {
                        values,
                        columns_present: info.columns_present.clone(),
                        valid_from: info.ref_date,
                        source_file_id: info.file_id,
                    },
                );
            }
        }
    }

    info.row_count = row_count;
    write_file_index(&mut writers.index, info, canonical_columns)?;
    report_event(
        "process_file_done",
        &[
            ("file_id", json!(info.file_id)),
            ("path", json!(info.path.display().to_string())),
            ("rows", json!(row_count)),
            ("columns_present", json!(info.columns_present_count)),
            ("columns_missing", json!(info.columns_missing_count)),
        ],
    );

    Ok(FileProcessStats { row_count, scd_rows })
}

fn flush_state_map(
    writer: &mut ParquetTableWriter<File>,
    state_map: HashMap<String, PnrState>,
    datacens: Date,
    column_types: &[OutputType],
    pnr_index: usize,
) -> Result<u64, AnyError> {
    let mut scd_rows: u64 = 0;
    for (pnr_key, state) in state_map {
        write_scd_row(
            writer,
            &pnr_key,
            &state,
            datacens,
            column_types,
            pnr_index,
        )?;
        scd_rows += 1;
    }
    Ok(scd_rows)
}

fn build_file_index_columns() -> Vec<ColumnBuffer> {
    vec![
        ColumnBuffer::new("file_id".to_owned(), PhysicalType::INT32, None),
        ColumnBuffer::new("path".to_owned(), PhysicalType::BYTE_ARRAY, Some(LogicalType::String)),
        ColumnBuffer::new("ref_date".to_owned(), PhysicalType::INT32, Some(LogicalType::Date)),
        ColumnBuffer::new(
            "period_kind".to_owned(),
            PhysicalType::BYTE_ARRAY,
            Some(LogicalType::String),
        ),
        ColumnBuffer::new(
            "schema_hash".to_owned(),
            PhysicalType::BYTE_ARRAY,
            Some(LogicalType::String),
        ),
        ColumnBuffer::new(
            "columns_present".to_owned(),
            PhysicalType::BYTE_ARRAY,
            Some(LogicalType::String),
        ),
        ColumnBuffer::new("row_count".to_owned(), PhysicalType::INT64, None),
    ]
}

fn build_schema_coverage_columns() -> Vec<ColumnBuffer> {
    vec![
        ColumnBuffer::new("file_id".to_owned(), PhysicalType::INT32, None),
        ColumnBuffer::new(
            "column_name".to_owned(),
            PhysicalType::BYTE_ARRAY,
            Some(LogicalType::String),
        ),
        ColumnBuffer::new("present".to_owned(), PhysicalType::BOOLEAN, None),
    ]
}

fn build_scd_columns(
    canonical_columns: &[String],
    column_types: &[OutputType],
    pnr_index: usize,
) -> Vec<ColumnBuffer> {
    let mut columns = Vec::with_capacity(canonical_columns.len() + 4);
    columns.push(ColumnBuffer::new(
        "pnr".to_owned(),
        PhysicalType::BYTE_ARRAY,
        Some(LogicalType::String),
    ));
    columns.push(ColumnBuffer::new(
        "valid_from".to_owned(),
        PhysicalType::INT32,
        Some(LogicalType::Date),
    ));
    columns.push(ColumnBuffer::new(
        "valid_to".to_owned(),
        PhysicalType::INT32,
        Some(LogicalType::Date),
    ));

    for (idx, name) in canonical_columns.iter().enumerate() {
        if idx == pnr_index {
            continue;
        }
        let output_type = column_types
            .get(idx)
            .copied()
            .unwrap_or(OutputType::Utf8);
        let (physical, logical) = output_type_to_parquet(output_type);
        columns.push(ColumnBuffer::new(name.to_owned(), physical, logical));
    }

    columns.push(ColumnBuffer::new("source_file_id".to_owned(), PhysicalType::INT32, None));
    columns
}

const fn output_type_to_parquet(output: OutputType) -> (PhysicalType, Option<LogicalType>) {
    match output {
        OutputType::Utf8 => (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)),
        OutputType::Double => (PhysicalType::DOUBLE, None),
        OutputType::Date32 => (PhysicalType::INT32, Some(LogicalType::Date)),
        OutputType::TimestampMicros => (
            PhysicalType::INT64,
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::MICROS,
            }),
        ),
        OutputType::TimeMicros => (
            PhysicalType::INT64,
            Some(LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::MICROS,
            }),
        ),
    }
}

fn write_schema_coverage(
    writer: &mut ParquetTableWriter<File>,
    file_id: i32,
    columns: &[String],
    present: &[bool],
) -> Result<(), AnyError> {
    for (name, present) in columns.iter().zip(present.iter()) {
        writer.write_row(&[
            Some(CellValue::Int32(file_id)),
            Some(CellValue::Utf8(name.clone())),
            Some(CellValue::Bool(*present)),
        ])?;
    }
    Ok(())
}

fn write_file_index(
    writer: &mut ParquetTableWriter<File>,
    info: &FileInfo,
    columns: &[String],
) -> Result<(), AnyError> {
    let columns_present: Vec<&String> = columns
        .iter()
        .zip(info.columns_present.iter())
        .filter_map(|(name, present)| present.then_some(name))
        .collect();
    let columns_present_json = serde_json::to_string(&columns_present)?;
    let row_count = i64::try_from(info.row_count)
        .map_err(|_| "row_count out of range for i64")?;
    writer.write_row(&[
        Some(CellValue::Int32(info.file_id)),
        Some(CellValue::Utf8(info.path.display().to_string())),
        Some(CellValue::Int32(date_to_days(info.ref_date)?)),
        Some(CellValue::Utf8(info.period_kind.as_str().to_owned())),
        Some(CellValue::Utf8(info.schema_hash.clone())),
        Some(CellValue::Utf8(columns_present_json)),
        Some(CellValue::Int64(row_count)),
    ])?;
    Ok(())
}

fn write_scd_row(
    writer: &mut ParquetTableWriter<File>,
    pnr_key: &str,
    state: &PnrState,
    valid_to: Date,
    column_types: &[OutputType],
    pnr_index: usize,
) -> Result<(), AnyError> {
    let mut row = Vec::with_capacity(column_types.len() + 4);
    row.push(Some(CellValue::Utf8(pnr_key.to_owned())));
    row.push(Some(CellValue::Int32(date_to_days(state.valid_from)?)));
    row.push(Some(CellValue::Int32(date_to_days(valid_to)?)));

    for (idx, value) in state.values.iter().enumerate() {
        if idx == pnr_index {
            continue;
        }
        let output_type = column_types
            .get(idx)
            .copied()
            .unwrap_or(OutputType::Utf8);
        row.push(sas_value_to_cell(value.as_ref(), output_type, idx));
    }

    row.push(Some(CellValue::Int32(state.source_file_id)));
    writer.write_row(&row)?;
    Ok(())
}

fn sas_value_to_cell(
    value: Option<&Value<'static>>,
    output_type: OutputType,
    _column_index: usize,
) -> Option<CellValue> {
    let value = value?;

    match output_type {
        OutputType::Utf8 => value_to_string(value).map(CellValue::Utf8),
        OutputType::Double => value_to_double(value).map(CellValue::Double),
        OutputType::Date32 => value_to_date32(value).map(CellValue::Int32),
        OutputType::TimestampMicros => value_to_timestamp_micros(value).map(CellValue::Int64),
        OutputType::TimeMicros => value_to_time_micros(value).map(CellValue::Int64),
    }
}

fn value_to_double(value: &Value<'_>) -> Option<f64> {
    match value {
        Value::Float(v) => Some(*v),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => v.to_string().parse::<f64>().ok(),
        Value::NumericString(v) | Value::Str(v) => v.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_date32(value: &Value<'_>) -> Option<i32> {
    match value {
        Value::Date(date) => date_to_days(date.date()).ok(),
        Value::DateTime(datetime) => date_to_days(datetime.date()).ok(),
        _ => None,
    }
}

fn value_to_timestamp_micros(value: &Value<'_>) -> Option<i64> {
    match value {
        Value::DateTime(datetime) => {
            let seconds = datetime.unix_timestamp();
            let micros = i64::from(datetime.microsecond());
            Some(seconds.saturating_mul(1_000_000).saturating_add(micros))
        }
        _ => None,
    }
}

fn value_to_time_micros(value: &Value<'_>) -> Option<i64> {
    match value {
        Value::Time(duration) => i64::try_from(duration.whole_microseconds()).ok(),
        _ => None,
    }
}

fn value_to_string(value: &Value<'_>) -> Option<String> {
    match value {
        Value::Str(v) | Value::NumericString(v) => Some(v.trim().to_owned()),
        Value::Bytes(v) => Some(String::from_utf8_lossy(v).trim().to_owned()),
        Value::Float(v) => {
            if v.is_finite() {
                Some(v.to_string())
            } else {
                None
            }
        }
        Value::Int32(v) => Some(v.to_string()),
        Value::Int64(v) => Some(v.to_string()),
        Value::Date(date) => Some(date.date().to_string()),
        Value::DateTime(datetime) => Some(datetime.to_string()),
        Value::Time(time) => Some((Time::MIDNIGHT + *time).to_string()),
        Value::Missing(_) => None,
    }
}

fn signature(values: &[Option<Value<'static>>], left: &[bool], right: &[bool]) -> Option<u64> {
    let mut hasher = AHasher::default();
    let mut matched = 0usize;
    for (idx, value) in values.iter().enumerate() {
        if !left.get(idx).copied().unwrap_or(false) || !right.get(idx).copied().unwrap_or(false) {
            continue;
        }
        matched += 1;
        idx.hash(&mut hasher);
        match value {
            Some(value) => hash_value(value, &mut hasher),
            None => 0_u8.hash(&mut hasher),
        }
    }
    if matched == 0 {
        None
    } else {
        Some(hasher.finish())
    }
}

fn has_new_columns(current: &[bool], previous: &[bool]) -> bool {
    current
        .iter()
        .zip(previous.iter())
        .any(|(curr, prev)| *curr && !*prev)
}

fn hash_value(value: &Value<'_>, hasher: &mut AHasher) {
    match value {
        Value::Float(v) => v.to_bits().hash(hasher),
        Value::Int32(v) => v.hash(hasher),
        Value::Int64(v) => v.hash(hasher),
        Value::NumericString(v) | Value::Str(v) => v.hash(hasher),
        Value::Bytes(v) => v.hash(hasher),
        Value::Date(value) => value.unix_timestamp().hash(hasher),
        Value::DateTime(value) => {
            value.unix_timestamp().hash(hasher);
            value.nanosecond().hash(hasher);
        }
        Value::Time(value) => value.whole_microseconds().hash(hasher),
        Value::Missing(_) => 1_u8.hash(hasher),
    }
}

fn parse_ref_date_from_path(path: &Path) -> Result<(Date, PeriodKind), AnyError> {
    let file_stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("invalid filename {}", path.display()))?;
    parse_ref_date_from_name(file_stem)
}

fn parse_ref_date_from_name(name: &str) -> Result<(Date, PeriodKind), AnyError> {
    if !name.is_ascii() {
        return Err(format!("filename is not ASCII: {name}").into());
    }

    let bytes = name.as_bytes();
    let mut candidates: Vec<(usize, bool, i32, Option<u8>)> = Vec::new();

    for idx in 0..bytes.len() {
        if idx + 6 <= bytes.len() && bytes[idx..idx + 6].iter().all(u8::is_ascii_digit)
        {
            let year = parse_digits(&bytes[idx..idx + 4])?;
            let month = parse_digits(&bytes[idx + 4..idx + 6])?;
            if (1..=12).contains(&month) {
                let month = u8::try_from(month)?;
                let prefixed = idx >= 3 && bytes[idx - 3..idx].eq_ignore_ascii_case(b"bef");
                candidates.push((idx, prefixed, year, Some(month)));
            }
        }
        if idx + 4 <= bytes.len() && bytes[idx..idx + 4].iter().all(u8::is_ascii_digit)
        {
            let year = parse_digits(&bytes[idx..idx + 4])?;
            let prefixed = idx >= 3 && bytes[idx - 3..idx].eq_ignore_ascii_case(b"bef");
            candidates.push((idx, prefixed, year, None));
        }
    }

    let mut candidates_with_month: Vec<_> = candidates
        .iter()
        .filter(|(_, _, _, month)| month.is_some())
        .copied()
        .collect();
    candidates_with_month.sort_by_key(|(idx, prefixed, _, _)| (!prefixed, *idx));
    if let Some((_idx, _prefixed, year, Some(month))) = candidates_with_month.first().copied() {
        let month = Month::try_from(month)?;
        let day = month.length(year);
        let date = Date::from_calendar_date(year, month, day)?;
        return Ok((date, PeriodKind::Month));
    }

    let mut candidates_year: Vec<_> = candidates
        .iter()
        .filter(|(_, _, _, month)| month.is_none())
        .copied()
        .collect();
    candidates_year.sort_by_key(|(idx, prefixed, _, _)| (!prefixed, *idx));
    if let Some((_idx, _prefixed, year, None)) = candidates_year.first().copied() {
        let date = Date::from_calendar_date(year, Month::December, 31)?;
        return Ok((date, PeriodKind::Year));
    }

    Err(format!("no YYYY or YYYYMM token found in {name}").into())
}

fn parse_digits(slice: &[u8]) -> Result<i32, AnyError> {
    let s = std::str::from_utf8(slice)?;
    Ok(s.parse::<i32>()?)
}

fn parse_date(input: &str) -> Result<Date, AnyError> {
    let parts: Vec<&str> = input.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("invalid date: {input}").into());
    }
    let year: i32 = parts[0].parse()?;
    let month: u8 = parts[1].parse()?;
    let day: u8 = parts[2].parse()?;
    let month = Month::try_from(month)?;
    Ok(Date::from_calendar_date(year, month, day)?)
}

fn date_to_days(date: Date) -> Result<i32, AnyError> {
    let epoch = Date::from_calendar_date(1970, Month::January, 1)?;
    let days = (date - epoch).whole_days();
    i32::try_from(days).map_err(|_| "date out of range for date32".into())
}

fn discover_bef_files(root: &Path) -> Result<Vec<PathBuf>, AnyError> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        let path = entry.path();
        if path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sas7bdat"))
        {
            files.push(path.to_path_buf());
        }
    }
    if files.is_empty() {
        return Err(format!("no sas7bdat files found under {}", root.display()).into());
    }
    Ok(files)
}

fn map_columns(
    metadata: &DatasetMetadata,
    canonical_columns: &[String],
    path: &Path,
) -> Result<(Vec<bool>, Vec<Option<usize>>), AnyError> {
    let mut lookup: HashMap<String, usize> = HashMap::new();
    for (idx, name) in canonical_columns.iter().enumerate() {
        lookup.insert(name.clone(), idx);
    }

    let mut present = vec![false; canonical_columns.len()];
    let mut mapping = vec![None; metadata.variables.len()];

    for (idx, variable) in metadata.variables.iter().enumerate() {
        let normalized = normalize_column_name(&variable.name);
        if let Some(target) = lookup.get(&normalized) {
            present[*target] = true;
            mapping[idx] = Some(*target);
        }
    }

    if !present.iter().any(|p| *p) {
        return Err(format!("no BEF columns found in {}", path.display()).into());
    }

    Ok((present, mapping))
}

fn update_column_types(
    metadata: &DatasetMetadata,
    mapping: &[Option<usize>],
    canonical: &[String],
    types: &mut [Option<OutputType>],
) {
    for (idx, variable) in metadata.variables.iter().enumerate() {
        let Some(target) = mapping.get(idx).copied().flatten() else {
            continue;
        };
        let inferred = infer_output_type(variable, canonical.get(target));
        match types.get_mut(target) {
            Some(slot @ None) => {
                *slot = Some(inferred);
            }
            Some(Some(existing)) if *existing != inferred => {
                let name = canonical
                    .get(target)
                    .map_or("?", String::as_str);
                log_warn(&format!(
                    "column {name} type changed from {existing:?} to {inferred:?}; keeping {existing:?}"
                ));
            }
            _ => {}
        }
    }
}

fn infer_output_type(variable: &Variable, name: Option<&String>) -> OutputType {
    if let Some(name) = name
        && name == "PNR" {
            return OutputType::Utf8;
        }

    match variable.kind {
        VariableKind::Character => OutputType::Utf8,
        VariableKind::Numeric => {
            if let Some(format) = &variable.format
                && let Some(kind) = infer_numeric_kind(&format.name) {
                    return match kind {
                        NumericKind::Date => OutputType::Date32,
                        NumericKind::DateTime => OutputType::TimestampMicros,
                        NumericKind::Time => OutputType::TimeMicros,
                    };
                }
            OutputType::Double
        }
    }
}

fn compute_schema_hash(metadata: &DatasetMetadata) -> String {
    let mut hasher = AHasher::default();
    for variable in &metadata.variables {
        let name = normalize_column_name(&variable.name);
        hasher.write(name.as_bytes());
        let tag = match variable.kind {
            VariableKind::Character => "char",
            VariableKind::Numeric => "num",
        };
        hasher.write(tag.as_bytes());
        if let Some(format) = &variable.format
            && let Some(kind) = infer_numeric_kind(&format.name) {
                let kind = format!("{kind:?}");
                hasher.write(kind.as_bytes());
            }
    }
    format!("{:016x}", hasher.finish())
}

fn normalize_column_name(name: &str) -> String {
    name.trim_end().to_ascii_uppercase()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NumericKind {
    Date,
    DateTime,
    Time,
}

fn infer_numeric_kind(format_name: &str) -> Option<NumericKind> {
    if format_name.is_empty() {
        return None;
    }
    let cleaned = format_name
        .trim()
        .trim_end_matches('.')
        .to_ascii_uppercase();
    if cleaned.is_empty() {
        return None;
    }
    if cleaned.contains("DATETIME")
        || cleaned.ends_with("DT")
        || cleaned.starts_with("E8601DT")
        || cleaned.starts_with("B8601DT")
    {
        return Some(NumericKind::DateTime);
    }
    if cleaned.contains("TIME")
        || cleaned.ends_with("TM")
        || cleaned.starts_with("E8601TM")
        || cleaned.starts_with("HHMM")
    {
        return Some(NumericKind::Time);
    }
    if cleaned.contains("DATE")
        || cleaned.contains("YY")
        || cleaned.contains("MON")
        || cleaned.contains("WEEK")
        || cleaned.contains("YEAR")
        || cleaned.contains("MINGUO")
        || cleaned.ends_with("DA")
        || cleaned.starts_with("E8601DA")
        || cleaned.starts_with("B8601DA")
    {
        return Some(NumericKind::Date);
    }
    None
}

fn report_event(event: &str, fields: &[(&str, serde_json::Value)]) {
    let mut map = serde_json::Map::new();
    map.insert("event".to_owned(), serde_json::Value::String(event.to_owned()));
    for (key, value) in fields {
        map.insert((*key).to_owned(), value.clone());
    }
    println!("{}", serde_json::Value::Object(map));
}

#[cfg(test)]
mod tests {
    use super::{PeriodKind, parse_ref_date_from_name};
    use time::{Date, Month};

    #[test]
    fn parses_year_token() {
        let (date, kind) = parse_ref_date_from_name("bef2018").unwrap();
        assert_eq!(kind, PeriodKind::Year);
        assert_eq!(date, Date::from_calendar_date(2018, Month::December, 31).unwrap());
    }

    #[test]
    fn parses_month_token() {
        let (date, kind) = parse_ref_date_from_name("201907").unwrap();
        assert_eq!(kind, PeriodKind::Month);
        assert_eq!(date, Date::from_calendar_date(2019, Month::July, 31).unwrap());
    }
}
