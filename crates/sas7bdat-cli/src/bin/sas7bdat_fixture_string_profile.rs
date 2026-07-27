use sas7bdat::{Dataset, LogicalType, RowIndex, RowSelection};
use sas7bdat_cli::{exit_code, next_parsed, next_value};
use serde::Serialize;
use std::{collections::BTreeMap, env, path::PathBuf, process::ExitCode};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Serialize)]
struct StringProfileOutput {
    fixture: String,
    sample_rows: usize,
    total_string_columns: usize,
    total_string_cells: u64,
    width_buckets: Vec<WidthBucket>,
    top_columns_by_non_empty: Vec<ColumnStringStats>,
    top_columns_by_empty: Vec<ColumnStringStats>,
}

#[derive(Debug, Clone, Serialize)]
struct WidthBucket {
    width: u32,
    columns: usize,
    sampled_cells: u64,
    empty_cells: u64,
    non_empty_cells: u64,
    ascii_cells: u64,
    total_trimmed_len: u64,
    max_trimmed_len: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ColumnStringStats {
    index: usize,
    name: String,
    width: u32,
    sampled_cells: u64,
    empty_cells: u64,
    non_empty_cells: u64,
    ascii_cells: u64,
    total_trimmed_len: u64,
    max_trimmed_len: u64,
}

impl ColumnStringStats {
    #[allow(clippy::cast_precision_loss)]
    fn avg_trimmed_len(&self) -> f64 {
        if self.sampled_cells == 0 {
            0.0
        } else {
            self.total_trimmed_len as f64 / self.sampled_cells as f64
        }
    }
}

#[allow(clippy::cast_precision_loss)]
#[derive(Debug, Serialize)]
struct PrintableColumnStringStats {
    index: usize,
    name: String,
    width: u32,
    sampled_cells: u64,
    empty_cells: u64,
    non_empty_cells: u64,
    empty_ratio: f64,
    ascii_ratio: f64,
    avg_trimmed_len: f64,
    max_trimmed_len: u64,
}

#[allow(clippy::cast_precision_loss)]
#[derive(Debug, Serialize)]
struct PrintableWidthBucket {
    width: u32,
    columns: usize,
    sampled_cells: u64,
    empty_cells: u64,
    non_empty_cells: u64,
    empty_ratio: f64,
    ascii_ratio: f64,
    avg_trimmed_len: f64,
    max_trimmed_len: u64,
}

fn main() -> ExitCode {
    exit_code(run())
}

#[allow(clippy::too_many_lines)]
fn run() -> std::result::Result<(), String> {
    let mut args = env::args_os().skip(1);
    let mut fixture: Option<PathBuf> = None;
    let mut sample_rows = 2048usize;
    let mut top = 20usize;

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--fixture" => fixture = Some(PathBuf::from(next_value(&mut args, "--fixture")?)),
            "--sample-rows" => {
                sample_rows = next_parsed(&mut args, "--sample-rows")?;
            }
            "--top" => {
                top = next_parsed(&mut args, "--top")?;
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            value => return Err(format!("unexpected argument: {value}")),
        }
    }

    let fixture = fixture.ok_or_else(|| "missing required --fixture".to_owned())?;
    let ds = Dataset::open(&fixture).map_err(|err| err.to_string())?;

    let string_columns: Vec<(usize, String, u32)> = ds
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, column)| column.logical_type == LogicalType::String)
        .map(|(idx, column)| (idx, column.name.clone(), column.physical_width))
        .collect();

    let sample_rows =
        sample_rows.min(usize::try_from(ds.metadata().row_count).unwrap_or(usize::MAX));
    let projection = string_columns
        .iter()
        .fold(ds.projection(), |builder, (idx, _, _)| {
            builder.column_idx(*idx)
        })
        .build()
        .map_err(|err| err.to_string())?;

    let mut per_column: Vec<ColumnStringStats> = string_columns
        .iter()
        .map(|(index, name, width)| ColumnStringStats {
            index: *index,
            name: name.clone(),
            width: *width,
            sampled_cells: 0,
            empty_cells: 0,
            non_empty_cells: 0,
            ascii_cells: 0,
            total_trimmed_len: 0,
            max_trimmed_len: 0,
        })
        .collect();

    if sample_rows > 0 {
        ds.scan()
            .with_projection(&projection)
            .select(RowSelection::Range {
                start: RowIndex(0),
                end: RowIndex(sample_rows as u64),
            })
            .visit_rows(|row| {
                for (stats, cell) in per_column.iter_mut().zip(row.iter()) {
                    if let sas7bdat::CellValue::Str(value) = cell {
                        stats.sampled_cells += 1;
                        let trimmed_len = value.len() as u64;
                        stats.total_trimmed_len += trimmed_len;
                        stats.max_trimmed_len = stats.max_trimmed_len.max(trimmed_len);
                        if value.is_empty() {
                            stats.empty_cells += 1;
                        } else {
                            stats.non_empty_cells += 1;
                        }
                        if value.is_ascii() {
                            stats.ascii_cells += 1;
                        }
                    }
                }
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .map_err(|err| err.to_string())?;
    }

    let mut width_map: BTreeMap<u32, WidthBucket> = BTreeMap::new();
    for stats in &per_column {
        let bucket = width_map.entry(stats.width).or_insert_with(|| WidthBucket {
            width: stats.width,
            columns: 0,
            sampled_cells: 0,
            empty_cells: 0,
            non_empty_cells: 0,
            ascii_cells: 0,
            total_trimmed_len: 0,
            max_trimmed_len: 0,
        });
        bucket.columns += 1;
        bucket.sampled_cells += stats.sampled_cells;
        bucket.empty_cells += stats.empty_cells;
        bucket.non_empty_cells += stats.non_empty_cells;
        bucket.ascii_cells += stats.ascii_cells;
        bucket.total_trimmed_len += stats.total_trimmed_len;
        bucket.max_trimmed_len = bucket.max_trimmed_len.max(stats.max_trimmed_len);
    }

    let mut top_non_empty = per_column.clone();
    top_non_empty.sort_by(|left, right| {
        right
            .non_empty_cells
            .cmp(&left.non_empty_cells)
            .then_with(|| right.total_trimmed_len.cmp(&left.total_trimmed_len))
            .then_with(|| left.index.cmp(&right.index))
    });
    top_non_empty.truncate(top);

    let mut top_empty = per_column.clone();
    top_empty.sort_by(|left, right| {
        right
            .empty_cells
            .cmp(&left.empty_cells)
            .then_with(|| left.index.cmp(&right.index))
    });
    top_empty.truncate(top);

    let output = StringProfileOutput {
        fixture: fixture.display().to_string(),
        sample_rows,
        total_string_columns: per_column.len(),
        total_string_cells: per_column.iter().map(|column| column.sampled_cells).sum(),
        width_buckets: width_map.into_values().collect(),
        top_columns_by_non_empty: top_non_empty,
        top_columns_by_empty: top_empty,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&PrintableOutput::from(output))
            .map_err(|err| err.to_string())?
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct PrintableOutput {
    fixture: String,
    sample_rows: usize,
    total_string_columns: usize,
    total_string_cells: u64,
    width_buckets: Vec<PrintableWidthBucket>,
    top_columns_by_non_empty: Vec<PrintableColumnStringStats>,
    top_columns_by_empty: Vec<PrintableColumnStringStats>,
}

impl From<StringProfileOutput> for PrintableOutput {
    fn from(value: StringProfileOutput) -> Self {
        Self {
            fixture: value.fixture,
            sample_rows: value.sample_rows,
            total_string_columns: value.total_string_columns,
            total_string_cells: value.total_string_cells,
            width_buckets: value
                .width_buckets
                .into_iter()
                .map(|bucket| PrintableWidthBucket {
                    width: bucket.width,
                    columns: bucket.columns,
                    sampled_cells: bucket.sampled_cells,
                    empty_cells: bucket.empty_cells,
                    non_empty_cells: bucket.non_empty_cells,
                    empty_ratio: ratio(bucket.empty_cells, bucket.sampled_cells),
                    ascii_ratio: ratio(bucket.ascii_cells, bucket.sampled_cells),
                    avg_trimmed_len: avg(bucket.total_trimmed_len, bucket.sampled_cells),
                    max_trimmed_len: bucket.max_trimmed_len,
                })
                .collect(),
            top_columns_by_non_empty: value
                .top_columns_by_non_empty
                .into_iter()
                .map(printable_column)
                .collect(),
            top_columns_by_empty: value
                .top_columns_by_empty
                .into_iter()
                .map(printable_column)
                .collect(),
        }
    }
}

fn printable_column(stats: ColumnStringStats) -> PrintableColumnStringStats {
    let avg_trimmed_len = stats.avg_trimmed_len();
    PrintableColumnStringStats {
        index: stats.index,
        name: stats.name,
        width: stats.width,
        sampled_cells: stats.sampled_cells,
        empty_cells: stats.empty_cells,
        non_empty_cells: stats.non_empty_cells,
        empty_ratio: ratio(stats.empty_cells, stats.sampled_cells),
        ascii_ratio: ratio(stats.ascii_cells, stats.sampled_cells),
        avg_trimmed_len,
        max_trimmed_len: stats.max_trimmed_len,
    }
}

#[allow(clippy::cast_precision_loss)]
fn ratio(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 / total as f64
    }
}

#[allow(clippy::cast_precision_loss)]
fn avg(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p sas7bdat-cli --bin sas7bdat-fixture-string-profile -- --fixture PATH [--sample-rows N] [--top N]"
    );
}
