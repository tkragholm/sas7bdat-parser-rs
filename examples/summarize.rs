#![allow(clippy::pedantic, clippy::nursery)]
use std::env;
use std::error::Error;
use std::path::Path;

use sas7bdat::SasReader;
use sas7bdat::dataset::{Format, VariableKind};
use sas7bdat::CellValue;
use serde::Serialize;

#[derive(Debug, Default, Serialize)]
struct NumericSummary {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ColumnSummary {
    index: usize,
    name: String,
    label: Option<String>,
    format: Option<String>,
    kind: &'static str,
    non_missing: u64,
    missing: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    numeric: Option<NumericSummary>,
}

#[derive(Debug, Serialize)]
struct DatasetSummary {
    row_count: u64,
    columns: Vec<ColumnSummary>,
}

fn variable_kind_name(kind: &VariableKind) -> &'static str {
    match kind {
        VariableKind::Numeric => "numeric",
        VariableKind::Character => "character",
    }
}

fn format_name(format: &Option<Format>) -> Option<String> {
    format.as_ref().map(|fmt| fmt.name.trim().to_owned())
}

fn update_numeric(summary: &mut ColumnSummary, value: f64) {
    if !value.is_finite() {
        summary.missing += 1;
        return;
    }

    summary.non_missing += 1;
    let stats = summary.numeric.get_or_insert_with(NumericSummary::default);
    stats.count += 1;
    stats.sum += value;

    stats.min = Some(match stats.min {
        Some(current) => current.min(value),
        None => value,
    });
    stats.max = Some(match stats.max {
        Some(current) => current.max(value),
        None => value,
    });
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args_os();
    let program = args
        .next()
        .and_then(|os| os.into_string().ok())
        .unwrap_or_else(|| "summarize".to_owned());
    let path = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("usage: {program} <path-to-sas7bdat>");
            std::process::exit(2);
        }
    };

    let path = Path::new(&path);
    let mut sas = SasReader::open(path)?;
    let metadata = sas.metadata().clone();

    let mut column_summaries: Vec<ColumnSummary> = metadata
        .variables
        .iter()
        .enumerate()
        .map(|(index, variable)| {
            let kind = variable_kind_name(&variable.kind);
            ColumnSummary {
                index,
                name: variable.name.clone(),
                label: variable.label.clone().filter(|s| !s.trim().is_empty()),
                format: format_name(&variable.format),
                kind,
                non_missing: 0,
                missing: 0,
                numeric: if kind == "numeric" {
                    Some(NumericSummary::default())
                } else {
                    None
                },
            }
        })
        .collect();

    let mut total_rows: u64 = 0;
    let mut rows = sas.rows()?;
    while let Some(row) = rows.try_next()? {
        total_rows += 1;

        for (index, value) in row.into_iter().enumerate() {
            let summary = column_summaries
                .get_mut(index)
                .expect("column summary out of bounds");

            match value {
                CellValue::Missing(_) => summary.missing += 1,
                CellValue::Float(actual) => update_numeric(summary, actual),
                CellValue::Int32(actual) => update_numeric(summary, f64::from(actual)),
                CellValue::Int64(actual) => update_numeric(summary, actual as f64),
                // Treat dates/times as non-numeric placeholders for now.
                CellValue::Date(_)
                | CellValue::DateTime(_)
                | CellValue::Time(_)
                | CellValue::NumericString(_)
                | CellValue::Str(_)
                | CellValue::Bytes(_) => {
                    summary.non_missing += 1;
                }
            }
        }
    }

    let summary = DatasetSummary {
        row_count: total_rows,
        columns: column_summaries,
    };

    serde_json::to_writer_pretty(std::io::stdout(), &summary)?;
    println!();
    Ok(())
}
