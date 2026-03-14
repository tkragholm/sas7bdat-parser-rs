use crate::AnyError;
use sas7bdat::{ColumnInfoJson, SasReader, TableInfoJson, dataset::VariableKind};
use std::path::PathBuf;

#[derive(Clone)]
pub struct InspectArgs {
    pub(crate) input: PathBuf,
    pub(crate) json: bool,
}

pub fn run_inspect(args: &InspectArgs) -> Result<(), AnyError> {
    let sas = SasReader::open(&args.input)?;
    let meta = sas.metadata().clone();
    if args.json {
        let columns = meta
            .variables
            .iter()
            .map(|v| ColumnInfoJson {
                index: v.index,
                name: v.name.clone(),
                label: v.label.clone(),
                kind: match v.kind {
                    VariableKind::Numeric => "numeric".to_string(),
                    VariableKind::Character => "character".to_string(),
                },
                format: v.format.as_ref().map(|f| f.name.clone()),
                width: v.storage_width,
            })
            .collect();
        let payload = TableInfoJson {
            table_name: meta.table_name.clone(),
            file_label: meta.file_label.clone(),
            row_count: meta.row_count,
            column_count: meta.column_count,
            columns,
        };
        serde_json::to_writer_pretty(std::io::stdout(), &payload)?;
        println!();
    } else {
        println!(
            "Rows: {}  Columns: {}  Table: {}",
            meta.row_count,
            meta.column_count,
            meta.table_name.as_deref().unwrap_or("")
        );
        for v in &meta.variables {
            let kind = match v.kind {
                VariableKind::Numeric => "numeric",
                VariableKind::Character => "character",
            };
            let fmt = v
                .format
                .as_ref()
                .map(|f| f.name.trim().to_owned())
                .unwrap_or_default();
            println!(
                "[{idx:>3}] {name:<24}  {kind:<9}  width={w:<4}  fmt={fmt}",
                idx = v.index,
                name = v.name,
                kind = kind,
                w = v.storage_width,
                fmt = fmt
            );
        }
    }
    Ok(())
}
