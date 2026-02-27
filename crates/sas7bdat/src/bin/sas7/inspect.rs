use crate::AnyError;
use sas7bdat::{SasReader, dataset::VariableKind};
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
        #[derive(serde::Serialize)]
        struct ColumnInfoJson {
            index: u32,
            name: String,
            label: Option<String>,
            kind: &'static str,
            format: Option<String>,
            width: usize,
        }
        #[derive(serde::Serialize)]
        struct InspectJson {
            row_count: u64,
            column_count: u32,
            columns: Vec<ColumnInfoJson>,
        }
        let columns = meta
            .variables
            .iter()
            .map(|v| ColumnInfoJson {
                index: v.index,
                name: v.name.clone(),
                label: v.label.clone(),
                kind: match v.kind {
                    VariableKind::Numeric => "numeric",
                    VariableKind::Character => "character",
                },
                format: v.format.as_ref().map(|f| f.name.clone()),
                width: v.storage_width,
            })
            .collect();
        let payload = InspectJson {
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
