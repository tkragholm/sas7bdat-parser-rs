use crate::cli::InspectArgs;
use crate::inspect_report::format_inspect_report;
use crate::sas_metadata::DatasetMetaJson;
use crate::selection::{ColumnSelection, resolve_column_indices, selected_columns_refs};
use anyhow::Result;
use std::io::{self, Write};
use std::path::Path;

/// # Errors
///
/// Returns an error if opening or rendering the dataset fails.
pub fn run_inspect(args: &InspectArgs) -> Result<()> {
    let dataset = sas7bdat::Dataset::open(&args.input)?;
    let selected_columns = resolve_column_indices(
        &dataset,
        ColumnSelection {
            names: args.columns.as_deref(),
            indices: args.column_indices.as_deref(),
        },
    )?;
    let selected_column_refs = selected_columns_refs(&dataset, selected_columns.as_deref());
    if args.json {
        let payload = DatasetMetaJson::new(&dataset, &selected_column_refs);
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        serde_json::to_writer_pretty(&mut handle, &payload)?;
        handle.write_all(b"\n")?;
        return Ok(());
    }
    let visible_columns = if args.all_columns {
        selected_column_refs.clone()
    } else {
        selected_column_refs
            .iter()
            .copied()
            .take(args.max_columns.max(1))
            .collect()
    };
    print!(
        "{}",
        format_inspect_report(
            &dataset,
            Path::new(&args.input),
            &visible_columns,
            selected_column_refs.len(),
        )
    );
    Ok(())
}
