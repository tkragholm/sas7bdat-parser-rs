use crate::cli::InspectArgs;
use crate::friendly;
use crate::head::{collect_preview, header_names, render_table};
use crate::inspect_report::format_inspect_report;
use crate::sas_metadata::DatasetMetaJson;
use crate::selection::{
    ColumnSelection, projection_from_selection, resolve_column_indices, selected_columns_refs,
};
use crate::style::{Style, terminal_width};
use anyhow::Result;
use std::io::{self, Write};
use std::path::Path;

/// Rows shown in the `info` data sample.
const SAMPLE_ROWS: u64 = 5;

/// # Errors
///
/// Returns an error if opening or rendering the dataset fails.
pub fn run_inspect(args: &InspectArgs) -> Result<()> {
    let dataset = friendly::open_with(
        &args.input,
        sas7bdat::OpenOptions::builder()
            .io_backend(args.io_backend.preference())
            .build(),
    )?;
    let selection = ColumnSelection {
        names: args.columns.as_deref(),
        indices: args.column_indices.as_deref(),
    };
    let selected_columns = resolve_column_indices(&dataset, selection)?;
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

    // A small data sample. Cover the full (or user-selected) column set — not just the
    // columns shown in the table above — so the renderer's width-capping and its
    // "+N more cols" note reflect the real column count, not the table's --max-columns cap.
    if !args.no_sample && dataset.metadata().row_count > 0 {
        let style = Style::for_stdout();
        let sample_cols = selected_columns.as_deref();
        let projection = projection_from_selection(
            &dataset,
            ColumnSelection {
                names: None,
                indices: sample_cols,
            },
        )?;
        let headers = header_names(&dataset, sample_cols);
        let table = collect_preview(&dataset, projection.as_ref(), headers, SAMPLE_ROWS)?;
        println!("\n{}", style.bold("Sample:"));
        print!("{}", render_table(&table, style, terminal_width()));
    }
    Ok(())
}
