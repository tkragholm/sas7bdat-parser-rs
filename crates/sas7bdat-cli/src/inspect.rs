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
    let dataset = friendly::open(&args.input)?;
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

    // A small data sample over the visible columns, unless suppressed or empty.
    if !args.no_sample && dataset.metadata().row_count > 0 {
        let style = Style::for_stdout();
        let indices: Vec<usize> = visible_columns.iter().map(|column| column.index).collect();
        let projection = projection_from_selection(
            &dataset,
            ColumnSelection {
                names: None,
                indices: Some(&indices),
            },
        )?;
        let headers = header_names(&dataset, Some(&indices));
        let table = collect_preview(&dataset, projection.as_ref(), headers, SAMPLE_ROWS)?;
        println!("\n{}", style.bold("Sample:"));
        print!("{}", render_table(&table, style, terminal_width()));
    }
    Ok(())
}
