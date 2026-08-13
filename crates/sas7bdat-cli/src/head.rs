//! `sas7bdat head` — preview the first rows of a dataset as an aligned table.
//! The renderer is shared with the `info` command's data sample.

use crate::cli::HeadArgs;
use crate::friendly;
use crate::selection::{ColumnSelection, projection_from_selection, resolve_column_indices};
use crate::style::{Style, terminal_width};
use crate::values::thousands;
use anyhow::Result;
use sas7bdat::{Dataset, LogicalType, Projection, RowSelection};
use sas7bdat_convert::values::format_cell;
use std::fmt::Write as _;
use std::ops::ControlFlow;

/// Widest a single column is allowed to get before its cells are truncated with an ellipsis.
const MAX_COL_WIDTH: usize = 40;

pub struct PreviewTable {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub total_rows: u64,
}

/// # Errors
///
/// Returns an error if the file can't be opened or a requested column doesn't exist.
pub fn run_head(args: &HeadArgs) -> Result<()> {
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
    // Validate column selection up front so the user gets a did-you-mean error.
    let indices = resolve_column_indices(&dataset, selection)?;
    let projection = projection_from_selection(&dataset, selection)?;
    let headers = header_names(&dataset, indices.as_deref());
    let table = collect_preview(&dataset, projection.as_ref(), headers, args.rows)?;
    print!(
        "{}",
        render_table(&table, Style::for_stdout(), terminal_width())
    );
    Ok(())
}

/// Column names in output (selection) order; all columns when `indices` is `None`.
#[must_use]
pub fn header_names(dataset: &Dataset, indices: Option<&[usize]>) -> Vec<String> {
    indices.map_or_else(
        || {
            dataset
                .columns()
                .iter()
                .map(|column| column.name.clone())
                .collect()
        },
        |indices| {
            indices
                .iter()
                .filter_map(|&idx| dataset.columns().get(idx))
                .map(|column| column.name.clone())
                .collect()
        },
    )
}

/// Scan up to `limit` rows and format every cell to a display string.
///
/// # Errors
///
/// Returns an error if the scan fails.
pub fn collect_preview(
    dataset: &Dataset,
    projection: Option<&Projection>,
    headers: Vec<String>,
    limit: u64,
) -> Result<PreviewTable> {
    let total_rows = dataset.metadata().row_count;
    // Logical type per output column, in scan order — lets the formatter render a temporal
    // cell that widened to f64 as a timestamp instead of a raw number.
    let kinds: Vec<LogicalType> = projection.map_or_else(
        || dataset.columns().iter().map(|c| c.logical_type).collect(),
        |proj| {
            proj.columns()
                .iter()
                .filter_map(|c| dataset.columns().get(c.index))
                .map(|c| c.logical_type)
                .collect()
        },
    );
    let mut rows: Vec<Vec<String>> = Vec::new();

    if limit > 0 {
        let mut scan = dataset.scan().select(RowSelection::First(limit));
        if let Some(projection) = projection {
            scan = scan.with_projection(projection);
        }
        scan.visit_rows(|row| {
            rows.push(
                row.iter()
                    .zip(&kinds)
                    .map(|(cell, kind)| format_cell(cell, *kind, ""))
                    .collect(),
            );
            if rows.len() as u64 >= limit {
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue(()))
            }
        })?;
    }

    Ok(PreviewTable {
        headers,
        rows,
        total_rows,
    })
}

/// Render a [`PreviewTable`] as an aligned, optionally-colored text block.
///
/// When `max_width` is `Some`, trailing columns that would overflow the terminal are
/// dropped and noted in the footer, so wide datasets stay readable.
#[must_use]
pub fn render_table(table: &PreviewTable, style: Style, max_width: Option<usize>) -> String {
    let widths = column_widths(table);
    let shown = columns_that_fit(&widths, max_width);
    let dropped = widths.len() - shown;
    let widths = &widths[..shown];
    let mut out = String::new();

    // Header row (bold) and an underline.
    let header_cells: Vec<String> = table
        .headers
        .iter()
        .take(shown)
        .zip(widths)
        .map(|(name, width)| pad(&truncate(name), *width))
        .collect();
    let _ = writeln!(out, "{}", style.bold(&header_cells.join("  ")));
    let rule: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
    let _ = writeln!(out, "{}", style.dim(&rule.join("  ")));

    // Data rows.
    for row in &table.rows {
        let cells: Vec<String> = widths
            .iter()
            .enumerate()
            .map(|(idx, width)| {
                let value = row.get(idx).map_or("", String::as_str);
                pad(&truncate(value), *width)
            })
            .collect();
        let _ = writeln!(out, "{}", cells.join("  "));
    }

    let mut footer = if table.rows.len() as u64 == table.total_rows {
        format!("({} rows)", thousands(table.total_rows))
    } else {
        format!(
            "(showing {} of {} rows)",
            table.rows.len(),
            thousands(table.total_rows)
        )
    };
    if dropped > 0 {
        let _ = write!(
            footer,
            " · +{} more cols (use --columns to pick)",
            thousands(dropped as u64)
        );
    }
    let _ = writeln!(out, "{}", style.dim(&footer));
    out
}

/// How many leading columns fit within `max_width` (counting 2-space separators).
/// Always keeps at least one column; `None` keeps them all.
fn columns_that_fit(widths: &[usize], max_width: Option<usize>) -> usize {
    let Some(max) = max_width else {
        return widths.len();
    };
    let mut used = 0;
    let mut count = 0;
    for (idx, width) in widths.iter().enumerate() {
        let needed = if idx == 0 { *width } else { 2 + *width };
        if used + needed > max && count > 0 {
            break;
        }
        used += needed;
        count += 1;
    }
    count.max(1)
}

fn column_widths(table: &PreviewTable) -> Vec<usize> {
    let mut widths: Vec<usize> = table
        .headers
        .iter()
        .map(|name| truncate(name).chars().count())
        .collect();
    for row in &table.rows {
        for (idx, value) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(idx) {
                *width = (*width).max(truncate(value).chars().count());
            }
        }
    }
    widths
}

/// Cap a cell at [`MAX_COL_WIDTH`] characters, appending an ellipsis when truncated.
fn truncate(value: &str) -> String {
    if value.chars().count() <= MAX_COL_WIDTH {
        return value.to_owned();
    }
    let mut out: String = value.chars().take(MAX_COL_WIDTH - 1).collect();
    out.push('\u{2026}');
    out
}

/// Left-pad `value` with spaces to `width` display characters.
fn pad(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len >= width {
        value.to_owned()
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

#[cfg(test)]
mod tests {
    use super::{PreviewTable, columns_that_fit, render_table};
    use crate::style::Style;

    #[test]
    fn columns_that_fit_respects_the_width_budget() {
        // Widths [3, 5, 5] render as 3 + (2+5) + (2+5) = 17 chars with separators.
        let widths = [3, 5, 5];
        assert_eq!(columns_that_fit(&widths, None), 3); // no cap -> all
        assert_eq!(columns_that_fit(&widths, Some(20)), 3); // 17 <= 20
        assert_eq!(columns_that_fit(&widths, Some(12)), 2); // 17 > 12, 10 <= 12
        assert_eq!(columns_that_fit(&widths, Some(1)), 1); // always at least one
    }

    fn table() -> PreviewTable {
        PreviewTable {
            headers: vec!["a".into(), "bb".into(), "ccc".into()],
            rows: vec![vec!["1".into(), "2".into(), "3".into()]],
            total_rows: 1,
        }
    }

    #[test]
    fn render_drops_overflowing_columns_and_notes_them() {
        let plain = Style::for_stderr(); // not a tty in tests -> styling off
        // Narrow budget: only the first column fits; the footer flags the rest.
        let out = render_table(&table(), plain, Some(3));
        assert!(
            out.contains("+2 more cols"),
            "footer should note dropped columns:\n{out}"
        );
        // Header should not include the dropped third column name.
        assert!(!out.contains("ccc"));
    }

    #[test]
    fn render_shows_all_columns_without_a_budget() {
        let out = render_table(&table(), Style::for_stderr(), None);
        assert!(out.contains("ccc"));
        assert!(!out.contains("more cols"));
        assert!(out.contains("(1 rows)"));
    }

    #[test]
    fn dropped_column_count_is_thousands_separated() {
        // A wide table squeezed to one column: the footer must report the real number of
        // hidden columns, grouped (e.g. 4,028) — the bug `info` had when it counted only
        // the columns dropped from an already-capped subset.
        let headers: Vec<String> = (0..1001).map(|i| format!("c{i}")).collect();
        let row: Vec<String> = (0..1001).map(|i| i.to_string()).collect();
        let table = PreviewTable {
            headers,
            rows: vec![row],
            total_rows: 1,
        };
        let out = render_table(&table, Style::for_stderr(), Some(1));
        assert!(
            out.contains("+1,000 more cols"),
            "footer:\n{}",
            out.lines().last().unwrap_or("")
        );
    }
}
