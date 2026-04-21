use sas7bdat_simd::{ColumnMeta, Dataset};
use std::path::Path;

#[must_use]
pub fn format_inspect_report(
    dataset: &Dataset,
    source: &Path,
    columns: &[&ColumnMeta],
    selected_count: usize,
) -> String {
    let mut out = String::new();
    append_inspect_summary(&mut out, dataset, source, columns.len(), selected_count);
    append_inspect_table(&mut out, columns, selected_count);
    out
}

fn append_inspect_summary(
    out: &mut String,
    dataset: &Dataset,
    source: &Path,
    visible_count: usize,
    selected_count: usize,
) {
    use std::fmt::Write as _;

    let meta = dataset.metadata();
    let _ = writeln!(out, "File: {}", source.display());
    let _ = writeln!(out, "Rows: {}", meta.row_count);
    let total_count = dataset.columns().len();
    let _ = writeln!(out, "Columns: {total_count}");
    if selected_count != total_count {
        let _ = writeln!(out, "Selected columns: {selected_count}");
    }
    if visible_count != selected_count {
        if selected_count == total_count {
            let _ = writeln!(out, "Showing: {visible_count} of {selected_count} columns");
        } else {
            let _ = writeln!(
                out,
                "Showing: {visible_count} of {selected_count} selected columns"
            );
        }
    }
    let _ = writeln!(out, "Table: {}", meta.table_name.as_deref().unwrap_or("-"));
    let _ = writeln!(out, "Label: {}", meta.file_label.as_deref().unwrap_or("-"));
    let _ = writeln!(out, "Encoding: {}", meta.encoding.as_deref().unwrap_or("-"));
    let _ = writeln!(out);
}

#[derive(Clone, Debug)]
struct InspectColumn {
    idx: String,
    name: String,
    kind: String,
    width: String,
    label: String,
    format_name: String,
}

fn append_inspect_table(out: &mut String, columns: &[&ColumnMeta], selected_count: usize) {
    use std::fmt::Write as _;

    let display_columns: Vec<_> = columns
        .iter()
        .copied()
        .map(|column| InspectColumn {
            idx: column.index.to_string(),
            name: column.name.clone(),
            kind: format!("{:?}", column.logical_type).to_lowercase(),
            width: column.physical_width.to_string(),
            label: column.label.as_deref().unwrap_or("-").to_string(),
            format_name: column.format.as_deref().unwrap_or("-").trim().to_string(),
        })
        .collect();

    let (idx_width, name_width, kind_width, width_width, label_width, format_width) =
        inspect_column_widths(&display_columns);
    let _ = writeln!(
        out,
        "{:>idx_width$}  {:<name_width$}  {:<kind_width$}  {:>width_width$}  {:<label_width$}  {:<format_width$}",
        "Idx",
        "Name",
        "Kind",
        "Width",
        "Label",
        "Format",
        idx_width = idx_width,
        name_width = name_width,
        kind_width = kind_width,
        width_width = width_width,
        label_width = label_width,
        format_width = format_width,
    );
    let _ = writeln!(
        out,
        "{:-<idx_width$}  {:-<name_width$}  {:-<kind_width$}  {:-<width_width$}  {:-<label_width$}  {:-<format_width$}",
        "",
        "",
        "",
        "",
        "",
        "",
        idx_width = idx_width,
        name_width = name_width,
        kind_width = kind_width,
        width_width = width_width,
        label_width = label_width,
        format_width = format_width,
    );
    for column in &display_columns {
        let _ = writeln!(
            out,
            "{idx:>idx_width$}  {name:<name_width$}  {kind:<kind_width$}  {width:>width_width$}  {label:<label_width$}  {format_name:<format_width$}",
            idx = column.idx,
            name = column.name,
            kind = column.kind,
            width = column.width,
            label = column.label,
            format_name = column.format_name,
            idx_width = idx_width,
            name_width = name_width,
            kind_width = kind_width,
            width_width = width_width,
            label_width = label_width,
            format_width = format_width,
        );
    }
    if selected_count > display_columns.len() {
        let _ = writeln!(
            out,
            "... {} more columns not shown",
            selected_count - display_columns.len()
        );
    }
}

fn inspect_column_widths(columns: &[InspectColumn]) -> (usize, usize, usize, usize, usize, usize) {
    let idx_width = columns
        .iter()
        .map(|column| column.idx.len())
        .max()
        .unwrap_or(3)
        .max(3);
    let name_width = columns
        .iter()
        .map(|column| column.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let kind_width = columns
        .iter()
        .map(|column| column.kind.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let width_width = columns
        .iter()
        .map(|column| column.width.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let label_width = columns
        .iter()
        .map(|column| column.label.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let format_width = columns
        .iter()
        .map(|column| column.format_name.len())
        .max()
        .unwrap_or(6)
        .max(6);
    (
        idx_width,
        name_width,
        kind_width,
        width_width,
        label_width,
        format_width,
    )
}
