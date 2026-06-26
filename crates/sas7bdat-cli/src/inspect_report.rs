use crate::values::{human_bytes, thousands};
use sas7bdat::{ColumnMeta, CompressionKind, Dataset, LogicalType};
use std::path::Path;

const fn compression_label(kind: CompressionKind) -> &'static str {
    match kind {
        CompressionKind::None => "none",
        CompressionKind::Row => "row (RLE)",
        CompressionKind::Binary => "binary (RDC)",
        CompressionKind::Unknown => "unknown",
    }
}

/// Turn a raw SAS format into a meaningful hint for the `info` table.
///
/// SAS's content-free general formats (`BEST`, `F`, `$`, ...) and temporal formats (whose
/// meaning is already carried by the `Kind` column) become `-`. Common numeric families
/// map to a plain word; anything user-defined keeps its raw name (it signals a custom
/// format — frequently value labels, which `convert --catalog` can hydrate).
fn humanize_format(format: Option<&str>, kind: LogicalType) -> String {
    // The Kind column already says date/datetime/time; the date format adds nothing.
    if matches!(
        kind,
        LogicalType::Date | LogicalType::DateTime | LogicalType::Time
    ) {
        return "-".to_owned();
    }
    let Some(raw) = format.map(str::trim).filter(|f| !f.is_empty()) else {
        return "-".to_owned();
    };
    match format_base_name(raw).as_str() {
        // SAS's "just a number / just a string" defaults — no real meaning.
        "BEST" | "F" | "D" | "Z" | "$" | "$CHAR" | "$F" | "BESTD" => "-".to_owned(),
        "DOLLAR" | "EURO" | "YEN" | "NLMNY" | "NLMNYI" => "currency".to_owned(),
        "PERCENT" | "PCT" => "percent".to_owned(),
        "COMMA" | "COMMAX" => "grouped".to_owned(),
        "E" => "scientific".to_owned(),
        "HEX" => "hex".to_owned(),
        // User-defined / unrecognized: keep the raw name as a hint.
        _ => raw.to_owned(),
    }
}

/// The leading format name without its `width.decimals` suffix, upper-cased
/// (e.g. `DOLLAR12.2` -> `DOLLAR`, `$SEX.` -> `$SEX`).
fn format_base_name(raw: &str) -> String {
    raw.chars()
        .take_while(|&ch| ch == '$' || ch.is_ascii_alphabetic())
        .map(|ch| ch.to_ascii_uppercase())
        .collect()
}

#[must_use]
pub fn format_inspect_report(
    dataset: &Dataset,
    source: &Path,
    columns: &[&ColumnMeta],
    selected_count: usize,
) -> String {
    let mut out = String::new();
    append_inspect_summary(&mut out, dataset, source, columns.len(), selected_count);
    append_inspect_table(&mut out, columns);
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
    let total_count = dataset.columns().len();

    // Path, then a single compact stats line, then optional table/label.
    let _ = writeln!(out, "{}", source.display());
    let mut stats = String::new();
    if let Ok(size) = std::fs::metadata(source) {
        let _ = write!(stats, "{} · ", human_bytes(size.len()));
    }
    let _ = write!(
        stats,
        "{} rows · {} cols · {} · {}",
        thousands(meta.row_count),
        thousands(total_count as u64),
        compression_label(meta.compression),
        meta.encoding.as_deref().unwrap_or("unknown encoding"),
    );
    let _ = writeln!(out, "  {stats}");
    if let Some(table) = meta.table_name.as_deref() {
        let _ = writeln!(out, "  table: {table}");
    }
    if let Some(label) = meta.file_label.as_deref() {
        let _ = writeln!(out, "  label: {label}");
    }

    // Heading for the column listing, carrying the showing/selected counts.
    let _ = writeln!(out);
    if selected_count == total_count {
        let _ = writeln!(
            out,
            "Columns (showing {} of {}):",
            thousands(visible_count as u64),
            thousands(total_count as u64)
        );
    } else {
        let _ = writeln!(
            out,
            "Columns (showing {} of {} selected, {} total):",
            thousands(visible_count as u64),
            thousands(selected_count as u64),
            thousands(total_count as u64)
        );
    }
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

fn append_inspect_table(out: &mut String, columns: &[&ColumnMeta]) {
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
            format_name: humanize_format(column.format.as_deref(), column.logical_type),
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

#[cfg(test)]
mod tests {
    use super::humanize_format;
    use sas7bdat::LogicalType;

    #[test]
    fn humanize_format_hides_noise_and_translates_families() {
        let f = LogicalType::Float;
        // SAS's content-free defaults become "-".
        assert_eq!(humanize_format(Some("BEST"), f), "-");
        assert_eq!(humanize_format(Some("BEST12."), f), "-");
        assert_eq!(humanize_format(Some("$"), LogicalType::String), "-");
        assert_eq!(humanize_format(None, f), "-");
        assert_eq!(humanize_format(Some("   "), f), "-");

        // Known numeric families map to a plain hint (width/decimals stripped).
        assert_eq!(humanize_format(Some("DOLLAR12.2"), f), "currency");
        assert_eq!(humanize_format(Some("PERCENT8.1"), f), "percent");
        assert_eq!(humanize_format(Some("COMMA10."), f), "grouped");
        assert_eq!(humanize_format(Some("E"), f), "scientific");

        // Temporal columns: the Kind column already conveys it, so blank.
        assert_eq!(humanize_format(Some("DATETIME"), LogicalType::DateTime), "-");
        assert_eq!(humanize_format(Some("DATE9."), LogicalType::Date), "-");

        // User-defined / unrecognized formats keep their raw name as a hint.
        assert_eq!(humanize_format(Some("$A"), LogicalType::String), "$A");
        assert_eq!(humanize_format(Some("SEXF."), f), "SEXF.");
    }
}
