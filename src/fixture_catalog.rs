#![allow(clippy::cast_precision_loss)]

use crate::{CellValue, Dataset, LogicalType, Projection, Result, RowSelection, ScanStats};
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureCatalog {
    pub roots: Vec<String>,
    pub sample_rows: usize,
    pub fixtures: Vec<FixtureEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureEntry {
    pub path: String,
    pub file_name: String,
    pub source_group: String,
    pub size_bytes: u64,
    #[serde(flatten)]
    pub status: FixtureStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FixtureStatus {
    Profiled(Box<FixtureProfile>),
    Error { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureProfile {
    pub table_name: Option<String>,
    pub encoding: Option<String>,
    pub compression: String,
    pub row_count: u64,
    pub column_count: usize,
    pub row_len: u32,
    pub page_size: u32,
    pub page_count: u64,
    pub logical_types: LogicalTypeCounts,
    pub widths: WidthSummary,
    pub temporal_formats: TemporalFormatSummary,
    pub sample: SampleSummary,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct LogicalTypeCounts {
    pub string: usize,
    pub integer: usize,
    pub float: usize,
    pub date: usize,
    pub datetime: usize,
    pub time: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct WidthSummary {
    pub string_width_sum: u64,
    pub string_width_max: u32,
    pub numeric_width_sum: u64,
    pub numeric_width_max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TemporalFormatSummary {
    pub date_format_columns: usize,
    pub datetime_format_columns: usize,
    pub time_format_columns: usize,
    pub date_formats: Vec<NamedCount>,
    pub datetime_formats: Vec<NamedCount>,
    pub time_formats: Vec<NamedCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedCount {
    pub name: String,
    pub count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct SampleSummary {
    pub rows_sampled: u64,
    pub string_cells: u64,
    pub empty_string_cells: u64,
    pub ascii_string_cells: u64,
    pub non_ascii_string_cells: u64,
    pub total_trimmed_string_len: u64,
    pub max_trimmed_string_len: u64,
    pub numeric_like_cells: u64,
    pub null_numeric_like_cells: u64,
}

impl SampleSummary {
    #[must_use]
    pub fn avg_trimmed_string_len(self) -> f64 {
        if self.string_cells == 0 {
            0.0
        } else {
            self.total_trimmed_string_len as f64 / self.string_cells as f64
        }
    }

    #[must_use]
    pub fn empty_string_ratio(self) -> f64 {
        if self.string_cells == 0 {
            0.0
        } else {
            self.empty_string_cells as f64 / self.string_cells as f64
        }
    }

    #[must_use]
    pub fn ascii_ratio(self) -> f64 {
        if self.string_cells == 0 {
            0.0
        } else {
            self.ascii_string_cells as f64 / self.string_cells as f64
        }
    }

    #[must_use]
    pub fn missing_numeric_ratio(self) -> f64 {
        if self.numeric_like_cells == 0 {
            0.0
        } else {
            self.null_numeric_like_cells as f64 / self.numeric_like_cells as f64
        }
    }
}

pub fn discover_fixture_paths(inputs: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for input in inputs {
        discover_path(input, &mut paths)?;
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn discover_path(path: &Path, paths: &mut Vec<PathBuf>) -> io::Result<()> {
    let meta = fs::metadata(path)?;
    if meta.is_dir() {
        for child in fs::read_dir(path)? {
            let child = child?.path();
            discover_path(&child, paths)?;
        }
    } else if path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("sas7bdat"))
    {
        paths.push(path.to_path_buf());
    }
    Ok(())
}

#[must_use]
pub fn build_catalog(paths: &[PathBuf], sample_rows: usize) -> FixtureCatalog {
    let fixtures = paths
        .iter()
        .map(|path| profile_fixture(path, sample_rows))
        .collect();
    FixtureCatalog {
        roots: paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        sample_rows,
        fixtures,
    }
}

pub fn profile_fixture(path: &Path, sample_rows: usize) -> FixtureEntry {
    let size_bytes = fs::metadata(path).map_or(0, |meta| meta.len());
    let path_string = path.display().to_string();
    let file_name = path
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    let source_group = source_group(path);

    let status = match Dataset::open(path) {
        Ok(ds) => FixtureStatus::Profiled(Box::new(profile_dataset(&ds, sample_rows))),
        Err(err) => FixtureStatus::Error {
            error: err.to_string(),
        },
    };

    FixtureEntry {
        path: path_string,
        file_name,
        source_group,
        size_bytes,
        status,
    }
}

fn profile_dataset(ds: &Dataset, sample_rows: usize) -> FixtureProfile {
    let sample = sample_dataset(ds, sample_rows).unwrap_or_default();
    profile_dataset_with_sample(ds, sample)
}

#[doc(hidden)]
#[must_use]
pub fn profile_dataset_with_sample(ds: &Dataset, sample: SampleSummary) -> FixtureProfile {
    let logical_types = logical_type_counts(ds);
    let widths = width_summary(ds);
    let temporal_formats = temporal_format_summary(ds);
    let tags = classify_tags(ds, logical_types, sample);
    FixtureProfile {
        table_name: ds.metadata().table_name.clone(),
        encoding: ds.metadata().encoding.clone(),
        compression: compression_name(ds).to_owned(),
        row_count: ds.metadata().row_count,
        column_count: ds.columns().len(),
        row_len: ds.metadata().row_len,
        page_size: ds.metadata().page_size,
        page_count: ds.metadata().page_count,
        logical_types,
        widths,
        temporal_formats,
        sample,
        tags,
    }
}

fn logical_type_counts(ds: &Dataset) -> LogicalTypeCounts {
    let mut counts = LogicalTypeCounts::default();
    for column in ds.columns() {
        match column.logical_type {
            LogicalType::String => counts.string += 1,
            LogicalType::Integer => counts.integer += 1,
            LogicalType::Float => counts.float += 1,
            LogicalType::Date => counts.date += 1,
            LogicalType::DateTime => counts.datetime += 1,
            LogicalType::Time => counts.time += 1,
            LogicalType::Bytes => counts.bytes += 1,
        }
    }
    counts
}

fn width_summary(ds: &Dataset) -> WidthSummary {
    let mut widths = WidthSummary::default();
    for column in ds.columns() {
        match column.logical_type {
            LogicalType::String | LogicalType::Bytes => {
                widths.string_width_sum += u64::from(column.physical_width);
                widths.string_width_max = widths.string_width_max.max(column.physical_width);
            }
            LogicalType::Integer
            | LogicalType::Float
            | LogicalType::Date
            | LogicalType::DateTime
            | LogicalType::Time => {
                widths.numeric_width_sum += u64::from(column.physical_width);
                widths.numeric_width_max = widths.numeric_width_max.max(column.physical_width);
            }
        }
    }
    widths
}

fn temporal_format_summary(ds: &Dataset) -> TemporalFormatSummary {
    let mut summary = TemporalFormatSummary::default();
    let mut date_formats = std::collections::BTreeMap::<String, usize>::new();
    let mut datetime_formats = std::collections::BTreeMap::<String, usize>::new();
    let mut time_formats = std::collections::BTreeMap::<String, usize>::new();

    for column in ds.columns() {
        let Some(format) = column.format.as_deref() else {
            continue;
        };
        let cleaned = format.trim();
        if cleaned.is_empty() {
            continue;
        }
        match column.logical_type {
            LogicalType::Date => {
                summary.date_format_columns += 1;
                *date_formats.entry(cleaned.to_owned()).or_default() += 1;
            }
            LogicalType::DateTime => {
                summary.datetime_format_columns += 1;
                *datetime_formats.entry(cleaned.to_owned()).or_default() += 1;
            }
            LogicalType::Time => {
                summary.time_format_columns += 1;
                *time_formats.entry(cleaned.to_owned()).or_default() += 1;
            }
            _ => {}
        }
    }

    summary.date_formats = named_counts(date_formats);
    summary.datetime_formats = named_counts(datetime_formats);
    summary.time_formats = named_counts(time_formats);
    summary
}

fn named_counts(values: std::collections::BTreeMap<String, usize>) -> Vec<NamedCount> {
    let mut values: Vec<NamedCount> = values
        .into_iter()
        .map(|(name, count)| NamedCount { name, count })
        .collect();
    values.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    values
}

fn sample_dataset(ds: &Dataset, sample_rows: usize) -> Result<SampleSummary> {
    if sample_rows == 0 || ds.metadata().row_count == 0 || ds.columns().is_empty() {
        return Ok(SampleSummary::default());
    }

    let windows = sample_windows(ds.metadata().row_count, sample_rows);
    let mut sample = SampleSummary::default();
    let columns = ds.columns().to_vec();
    for (start, end) in windows {
        let selection = RowSelection::Range { start, end };
        let stats = ds.scan().select(selection).visit_rows(|row| {
            sample.rows_sampled += 1;
            for (column, cell) in columns.iter().zip(row.iter()) {
                match (column.logical_type, cell) {
                    (LogicalType::String, CellValue::Str(value)) => {
                        sample.string_cells += 1;
                        let trimmed_len = value.len() as u64;
                        sample.total_trimmed_string_len += trimmed_len;
                        sample.max_trimmed_string_len =
                            sample.max_trimmed_string_len.max(trimmed_len);
                        if value.is_empty() {
                            sample.empty_string_cells += 1;
                        }
                        if value.is_ascii() {
                            sample.ascii_string_cells += 1;
                        } else {
                            sample.non_ascii_string_cells += 1;
                        }
                    }
                    (LogicalType::Bytes, CellValue::Bytes(bytes)) => {
                        sample.string_cells += 1;
                        let trimmed_len = bytes.len() as u64;
                        sample.total_trimmed_string_len += trimmed_len;
                        sample.max_trimmed_string_len =
                            sample.max_trimmed_string_len.max(trimmed_len);
                        if bytes.is_empty() {
                            sample.empty_string_cells += 1;
                        }
                        if bytes.iter().all(u8::is_ascii) {
                            sample.ascii_string_cells += 1;
                        } else {
                            sample.non_ascii_string_cells += 1;
                        }
                    }
                    (
                        LogicalType::Integer
                        | LogicalType::Float
                        | LogicalType::Date
                        | LogicalType::DateTime
                        | LogicalType::Time,
                        CellValue::Null,
                    ) => {
                        sample.numeric_like_cells += 1;
                        sample.null_numeric_like_cells += 1;
                    }
                    (
                        LogicalType::Integer
                        | LogicalType::Float
                        | LogicalType::Date
                        | LogicalType::DateTime
                        | LogicalType::Time,
                        _,
                    ) => {
                        sample.numeric_like_cells += 1;
                    }
                    _ => {}
                }
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        debug_assert!(stats.rows_emitted <= end.saturating_sub(start));
    }
    Ok(sample)
}

fn sample_windows(total_rows: u64, sample_rows: usize) -> Vec<(u64, u64)> {
    let target = u64::try_from(sample_rows)
        .unwrap_or(u64::MAX)
        .min(total_rows);
    if target == 0 {
        return Vec::new();
    }
    if total_rows <= target {
        return vec![(0, total_rows)];
    }
    if target <= 3 {
        return vec![(0, target)];
    }

    let front = target / 3;
    let middle = target / 3;
    let back = target.saturating_sub(front + middle);
    let middle_start = total_rows.saturating_sub(middle) / 2;
    let back_start = total_rows.saturating_sub(back);
    let mut windows = vec![
        (0, front),
        (middle_start, middle_start.saturating_add(middle)),
        (back_start, total_rows),
    ];
    windows.retain(|(start, end)| end > start);
    windows
}

fn classify_tags(
    ds: &Dataset,
    logical_types: LogicalTypeCounts,
    sample: SampleSummary,
) -> Vec<String> {
    let mut tags = vec![
        compression_name(ds).to_owned(),
        encoding_tag(ds),
        size_tag(ds),
        width_tag(ds),
        content_tag(ds, logical_types),
    ];

    if logical_types.string > logical_types.integer + logical_types.float
        && sample.avg_trimmed_string_len() <= 4.0
    {
        tags.push("categorical-heavy".to_owned());
    }

    if ds.metadata().row_count == 0 {
        tags.push("empty".to_owned());
    } else {
        tags.push("correctness".to_owned());
    }

    let benchmark_tag = if ds.metadata().page_count >= 4096 || ds.columns().len() >= 1024 {
        "benchmark-macro"
    } else if ds.columns().len() <= 12
        && ds.metadata().row_len <= 32
        && ds.metadata().page_count <= 32
        && ds.metadata().row_count <= 5_000
    {
        "correctness-only"
    } else if ds.metadata().page_count >= 64
        || ds.metadata().row_count >= 10_000
        || ds.columns().len() >= 16
    {
        "benchmark-standard"
    } else {
        "correctness-only"
    };
    tags.push(benchmark_tag.to_owned());
    tags
}

fn compression_name(ds: &Dataset) -> &'static str {
    match ds.metadata().compression {
        crate::CompressionKind::None => "uncompressed",
        crate::CompressionKind::Row => "compressed",
        crate::CompressionKind::Binary => "compressed-binary",
        crate::CompressionKind::Unknown => "compressed-unknown",
    }
}

fn encoding_tag(ds: &Dataset) -> String {
    match ds.metadata().encoding.as_deref() {
        Some("UTF-8") => "utf8".to_owned(),
        Some(_) => "legacy-encoding".to_owned(),
        None => "encoding-unknown".to_owned(),
    }
}

fn size_tag(ds: &Dataset) -> String {
    let page_bytes = u64::from(ds.metadata().page_size).saturating_mul(ds.metadata().page_count);
    match page_bytes {
        0..=1_048_575 => "tiny".to_owned(),
        1_048_576..=16_777_215 => "small".to_owned(),
        16_777_216..=268_435_455 => "medium".to_owned(),
        268_435_456..=1_073_741_823 => "large".to_owned(),
        _ => "huge".to_owned(),
    }
}

fn width_tag(ds: &Dataset) -> String {
    let cols = ds.columns().len();
    let row_len = ds.metadata().row_len;
    if cols > 1024 || row_len > 4096 {
        "ultra-wide".to_owned()
    } else if cols > 64 || row_len > 256 {
        "wide".to_owned()
    } else if cols > 16 || row_len > 64 {
        "medium".to_owned()
    } else {
        "narrow".to_owned()
    }
}

fn content_tag(ds: &Dataset, logical_types: LogicalTypeCounts) -> String {
    let cols = ds.columns().len();
    if cols == 0 {
        return "empty".to_owned();
    }
    let string_like = logical_types.string + logical_types.bytes;
    let numeric_like = logical_types.integer
        + logical_types.float
        + logical_types.date
        + logical_types.datetime
        + logical_types.time;
    let total = cols as f64;
    if string_like as f64 / total >= 0.7 {
        "string-heavy".to_owned()
    } else if numeric_like as f64 / total >= 0.7 {
        "numeric-heavy".to_owned()
    } else {
        "mixed".to_owned()
    }
}

fn source_group(path: &Path) -> String {
    let mut iter = path.components().map(std::path::Component::as_os_str);
    while let Some(component) = iter.next() {
        if component == "raw_data" {
            return iter.next().map_or_else(
                || "raw_data".to_owned(),
                |part| part.to_string_lossy().into_owned(),
            );
        }
    }
    path.parent().and_then(Path::file_name).map_or_else(
        || "root".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

#[must_use]
pub const fn summarize_scan_stats(stats: &ScanStats) -> ScanStatsSummary {
    ScanStatsSummary {
        rows_seen: stats.rows_seen,
        rows_emitted: stats.rows_emitted,
        pages_seen: stats.pages_seen,
        fused_pages: stats.fused_pages,
        indexed_pages: stats.indexed_pages,
        compressed_pages: stats.compressed_pages,
        raw_bytes_read: stats.raw_bytes_read,
        row_bytes_materialized: stats.row_bytes_materialized,
        decode_batches: stats.decode_batches,
    }
}

#[derive(Debug, Clone, Copy, Serialize, Default)]
pub struct ScanStatsSummary {
    pub rows_seen: u64,
    pub rows_emitted: u64,
    pub pages_seen: u64,
    pub fused_pages: u64,
    pub indexed_pages: u64,
    pub compressed_pages: u64,
    pub raw_bytes_read: u64,
    pub row_bytes_materialized: u64,
    pub decode_batches: u64,
}

#[must_use]
pub fn build_projection(ds: &Dataset, preset: ProjectionPreset) -> Option<Projection> {
    match preset {
        ProjectionPreset::Full => None,
        ProjectionPreset::Numeric => {
            let numeric_indices: Vec<usize> = ds
                .columns()
                .iter()
                .filter(|column| {
                    matches!(
                        column.logical_type,
                        LogicalType::Integer
                            | LogicalType::Float
                            | LogicalType::Date
                            | LogicalType::DateTime
                            | LogicalType::Time
                    )
                })
                .map(|column| column.index)
                .collect();
            projection_from_indices(ds, &numeric_indices)
        }
        ProjectionPreset::Strings => {
            let string_indices: Vec<usize> = ds
                .columns()
                .iter()
                .filter(|column| {
                    matches!(
                        column.logical_type,
                        LogicalType::String | LogicalType::Bytes
                    )
                })
                .map(|column| column.index)
                .collect();
            projection_from_indices(ds, &string_indices)
        }
        ProjectionPreset::Mixed => {
            let numeric = ds
                .columns()
                .iter()
                .filter(|column| {
                    matches!(
                        column.logical_type,
                        LogicalType::Integer
                            | LogicalType::Float
                            | LogicalType::Date
                            | LogicalType::DateTime
                            | LogicalType::Time
                    )
                })
                .take(4)
                .map(|column| column.index);
            let strings = ds
                .columns()
                .iter()
                .filter(|column| {
                    matches!(
                        column.logical_type,
                        LogicalType::String | LogicalType::Bytes
                    )
                })
                .take(4)
                .map(|column| column.index);
            let indices: Vec<usize> = numeric.chain(strings).collect();
            projection_from_indices(ds, &indices)
        }
    }
}

fn projection_from_indices(ds: &Dataset, indices: &[usize]) -> Option<Projection> {
    if indices.is_empty() {
        return None;
    }
    let mut builder = ds.projection();
    for idx in indices {
        builder = builder.column_idx(*idx);
    }
    builder.build().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPreset {
    Full,
    Numeric,
    Strings,
    Mixed,
}

impl ProjectionPreset {
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "full" => Some(Self::Full),
            "numeric" => Some(Self::Numeric),
            "strings" => Some(Self::Strings),
            "mixed" => Some(Self::Mixed),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_windows_cover_start_middle_end() {
        assert_eq!(sample_windows(100, 9), vec![(0, 3), (48, 51), (97, 100)]);
    }

    #[test]
    fn classify_string_heavy_fixture() {
        let tags = content_tag_for_counts(LogicalTypeCounts {
            string: 8,
            bytes: 0,
            integer: 0,
            float: 1,
            date: 0,
            datetime: 0,
            time: 0,
        });
        assert_eq!(tags, "string-heavy");
    }

    #[test]
    fn classify_numeric_heavy_fixture() {
        let tags = content_tag_for_counts(LogicalTypeCounts {
            string: 1,
            bytes: 0,
            integer: 5,
            float: 4,
            date: 0,
            datetime: 0,
            time: 0,
        });
        assert_eq!(tags, "numeric-heavy");
    }

    #[test]
    fn classify_mixed_fixture() {
        let tags = content_tag_for_counts(LogicalTypeCounts {
            string: 4,
            bytes: 0,
            integer: 3,
            float: 3,
            date: 0,
            datetime: 0,
            time: 0,
        });
        assert_eq!(tags, "mixed");
    }

    fn content_tag_for_counts(counts: LogicalTypeCounts) -> String {
        let string_like = counts.string + counts.bytes;
        let numeric_like =
            counts.integer + counts.float + counts.date + counts.datetime + counts.time;
        let total = string_like + numeric_like;
        if string_like as f64 / total as f64 >= 0.7 {
            "string-heavy".to_owned()
        } else if numeric_like as f64 / total as f64 >= 0.7 {
            "numeric-heavy".to_owned()
        } else {
            "mixed".to_owned()
        }
    }
}
