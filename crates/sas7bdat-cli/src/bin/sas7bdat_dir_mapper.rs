//! Produces a structured map of a directory tree containing SAS7BDAT files.
//!
//! Usage:
//!   sas7bdat-dir-mapper <`root_dir`> [options]
//!
//! Options:
//!   --out <path>          Write output to file instead of stdout
//!   --format json|md|both Output format (default: both)
//!   --jobs <N>            Parallel worker threads (default: all cores)
//!   --min-group-size <N>  Minimum files to form a register group (default: 2)
//!   --relative-paths      Use paths relative to root in output (default: true)
//!   --help / -h           Print this message

use chrono::Utc;
use rayon::prelude::*;
use sas7bdat::Dataset;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as FmtWrite,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};
use walkdir::WalkDir;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Config {
    root: PathBuf,
    out: Option<PathBuf>,
    format: OutputFormat,
    jobs: Option<usize>,
    min_group_size: usize,
    relative_paths: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Json,
    Markdown,
    Both,
}

fn parse_args() -> Result<Config, String> {
    let mut args = env::args_os().skip(1);
    let mut root: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut format = OutputFormat::Both;
    let mut jobs: Option<usize> = None;
    let mut min_group_size = 2usize;
    let mut relative_paths = true;

    while let Some(arg) = args.next() {
        let s = arg.to_string_lossy().into_owned();
        match s.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--out" => {
                out = Some(PathBuf::from(args.next().ok_or("--out requires a value")?));
            }
            "--format" => {
                let v = args
                    .next()
                    .ok_or("--format requires a value")?
                    .to_string_lossy()
                    .into_owned();
                format = match v.as_str() {
                    "json" => OutputFormat::Json,
                    "md" | "markdown" => OutputFormat::Markdown,
                    "both" => OutputFormat::Both,
                    other => return Err(format!("unknown --format value: {other}")),
                };
            }
            "--jobs" => {
                let v = args
                    .next()
                    .ok_or("--jobs requires a value")?
                    .to_string_lossy()
                    .into_owned();
                jobs = Some(v.parse::<usize>().map_err(|_| "--jobs must be a number")?);
            }
            "--min-group-size" => {
                let v = args
                    .next()
                    .ok_or("--min-group-size requires a value")?
                    .to_string_lossy()
                    .into_owned();
                min_group_size = v
                    .parse::<usize>()
                    .map_err(|_| "--min-group-size must be a number")?;
            }
            "--no-relative-paths" => {
                relative_paths = false;
            }
            _ if s.starts_with("--") => return Err(format!("unknown argument: {s}")),
            _ => {
                if root.is_some() {
                    return Err("only one root directory supported".to_owned());
                }
                root = Some(PathBuf::from(&s));
            }
        }
    }

    let root = root.ok_or_else(|| "usage: sas7bdat-dir-mapper <root_dir> [options]".to_owned())?;
    Ok(Config {
        root,
        out,
        format,
        jobs,
        min_group_size,
        relative_paths,
    })
}

fn print_usage() {
    eprintln!(
        "usage: sas7bdat-dir-mapper <root_dir> [--out PATH] [--format json|md|both] \
         [--jobs N] [--min-group-size N] [--no-relative-paths]"
    );
}

// ─── Data model ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnRecord {
    name: String,
    logical_type: String,
    physical_width: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileRecord {
    path: String,
    file_name: String,
    size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    year: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encoding: Option<String>,
    compression: String,
    row_count: u64,
    column_count: usize,
    columns: Vec<ColumnRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnSummary {
    name: String,
    logical_type: String,
    is_core: bool,
    present_in_count: usize,
    present_in_years: Vec<u16>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    absent_in_years: Vec<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    observed_labels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    observed_formats: Vec<String>,
    width_min: u32,
    width_max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypeVariant {
    logical_type: String,
    files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypeConflict {
    column_name: String,
    variants: Vec<TypeVariant>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SchemaStability {
    /// Every file in the group has the identical column set.
    Stable,
    /// Columns only ever appear in later years (schema grows monotonically).
    Growing,
    /// Columns only ever disappear in later years (schema shrinks monotonically).
    Shrinking,
    /// No consistent monotonic trend – columns come and go.
    Volatile,
    /// Only one file in the group; no comparison possible.
    Single,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaAnalysis {
    total_unique_columns: usize,
    core_column_count: usize,
    optional_column_count: usize,
    type_conflict_count: usize,
    schema_stability: SchemaStability,
    columns: Vec<ColumnSummary>,
    type_conflicts: Vec<TypeConflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegisterGroup {
    group_key: String,
    directory: String,
    display_name: String,
    file_count: usize,
    error_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    year_range: Option<[u16; 2]>,
    years: Vec<u16>,
    total_rows: u64,
    total_bytes: u64,
    schema: SchemaAnalysis,
    files: Vec<FileRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestionRecommendation {
    group_key: String,
    strategy: String,
    notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectoryMap {
    root: String,
    generated_at: String,
    total_files: usize,
    total_errors: usize,
    total_rows: u64,
    total_bytes: u64,
    register_groups: Vec<RegisterGroup>,
    singleton_files: Vec<FileRecord>,
    ingestion_recommendations: Vec<IngestionRecommendation>,
}

// ─── Year detection ───────────────────────────────────────────────────────────

/// Describes a found year token within a stem.
#[derive(Debug, Clone, Copy)]
struct YearToken {
    year: u16,
    start: usize,
    len: usize,
}

/// Find the rightmost year-like token in a file stem.
///
/// Accepts 4-digit years (1990–2035) or 2-digit years (90–99, 00–35) that are
/// bordered by a non-digit character (underscore, hyphen, letter boundary).
fn find_year_token(stem: &str) -> Option<YearToken> {
    let bytes = stem.as_bytes();
    let len = bytes.len();
    if len < 2 {
        return None;
    }

    // Prefer 4-digit years first (rightmost).
    if len >= 4 {
        let mut i = len;
        while i >= 4 {
            let window = &bytes[i - 4..i];
            if window.iter().all(u8::is_ascii_digit) {
                let year: u16 = std::str::from_utf8(window)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                if (1990..=2035).contains(&year) {
                    return Some(YearToken {
                        year,
                        start: i - 4,
                        len: 4,
                    });
                }
            }
            i -= 1;
        }
    }

    // Fall back to 2-digit years bordered by a non-digit (e.g., `grf14_`).
    if len >= 2 {
        let mut i = len;
        while i >= 2 {
            let window = &bytes[i - 2..i];
            if window.iter().all(u8::is_ascii_digit) {
                let prev_ok = i < 3 || !bytes[i - 3].is_ascii_digit();
                let next_ok = i >= len || !bytes[i].is_ascii_digit();
                if prev_ok && next_ok {
                    let yy: u16 = std::str::from_utf8(window)
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(200);
                    let year = if yy <= 35 { 2000 + yy } else { 1900 + yy };
                    if (1990..=2035).contains(&year) {
                        return Some(YearToken {
                            year,
                            start: i - 2,
                            len: 2,
                        });
                    }
                }
            }
            i -= 1;
        }
    }

    None
}

fn extract_year(stem: &str) -> Option<u16> {
    find_year_token(stem).map(|t| t.year)
}

/// Strip the year (and optional YYYYMM/YYYYQ suffix) from a stem.
///
/// - `lpr_adm_2010`  → `lpr_adm`   (separator before year is eaten)
/// - `bef_1994`      → `bef`        (same)
/// - `bef201212`     → `bef`        (YYYYMM concatenated, no separator)
/// - `bef200901`     → `bef`        (YYYYMM, month=01)
/// - `mfr2015`       → `mfr`        (YYYY glued to name, no separator)
/// - `grf14_lea`     → `grf_lea`    (2-digit year, trailing sep not eaten to avoid merging)
/// - `bef`           → `bef`        (no year — unchanged)
fn strip_year(stem: &str) -> String {
    let Some(tok) = find_year_token(stem) else {
        return stem.to_owned();
    };

    let bytes = stem.as_bytes();
    let year_end = tok.start + tok.len;

    // Extend to cover an YYYYMM suffix (2 digits, month 01–12) or
    // an YYYYQ suffix (1 digit, quarter 1–4) glued directly to the year.
    let date_end = if year_end + 2 <= bytes.len()
        && bytes[year_end].is_ascii_digit()
        && bytes[year_end + 1].is_ascii_digit()
        && bytes.get(year_end + 2).is_none_or(|b| !b.is_ascii_digit())
    {
        let mm = (bytes[year_end] - b'0') * 10 + (bytes[year_end + 1] - b'0');
        if (1..=12).contains(&mm) {
            year_end + 2
        } else {
            year_end
        }
    } else if year_end < bytes.len()
        && bytes[year_end].is_ascii_digit()
        && bytes.get(year_end + 1).is_none_or(|b| !b.is_ascii_digit())
    {
        let q = bytes[year_end] - b'0';
        if (1..=4).contains(&q) {
            year_end + 1
        } else {
            year_end
        }
    } else {
        year_end
    };

    // Eat the separator that *precedes* the year token (name_YEAR → name).
    // Only eat a trailing separator when the year is at position 0 (YEAR_name → name),
    // to avoid accidentally merging surrounding name segments like grf_lea.
    let mut remove_start = tok.start;
    let mut remove_end = date_end;

    if remove_start > 0 && !bytes[remove_start - 1].is_ascii_alphanumeric() {
        remove_start -= 1;
    } else if remove_start == 0
        && remove_end < bytes.len()
        && !bytes[remove_end].is_ascii_alphanumeric()
    {
        remove_end += 1;
    }

    let mut s = stem.to_owned();
    s.drain(remove_start..remove_end);
    let trimmed = s.trim_matches(|c: char| !c.is_alphanumeric()).to_owned();
    if trimmed.is_empty() {
        stem.to_owned()
    } else {
        trimmed
    }
}

// ─── File scanning ────────────────────────────────────────────────────────────

fn collect_paths(root: &Path) -> Vec<PathBuf> {
    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if !entry.file_type().is_file() {
                return None;
            }
            let path = entry.path();
            let ext = path.extension()?.to_string_lossy().to_lowercase();
            if ext == "sas7bdat" {
                Some(path.to_path_buf())
            } else {
                None
            }
        })
        .collect()
}

const fn logical_type_name(lt: sas7bdat::LogicalType) -> &'static str {
    use sas7bdat::LogicalType;
    match lt {
        LogicalType::Integer => "integer",
        LogicalType::Float => "float",
        LogicalType::String => "string",
        LogicalType::Date => "date",
        LogicalType::DateTime => "datetime",
        LogicalType::Time => "time",
        LogicalType::Bytes => "bytes",
    }
}

const fn compression_name(c: sas7bdat::CompressionKind) -> &'static str {
    use sas7bdat::CompressionKind;
    match c {
        CompressionKind::None => "none",
        CompressionKind::Row => "rle_row",
        CompressionKind::Binary => "rle_binary",
        CompressionKind::Unknown => "unknown",
    }
}

fn probe_file(path: &Path, root: &Path, relative: bool) -> FileRecord {
    let display_path = if relative {
        path.strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    } else {
        path.to_string_lossy().into_owned()
    };
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let size_bytes = fs::metadata(path).map_or(0, |m| m.len());
    let year = extract_year(&stem);

    match Dataset::open(path) {
        Ok(ds) => {
            let meta = ds.metadata();
            let columns = ds
                .columns()
                .iter()
                .map(|c| ColumnRecord {
                    name: c.name.clone(),
                    logical_type: logical_type_name(c.logical_type).to_owned(),
                    physical_width: c.physical_width,
                    label: c.label.clone(),
                    format: c.format.clone(),
                })
                .collect();
            FileRecord {
                path: display_path,
                file_name,
                size_bytes,
                year,
                table_name: meta.table_name.clone(),
                encoding: meta.encoding.clone(),
                compression: compression_name(meta.compression).to_owned(),
                row_count: meta.row_count,
                column_count: ds.columns().len(),
                columns,
                error: None,
            }
        }
        Err(e) => FileRecord {
            path: display_path,
            file_name,
            size_bytes,
            year,
            table_name: None,
            encoding: None,
            compression: "unknown".to_owned(),
            row_count: 0,
            column_count: 0,
            columns: Vec::new(),
            error: Some(e.to_string()),
        },
    }
}

// ─── Grouping ─────────────────────────────────────────────────────────────────

fn group_key(record: &FileRecord, root: &Path, relative: bool) -> String {
    let p = PathBuf::from(&record.path);
    let dir = if relative {
        p.parent()
            .map(|d| d.to_string_lossy().into_owned())
            .unwrap_or_default()
    } else {
        p.parent()
            .and_then(|d| d.strip_prefix(root).ok())
            .or_else(|| p.parent())
            .map(|d| d.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let stem = PathBuf::from(&record.file_name)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let base = strip_year(&stem);
    if dir.is_empty() {
        base
    } else {
        format!("{dir}/{base}")
    }
}

// ─── Schema analysis ──────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn analyze_schema(files: &[FileRecord]) -> SchemaAnalysis {
    let valid: Vec<&FileRecord> = files.iter().filter(|f| f.error.is_none()).collect();
    let file_count = valid.len();

    if file_count == 0 {
        return SchemaAnalysis {
            total_unique_columns: 0,
            core_column_count: 0,
            optional_column_count: 0,
            type_conflict_count: 0,
            schema_stability: SchemaStability::Stable,
            columns: Vec::new(),
            type_conflicts: Vec::new(),
        };
    }

    // column_name → [(file_idx, ColumnRecord)]
    let mut col_occurrences: BTreeMap<String, Vec<(usize, &ColumnRecord)>> = BTreeMap::new();
    for (fi, f) in valid.iter().enumerate() {
        for c in &f.columns {
            col_occurrences
                .entry(c.name.clone())
                .or_default()
                .push((fi, c));
        }
    }

    let all_years: Vec<u16> = {
        let mut y: Vec<u16> = valid.iter().filter_map(|f| f.year).collect();
        y.sort_unstable();
        y.dedup();
        y
    };

    let mut type_conflicts: Vec<TypeConflict> = Vec::new();
    let mut columns: Vec<ColumnSummary> = col_occurrences
        .iter()
        .map(|(name, occurrences)| {
            let present_in_count = occurrences.len();
            let is_core = present_in_count == file_count;

            let mut years_present: Vec<u16> = occurrences
                .iter()
                .filter_map(|(fi, _)| valid[*fi].year)
                .collect();
            years_present.sort_unstable();
            years_present.dedup();

            let absent_in_years: Vec<u16> = all_years
                .iter()
                .filter(|y| !years_present.contains(y))
                .copied()
                .collect();

            // Collect distinct logical types
            let types: BTreeMap<String, Vec<String>> = {
                let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
                for (fi, c) in occurrences {
                    m.entry(c.logical_type.clone())
                        .or_default()
                        .push(valid[*fi].file_name.clone());
                }
                m
            };
            let has_conflict = types.len() > 1;
            if has_conflict {
                type_conflicts.push(TypeConflict {
                    column_name: name.clone(),
                    variants: types
                        .into_iter()
                        .map(|(t, fs)| TypeVariant {
                            logical_type: t,
                            files: fs,
                        })
                        .collect(),
                });
            }

            let dominant_type = occurrences
                .iter()
                .max_by_key(|(_, c)| c.logical_type.as_str())
                .map(|(_, c)| c.logical_type.clone())
                .unwrap_or_default();

            // Labels and formats
            let labels: BTreeSet<String> = occurrences
                .iter()
                .filter_map(|(_, c)| c.label.clone())
                .collect();
            let formats: BTreeSet<String> = occurrences
                .iter()
                .filter_map(|(_, c)| c.format.clone())
                .collect();
            let widths: Vec<u32> = occurrences.iter().map(|(_, c)| c.physical_width).collect();
            let width_min = widths.iter().copied().min().unwrap_or(0);
            let width_max = widths.iter().copied().max().unwrap_or(0);

            let label = labels.iter().next().cloned();
            let format = formats.iter().next().cloned();
            let observed_labels: Vec<String> = if labels.len() > 1 {
                labels.into_iter().collect()
            } else {
                Vec::new()
            };
            let observed_formats: Vec<String> = if formats.len() > 1 {
                formats.into_iter().collect()
            } else {
                Vec::new()
            };

            ColumnSummary {
                name: name.clone(),
                logical_type: if has_conflict {
                    "mixed".to_owned()
                } else {
                    dominant_type
                },
                is_core,
                present_in_count,
                present_in_years: years_present,
                absent_in_years,
                label,
                observed_labels,
                format,
                observed_formats,
                width_min,
                width_max,
            }
        })
        .collect();

    // Sort: core columns first, then alphabetical within each group
    columns.sort_by(|a, b| b.is_core.cmp(&a.is_core).then_with(|| a.name.cmp(&b.name)));

    let core_column_count = columns.iter().filter(|c| c.is_core).count();
    let optional_column_count = columns.len() - core_column_count;

    // Schema stability
    let stability = if file_count == 1 {
        SchemaStability::Single
    } else if optional_column_count == 0 && type_conflicts.is_empty() {
        SchemaStability::Stable
    } else {
        classify_stability(files, &col_occurrences, file_count)
    };

    SchemaAnalysis {
        total_unique_columns: columns.len(),
        core_column_count,
        optional_column_count,
        type_conflict_count: type_conflicts.len(),
        schema_stability: stability,
        columns,
        type_conflicts,
    }
}

fn classify_stability(
    files: &[FileRecord],
    col_occurrences: &BTreeMap<String, Vec<(usize, &ColumnRecord)>>,
    file_count: usize,
) -> SchemaStability {
    // Only possible to classify monotonic trend when years are available
    let mut sorted: Vec<&FileRecord> = files
        .iter()
        .filter(|f| f.error.is_none() && f.year.is_some())
        .collect();
    if sorted.len() < 2 {
        return SchemaStability::Volatile;
    }
    sorted.sort_by_key(|f| f.year);

    // For each consecutive pair, check whether columns only appear or disappear
    let mut ever_grew = false;
    let mut ever_shrank = false;

    let col_to_file_set: BTreeMap<&str, BTreeSet<usize>> = col_occurrences
        .iter()
        .map(|(name, occ)| {
            let s: BTreeSet<usize> = occ.iter().map(|(fi, _)| *fi).collect();
            (name.as_str(), s)
        })
        .collect();

    // Build per-file ordered column sets
    // file index in sorted order → set of column names
    let file_indices: Vec<usize> = sorted
        .iter()
        .map(|f| {
            files
                .iter()
                .position(|r| std::ptr::eq(*f, r))
                .unwrap_or(file_count)
        })
        .collect();

    let file_col_sets: Vec<BTreeSet<&str>> = file_indices
        .iter()
        .map(|&fi| {
            col_to_file_set
                .iter()
                .filter_map(|(&name, set)| if set.contains(&fi) { Some(name) } else { None })
                .collect()
        })
        .collect();

    for window in file_col_sets.windows(2) {
        let prev = &window[0];
        let next = &window[1];
        let added = next.difference(prev).count();
        let removed = prev.difference(next).count();
        if added > 0 {
            ever_grew = true;
        }
        if removed > 0 {
            ever_shrank = true;
        }
    }

    match (ever_grew, ever_shrank) {
        (true, false) => SchemaStability::Growing,
        (false, true) => SchemaStability::Shrinking,
        (false, false) => SchemaStability::Stable,
        (true, true) => SchemaStability::Volatile,
    }
}

// ─── Ingestion recommendations ────────────────────────────────────────────────

fn ingestion_recommendations(groups: &[RegisterGroup]) -> Vec<IngestionRecommendation> {
    groups
        .iter()
        .map(|g| {
            let schema = &g.schema;

            let mut notes: Vec<String> = Vec::new();

            if g.error_count > 0 {
                notes.push(format!(
                    "{} file(s) failed to parse — exclude or investigate separately",
                    g.error_count
                ));
            }

            if !schema.type_conflicts.is_empty() {
                let names: Vec<&str> = schema
                    .type_conflicts
                    .iter()
                    .map(|tc| tc.column_name.as_str())
                    .collect();
                notes.push(format!(
                    "Type conflicts on columns [{}] — cast before concat",
                    names.join(", ")
                ));
            }

            let strategy = match schema.schema_stability {
                SchemaStability::Stable => {
                    notes.push("Identical schema across all years — use a single fixed schema.".to_owned());
                    "fixed_schema".to_owned()
                }
                SchemaStability::Growing => {
                    notes.push(format!(
                        "{} optional columns not present in earliest files — read with schema union, fill missing with null.",
                        schema.optional_column_count
                    ));
                    "schema_union_nullable".to_owned()
                }
                SchemaStability::Shrinking => {
                    notes.push(format!(
                        "{} columns absent in newer files — read with schema union, fill missing with null.",
                        schema.optional_column_count
                    ));
                    "schema_union_nullable".to_owned()
                }
                SchemaStability::Volatile => {
                    notes.push(format!(
                        "{} optional columns with no monotonic trend — use a full schema union and expect nulls in many cells.",
                        schema.optional_column_count
                    ));
                    "schema_union_nullable_volatile".to_owned()
                }
                SchemaStability::Single => {
                    notes.push("Single file — no longitudinal comparison possible.".to_owned());
                    "single_file".to_owned()
                }
            };

            if let Some([y_min, y_max]) = g.year_range {
                notes.push(format!("Year span: {y_min}–{y_max} ({} years, {} files)", g.years.len(), g.file_count));
            }

            IngestionRecommendation {
                group_key: g.group_key.clone(),
                strategy,
                notes,
            }
        })
        .collect()
}

// ─── Markdown rendering ───────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn render_markdown(map: &DirectoryMap) -> String {
    let mut out = String::new();

    writeln!(out, "# SAS7BDAT Directory Map").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**Root:** `{}`  ", map.root).unwrap();
    writeln!(out, "**Generated:** {}  ", map.generated_at).unwrap();
    writeln!(
        out,
        "**Files:** {} ({} errors)  ",
        map.total_files, map.total_errors
    )
    .unwrap();
    writeln!(out, "**Total rows:** {}  ", fmt_large(map.total_rows)).unwrap();
    writeln!(out, "**Total size:** {}", fmt_bytes(map.total_bytes)).unwrap();
    writeln!(out).unwrap();

    // Summary table
    writeln!(
        out,
        "## Register Groups ({} groups)",
        map.register_groups.len()
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "| Group | Files | Year range | Rows | Cols (core/total) | Stability | Conflicts |"
    )
    .unwrap();
    writeln!(
        out,
        "|-------|-------|-----------|------|-------------------|-----------|-----------|"
    )
    .unwrap();
    for g in &map.register_groups {
        let yr = g
            .year_range
            .map_or_else(|| "—".to_owned(), |[a, b]| format!("{a}–{b}"));
        let cols = format!(
            "{}/{}",
            g.schema.core_column_count, g.schema.total_unique_columns
        );
        let stability = format!("{:?}", g.schema.schema_stability).to_lowercase();
        let conflicts = if g.schema.type_conflict_count > 0 {
            format!("⚠ {}", g.schema.type_conflict_count)
        } else {
            "✓".to_owned()
        };
        writeln!(
            out,
            "| `{}` | {} | {} | {} | {} | {} | {} |",
            g.display_name,
            g.file_count,
            yr,
            fmt_large(g.total_rows),
            cols,
            stability,
            conflicts
        )
        .unwrap();
    }
    writeln!(out).unwrap();

    // Per-group details
    writeln!(out, "## Group Details").unwrap();
    for g in &map.register_groups {
        writeln!(out).unwrap();
        writeln!(out, "### `{}`", g.group_key).unwrap();
        writeln!(out).unwrap();
        writeln!(out, "**Directory:** `{}`  ", g.directory).unwrap();
        writeln!(out, "**Files:** {}  ", g.file_count).unwrap();
        if let Some([y_min, y_max]) = g.year_range {
            writeln!(
                out,
                "**Years:** {y_min}–{y_max} ({})  ",
                g.years
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .unwrap();
        }
        writeln!(out, "**Rows:** {}  ", fmt_large(g.total_rows)).unwrap();
        writeln!(out, "**Size:** {}  ", fmt_bytes(g.total_bytes)).unwrap();
        writeln!(
            out,
            "**Schema stability:** {:?}  ",
            g.schema.schema_stability
        )
        .unwrap();

        if !g.schema.type_conflicts.is_empty() {
            writeln!(out).unwrap();
            writeln!(out, "#### Type Conflicts").unwrap();
            writeln!(out).unwrap();
            for tc in &g.schema.type_conflicts {
                writeln!(out, "- **`{}`**", tc.column_name).unwrap();
                for v in &tc.variants {
                    writeln!(
                        out,
                        "  - `{}` in: {}",
                        v.logical_type,
                        v.files
                            .iter()
                            .map(|f| format!("`{f}`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                    .unwrap();
                }
            }
        }

        writeln!(out).unwrap();
        writeln!(out, "#### Column Schema").unwrap();
        writeln!(out).unwrap();
        writeln!(out, "| Column | Type | Core | Width | Present | Label |").unwrap();
        writeln!(out, "|--------|------|------|-------|---------|-------|").unwrap();
        for col in &g.schema.columns {
            let core = if col.is_core { "✓" } else { "—" };
            let label = col.label.as_deref().unwrap_or("");
            let width = if col.width_min == col.width_max {
                col.width_min.to_string()
            } else {
                format!("{}-{}", col.width_min, col.width_max)
            };
            let present = if col.is_core {
                "all".to_owned()
            } else {
                format!("{}/{}", col.present_in_count, g.file_count)
            };
            writeln!(
                out,
                "| `{}` | {} | {} | {} | {} | {} |",
                col.name, col.logical_type, core, width, present, label
            )
            .unwrap();
        }

        writeln!(out).unwrap();
        writeln!(out, "#### Files").unwrap();
        writeln!(out).unwrap();
        for f in &g.files {
            let yr = f.year.map_or_else(|| "—".to_owned(), |y| y.to_string());
            let err = f
                .error
                .as_ref()
                .map_or_else(String::new, |e| format!(" ⚠ `{e}`"));
            writeln!(
                out,
                "- `{}` — year: {}, rows: {}, cols: {}, size: {}{}",
                f.file_name,
                yr,
                fmt_large(f.row_count),
                f.column_count,
                fmt_bytes(f.size_bytes),
                err
            )
            .unwrap();
        }
    }

    // Singletons
    if !map.singleton_files.is_empty() {
        writeln!(out).unwrap();
        writeln!(out, "## Singleton Files ({})", map.singleton_files.len()).unwrap();
        for f in &map.singleton_files {
            writeln!(out).unwrap();
            writeln!(out, "### `{}`", f.file_name).unwrap();
            writeln!(out).unwrap();
            writeln!(out, "**Path:** `{}`  ", f.path).unwrap();
            writeln!(out, "**Rows:** {}  ", fmt_large(f.row_count)).unwrap();
            writeln!(out, "**Size:** {}  ", fmt_bytes(f.size_bytes)).unwrap();
            if let Some(enc) = &f.encoding {
                writeln!(out, "**Encoding:** {enc}  ").unwrap();
            }
            if let Some(err) = &f.error {
                writeln!(out).unwrap();
                writeln!(out, "> **Parse error:** {err}").unwrap();
            } else if !f.columns.is_empty() {
                writeln!(out).unwrap();
                writeln!(out, "| Column | Type | Width | Label |").unwrap();
                writeln!(out, "|--------|------|-------|-------|").unwrap();
                for col in &f.columns {
                    let label = col.label.as_deref().unwrap_or("");
                    writeln!(
                        out,
                        "| `{}` | {} | {} | {} |",
                        col.name, col.logical_type, col.physical_width, label
                    )
                    .unwrap();
                }
            }
        }
    }

    // Ingestion plan
    writeln!(out).unwrap();
    writeln!(out, "## Python Ingestion Recommendations").unwrap();
    for rec in &map.ingestion_recommendations {
        writeln!(out).unwrap();
        writeln!(out, "### `{}`", rec.group_key).unwrap();
        writeln!(out, "**Strategy:** `{}`  ", rec.strategy).unwrap();
        for note in &rec.notes {
            writeln!(out, "- {note}").unwrap();
        }
    }

    out
}

fn fmt_large(n: u64) -> String {
    // Insert thousands separators
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push('_');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[allow(clippy::cast_precision_loss)]
fn fmt_bytes(b: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if b >= GB {
        format!("{:.1} GiB", b as f64 / GB as f64)
    } else if b >= MB {
        format!("{:.1} MiB", b as f64 / MB as f64)
    } else if b >= KB {
        format!("{:.1} KiB", b as f64 / KB as f64)
    } else {
        format!("{b} B")
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn run(config: &Config) -> Result<(), String> {
    if let Some(jobs) = config.jobs {
        rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build_global()
            .map_err(|e| e.to_string())?;
    }

    let root = &config.root;
    if !root.exists() {
        return Err(format!("root path does not exist: {}", root.display()));
    }

    eprintln!("Discovering .sas7bdat files in {} ...", root.display());
    let paths = collect_paths(root);
    let total = paths.len();
    eprintln!("Found {total} files. Extracting metadata in parallel...");

    let records: Vec<FileRecord> = paths
        .par_iter()
        .map(|p| probe_file(p, root, config.relative_paths))
        .collect();

    eprintln!("Metadata extraction complete. Grouping...");

    // Group by (dir, base_stem_without_year)
    let mut groups: BTreeMap<String, Vec<FileRecord>> = BTreeMap::new();
    for record in records {
        let key = group_key(&record, root, config.relative_paths);
        groups.entry(key).or_default().push(record);
    }

    // Split into proper groups (≥ min_group_size) and singletons
    let mut register_groups: Vec<RegisterGroup> = Vec::new();
    let mut singleton_files: Vec<FileRecord> = Vec::new();

    for (key, mut files) in groups {
        files.sort_by(|a, b| {
            a.year
                .cmp(&b.year)
                .then_with(|| a.file_name.cmp(&b.file_name))
        });

        let dir = {
            let p = PathBuf::from(&files[0].path);
            p.parent()
                .map(|d| d.to_string_lossy().into_owned())
                .unwrap_or_default()
        };
        let display_name = key.rsplit('/').next().unwrap_or(&key).to_owned();

        if files.len() < config.min_group_size {
            singleton_files.extend(files);
            continue;
        }

        let years: Vec<u16> = {
            let mut y: Vec<u16> = files.iter().filter_map(|f| f.year).collect();
            y.sort_unstable();
            y.dedup();
            y
        };
        let year_range = if years.is_empty() {
            None
        } else {
            Some([*years.first().unwrap(), *years.last().unwrap()])
        };
        let total_rows: u64 = files.iter().map(|f| f.row_count).sum();
        let total_bytes: u64 = files.iter().map(|f| f.size_bytes).sum();
        let error_count = files.iter().filter(|f| f.error.is_some()).count();
        let schema = analyze_schema(&files);
        let file_count = files.len();

        register_groups.push(RegisterGroup {
            group_key: key,
            directory: dir,
            display_name,
            file_count,
            error_count,
            year_range,
            years,
            total_rows,
            total_bytes,
            schema,
            files,
        });
    }

    // Sort groups by directory then name
    register_groups.sort_by(|a, b| a.group_key.cmp(&b.group_key));
    singleton_files.sort_by(|a, b| a.path.cmp(&b.path));

    let total_errors: usize = register_groups.iter().map(|g| g.error_count).sum::<usize>()
        + singleton_files.iter().filter(|f| f.error.is_some()).count();

    let total_rows: u64 = register_groups.iter().map(|g| g.total_rows).sum::<u64>()
        + singleton_files.iter().map(|f| f.row_count).sum::<u64>();

    let total_bytes: u64 = register_groups.iter().map(|g| g.total_bytes).sum::<u64>()
        + singleton_files.iter().map(|f| f.size_bytes).sum::<u64>();

    let ingestion_recommendations = ingestion_recommendations(&register_groups);

    let generated_at = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let map = DirectoryMap {
        root: root.display().to_string(),
        generated_at,
        total_files: total,
        total_errors,
        total_rows,
        total_bytes,
        register_groups,
        singleton_files,
        ingestion_recommendations,
    };

    eprintln!(
        "Done. {} groups, {} singletons, {} errors.",
        map.register_groups.len(),
        map.singleton_files.len(),
        map.total_errors
    );

    let base_out = config
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from("dir_map"));

    match config.format {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&map).map_err(|e| e.to_string())?;
            let path = base_out.with_extension("json");
            fs::write(&path, &json).map_err(|e| e.to_string())?;
            eprintln!("JSON written to {}", path.display());
        }
        OutputFormat::Markdown => {
            let md = render_markdown(&map);
            let path = base_out.with_extension("md");
            fs::write(&path, &md).map_err(|e| e.to_string())?;
            eprintln!("Markdown written to {}", path.display());
        }
        OutputFormat::Both => {
            let json = serde_json::to_string_pretty(&map).map_err(|e| e.to_string())?;
            let json_path = base_out.with_extension("json");
            fs::write(&json_path, &json).map_err(|e| e.to_string())?;
            eprintln!("JSON written to {}", json_path.display());

            let md = render_markdown(&map);
            let md_path = base_out.with_extension("md");
            fs::write(&md_path, &md).map_err(|e| e.to_string())?;
            eprintln!("Markdown written to {}", md_path.display());
        }
    }

    Ok(())
}

fn main() -> ExitCode {
    let config = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    match run(&config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sy(s: &str) -> String {
        strip_year(s)
    }

    fn ey(s: &str) -> Option<u16> {
        extract_year(s)
    }

    #[test]
    fn year_extraction() {
        assert_eq!(ey("bef201212"), Some(2012));
        assert_eq!(ey("bef_1994"), Some(1994));
        assert_eq!(ey("bef"), None);
        assert_eq!(ey("lpr_adm_2010"), Some(2010));
        assert_eq!(ey("mfr2015"), Some(2015));
        assert_eq!(ey("grf14_lea_blkgrp"), Some(2014));
        assert_eq!(ey("t_person201312"), Some(2013));
        assert_eq!(ey("ind_akm201512"), Some(2015));
    }

    #[test]
    fn year_stripping() {
        // Separator before year
        assert_eq!(sy("bef_1994"), "bef");
        assert_eq!(sy("lpr_adm_2010"), "lpr_adm");
        assert_eq!(sy("lpr_adm_2011"), "lpr_adm");

        // YYYYMM concatenated (no separator)
        assert_eq!(sy("bef201212"), "bef");
        assert_eq!(sy("bef200901"), "bef");
        assert_eq!(sy("t_person201312"), "t_person");
        assert_eq!(sy("ind_akm201512"), "ind_akm");

        // Plain YYYY concatenated
        assert_eq!(sy("mfr2015"), "mfr");

        // 2-digit year with trailing separator — must not merge surrounding segments
        assert_eq!(sy("grf14_lea_blkgrp"), "grf_lea_blkgrp");
        assert_eq!(sy("grf15_lea_blkgrp"), "grf_lea_blkgrp");

        // No year — unchanged
        assert_eq!(sy("bef"), "bef");
        assert_eq!(sy("lpr_adm"), "lpr_adm");
    }

    #[test]
    fn grouping_consistency() {
        // All three bef variants must produce the same group base
        let base: Vec<_> = ["bef201212", "bef_1994", "bef", "bef200901"]
            .iter()
            .map(|s| sy(s))
            .collect();
        assert!(
            base.iter().all(|b| b == "bef"),
            "expected all bef bases to be 'bef', got {base:?}"
        );

        // lpr variants
        let lpr: Vec<_> = ["lpr_adm_2010", "lpr_adm_2015", "lpr_adm_2022"]
            .iter()
            .map(|s| sy(s))
            .collect();
        assert!(lpr.iter().all(|b| b == "lpr_adm"), "{lpr:?}");
    }
}
