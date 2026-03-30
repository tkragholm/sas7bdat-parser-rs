use sas7bdat_simd::fixture_catalog::{
    FixtureCatalog, FixtureEntry, FixtureProfile, FixtureStatus, build_catalog, discover_fixture_paths,
};
use serde::Serialize;
use std::{collections::BTreeMap, env, fs, path::PathBuf, process::ExitCode};

#[derive(Debug, Serialize)]
struct CorpusProfileOutput {
    roots: Vec<String>,
    sample_rows: usize,
    summary: CorpusSummary,
    fixtures: Vec<FixtureEntry>,
}

#[derive(Debug, Serialize, Default)]
struct CorpusSummary {
    discovered_files: usize,
    profiled_files: usize,
    failed_files: usize,
    total_size_bytes: u64,
    total_rows: u64,
    total_columns: u64,
    total_string_columns: u64,
    total_numeric_like_columns: u64,
    total_sampled_string_cells: u64,
    total_sampled_empty_string_cells: u64,
    total_sampled_ascii_string_cells: u64,
    compression_counts: BTreeMap<String, u64>,
    encoding_counts: BTreeMap<String, u64>,
    tag_counts: BTreeMap<String, u64>,
    top_by_size_bytes: Vec<RankedFile>,
    top_by_row_count: Vec<RankedFile>,
    top_by_column_count: Vec<RankedFile>,
    top_by_string_columns: Vec<RankedFile>,
}

#[derive(Debug, Clone, Serialize)]
struct RankedFile {
    path: String,
    file_name: String,
    value: u64,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> std::result::Result<(), String> {
    let mut args = env::args_os().skip(1);
    let mut sample_rows = 256usize;
    let mut out: Option<PathBuf> = None;
    let mut summary_only = false;
    let mut inputs = Vec::new();

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--sample-rows" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --sample-rows".to_owned());
                };
                sample_rows = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| "invalid --sample-rows value".to_owned())?;
            }
            "--out" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --out".to_owned());
                };
                out = Some(PathBuf::from(value));
            }
            "--summary-only" => {
                summary_only = true;
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            value => inputs.push(PathBuf::from(value)),
        }
    }

    if inputs.is_empty() {
        return Err("usage requires at least one input path".to_owned());
    }

    let roots: Vec<String> = inputs
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    let paths = discover_fixture_paths(&inputs).map_err(|err| err.to_string())?;
    let mut catalog = build_catalog(&paths, sample_rows);
    catalog.roots = roots.clone();

    let summary = summarize_catalog(&catalog);
    let output = CorpusProfileOutput {
        roots,
        sample_rows,
        summary,
        fixtures: if summary_only {
            Vec::new()
        } else {
            catalog.fixtures
        },
    };

    let json = serde_json::to_string_pretty(&output).map_err(|err| err.to_string())?;
    if let Some(path) = out {
        fs::write(path, json).map_err(|err| err.to_string())?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn summarize_catalog(catalog: &FixtureCatalog) -> CorpusSummary {
    let mut summary = CorpusSummary {
        discovered_files: catalog.fixtures.len(),
        ..CorpusSummary::default()
    };

    let mut size_ranked = Vec::new();
    let mut row_ranked = Vec::new();
    let mut column_ranked = Vec::new();
    let mut string_ranked = Vec::new();

    for fixture in &catalog.fixtures {
        summary.total_size_bytes = summary.total_size_bytes.saturating_add(fixture.size_bytes);
        size_ranked.push(RankedFile {
            path: fixture.path.clone(),
            file_name: fixture.file_name.clone(),
            value: fixture.size_bytes,
        });

        match &fixture.status {
            FixtureStatus::Profiled(profile) => {
                summary.profiled_files += 1;
                accumulate_profile(&mut summary, fixture, profile);
                row_ranked.push(RankedFile {
                    path: fixture.path.clone(),
                    file_name: fixture.file_name.clone(),
                    value: profile.row_count,
                });
                column_ranked.push(RankedFile {
                    path: fixture.path.clone(),
                    file_name: fixture.file_name.clone(),
                    value: profile.column_count as u64,
                });
                string_ranked.push(RankedFile {
                    path: fixture.path.clone(),
                    file_name: fixture.file_name.clone(),
                    value: profile.logical_types.string as u64,
                });
            }
            FixtureStatus::Error { .. } => {
                summary.failed_files += 1;
            }
        }
    }

    summary.top_by_size_bytes = top_n(size_ranked, 10);
    summary.top_by_row_count = top_n(row_ranked, 10);
    summary.top_by_column_count = top_n(column_ranked, 10);
    summary.top_by_string_columns = top_n(string_ranked, 10);
    summary
}

fn accumulate_profile(summary: &mut CorpusSummary, fixture: &FixtureEntry, profile: &FixtureProfile) {
    summary.total_rows = summary.total_rows.saturating_add(profile.row_count);
    summary.total_columns = summary.total_columns.saturating_add(profile.column_count as u64);
    summary.total_string_columns = summary
        .total_string_columns
        .saturating_add(profile.logical_types.string as u64);
    let numeric_like = profile.logical_types.integer
        + profile.logical_types.float
        + profile.logical_types.date
        + profile.logical_types.datetime
        + profile.logical_types.time;
    summary.total_numeric_like_columns = summary
        .total_numeric_like_columns
        .saturating_add(numeric_like as u64);
    summary.total_sampled_string_cells = summary
        .total_sampled_string_cells
        .saturating_add(profile.sample.string_cells);
    summary.total_sampled_empty_string_cells = summary
        .total_sampled_empty_string_cells
        .saturating_add(profile.sample.empty_string_cells);
    summary.total_sampled_ascii_string_cells = summary
        .total_sampled_ascii_string_cells
        .saturating_add(profile.sample.ascii_string_cells);

    *summary
        .compression_counts
        .entry(profile.compression.clone())
        .or_default() += 1;
    let encoding = profile
        .encoding
        .clone()
        .unwrap_or_else(|| "unknown".to_owned());
    *summary.encoding_counts.entry(encoding).or_default() += 1;
    for tag in &profile.tags {
        *summary.tag_counts.entry(tag.clone()).or_default() += 1;
    }

    if let Some(source_group) = fixture.path.split('/').nth_back(1) {
        let key = format!("source:{source_group}");
        *summary.tag_counts.entry(key).or_default() += 1;
    }
}

fn top_n(mut ranked: Vec<RankedFile>, n: usize) -> Vec<RankedFile> {
    ranked.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.path.cmp(&right.path))
    });
    ranked.truncate(n);
    ranked
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --bin corpus_profile -- INPUT [INPUT ...] [--sample-rows N] [--summary-only] [--out PATH]"
    );
}
