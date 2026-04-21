use super::{
    CorpusCsvContext, CorpusCsvRow, CorpusProfileOutput, CorpusSummary, FixtureCatalog,
    FixtureEntry, FixtureProfile, FixtureStatus, bytes_to_megabytes, content_class, encoding_class,
    join_named_counts, round_metric, size_class, summary_txt_path, width_class,
};
use csv::Writer;
use std::{fs, path::PathBuf};

pub fn write_json(
    catalog: &FixtureCatalog,
    roots: &[String],
    sample_rows: usize,
    summary: &CorpusSummary,
    summary_only: bool,
    out: Option<PathBuf>,
) -> std::result::Result<(), String> {
    let output = CorpusProfileOutput {
        roots: roots.to_vec(),
        sample_rows,
        summary: summary.clone(),
        fixtures: if summary_only {
            Vec::new()
        } else {
            catalog.fixtures.clone()
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

pub fn write_csv(
    catalog: &FixtureCatalog,
    roots: &[String],
    sample_rows: usize,
    summary: &CorpusSummary,
    summary_only: bool,
    out: Option<PathBuf>,
) -> std::result::Result<(), String> {
    let context = CorpusCsvContext {
        roots: roots.join("|"),
        sample_rows,
    };
    let rows = build_csv_rows(&catalog.fixtures, &context, summary_only);

    if let Some(path) = out {
        let mut writer = Writer::from_path(&path).map_err(|err| err.to_string())?;
        for row in rows {
            writer.serialize(row).map_err(|err| err.to_string())?;
        }
        writer.flush().map_err(|err| err.to_string())?;
        let summary_path = summary_txt_path(&path);
        fs::write(
            summary_path,
            super::corpus_render::render_summary_txt(summary, roots, sample_rows),
        )
        .map_err(|err| err.to_string())
    } else {
        let stdout = std::io::stdout();
        let mut writer = Writer::from_writer(stdout.lock());
        for row in rows {
            writer.serialize(row).map_err(|err| err.to_string())?;
        }
        writer.flush().map_err(|err| err.to_string())
    }
}

pub fn build_csv_rows(
    fixtures: &[FixtureEntry],
    context: &CorpusCsvContext,
    summary_only: bool,
) -> Vec<CorpusCsvRow> {
    if summary_only {
        return vec![build_csv_row(None, context)];
    }
    fixtures
        .iter()
        .map(|fixture| build_csv_row(Some(fixture), context))
        .collect()
}

pub fn build_csv_row(fixture: Option<&FixtureEntry>, context: &CorpusCsvContext) -> CorpusCsvRow {
    let mut row = CorpusCsvRow {
        corpus_roots: context.roots.clone(),
        corpus_sample_rows: context.sample_rows,
        ..CorpusCsvRow::default()
    };

    let Some(fixture) = fixture else {
        return row;
    };

    row.path.clone_from(&fixture.path);
    row.file_name.clone_from(&fixture.file_name);
    row.source_group.clone_from(&fixture.source_group);
    row.size_megabytes = bytes_to_megabytes(fixture.size_bytes);

    match &fixture.status {
        FixtureStatus::Profiled(profile) => fill_profile_row(&mut row, profile),
        FixtureStatus::Error { error } => fill_error_row(&mut row, error),
    }

    row
}

fn fill_profile_row(row: &mut CorpusCsvRow, profile: &FixtureProfile) {
    row.status.clear();
    row.status.push_str("profiled");
    if let Some(table_name) = &profile.table_name {
        row.table_name.clone_from(table_name);
    }
    if let Some(encoding) = &profile.encoding {
        row.encoding.clone_from(encoding);
    }
    row.compression.clone_from(&profile.compression);
    row.row_count = profile.row_count;
    row.column_count = profile.column_count;
    row.row_len = profile.row_len;
    row.page_size = profile.page_size;
    row.page_count = profile.page_count;
    row.string_columns = profile.logical_types.string;
    row.integer_columns = profile.logical_types.integer;
    row.float_columns = profile.logical_types.float;
    row.date_columns = profile.logical_types.date;
    row.datetime_columns = profile.logical_types.datetime;
    row.time_columns = profile.logical_types.time;
    row.bytes_columns = profile.logical_types.bytes;
    row.numeric_like_columns = profile.logical_types.integer
        + profile.logical_types.float
        + profile.logical_types.date
        + profile.logical_types.datetime
        + profile.logical_types.time;
    row.string_width_sum = profile.widths.string_width_sum;
    row.string_width_max = profile.widths.string_width_max;
    row.numeric_width_sum = profile.widths.numeric_width_sum;
    row.numeric_width_max = profile.widths.numeric_width_max;
    row.date_format_columns = profile.temporal_formats.date_format_columns;
    row.datetime_format_columns = profile.temporal_formats.datetime_format_columns;
    row.time_format_columns = profile.temporal_formats.time_format_columns;
    row.date_formats = join_named_counts(&profile.temporal_formats.date_formats);
    row.datetime_formats = join_named_counts(&profile.temporal_formats.datetime_formats);
    row.time_formats = join_named_counts(&profile.temporal_formats.time_formats);
    row.rows_sampled = profile.sample.rows_sampled;
    row.string_cells = profile.sample.string_cells;
    row.empty_string_cells = profile.sample.empty_string_cells;
    row.empty_string_ratio = round_metric(profile.sample.empty_string_ratio());
    row.ascii_string_cells = profile.sample.ascii_string_cells;
    row.ascii_ratio = round_metric(profile.sample.ascii_ratio());
    row.non_ascii_string_cells = profile.sample.non_ascii_string_cells;
    row.avg_trimmed_string_len = round_metric(profile.sample.avg_trimmed_string_len());
    row.max_trimmed_string_len = profile.sample.max_trimmed_string_len;
    row.numeric_like_cells = profile.sample.numeric_like_cells;
    row.null_numeric_like_cells = profile.sample.null_numeric_like_cells;
    row.missing_numeric_ratio = round_metric(profile.sample.missing_numeric_ratio());
    row.compression_class.clone_from(&profile.compression);
    row.encoding_class = encoding_class(profile);
    row.size_class = size_class(profile);
    row.width_class = width_class(profile);
    row.content_class = content_class(profile);
    row.categorical_heavy = profile.tags.iter().any(|tag| tag == "categorical-heavy");
}

fn fill_error_row(row: &mut CorpusCsvRow, error: &str) {
    row.status.clear();
    row.status.push_str("error");
    row.error.clear();
    row.error.push_str(error);
    row.compression_class.clear();
    row.compression_class.push_str("unknown");
    row.encoding_class.clear();
    row.encoding_class.push_str("unknown");
    row.size_class.clear();
    row.size_class.push_str("unknown");
    row.width_class.clear();
    row.width_class.push_str("unknown");
    row.content_class.clear();
    row.content_class.push_str("unknown");
}
