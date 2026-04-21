use crate::catalog::Catalog;
use crate::cli::{ConvertArgs, ProgressMode, SinkKind};
use crate::export::{
    DelimitedWriteOptions, ScanOptions, WriteOptions, write_csv_or_tsv, write_parquet,
};
use crate::paths::{compute_output_path, discover_inputs, validate_convert_args};
use crate::selection::{
    ColumnSelection, RowWindow, projection_from_selection, row_selection_from_window,
};
use anyhow::{Result, anyhow};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rayon::prelude::*;
use sas7bdat_simd::{Dataset, OpenOptions, ValidationMode};
use std::fs;
use std::io::IsTerminal;
use std::path::Path;

/// # Errors
///
/// Returns an error if input discovery, catalog loading, conversion, or writing fails.
pub fn run_convert(args: &ConvertArgs) -> Result<()> {
    let files = discover_inputs(&args.inputs, args.recursive)?;
    validate_convert_args(args, files.len())?;
    let catalog = if let Some(path) = &args.catalog {
        Some(Catalog::load(path)?)
    } else {
        None
    };
    let progress = ProgressState::new(args, files.len());

    if let Some(jobs) = args.execution.jobs {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs.max(1))
            .build()?;
        let results = pool.install(|| {
            files
                .par_iter()
                .map(|(root, input)| {
                    convert_one(root, input, args, catalog.as_ref(), progress.as_ref())
                        .map_err(|err| anyhow!("{}: {err}", input.display()))
                })
                .collect::<Vec<_>>()
        });
        finish_progress(progress.as_ref());
        report_failures(&results)
    } else if args.execution.fail_fast {
        let result = files.iter().try_for_each(|(root, input)| {
            convert_one(root, input, args, catalog.as_ref(), progress.as_ref())
                .map_err(|err| anyhow!("{}: {err}", input.display()))
        });
        finish_progress(progress.as_ref());
        result
    } else {
        let mut failures = 0usize;
        for (root, input) in &files {
            if let Err(err) = convert_one(root, input, args, catalog.as_ref(), progress.as_ref()) {
                failures = failures.saturating_add(1);
                eprintln!("{}: {err}", input.display());
            }
        }
        finish_progress(progress.as_ref());
        if failures > 0 {
            Err(anyhow!("completed with {failures} failures"))
        } else {
            Ok(())
        }
    }
}

fn convert_one(
    root: &Path,
    input: &Path,
    args: &ConvertArgs,
    catalog: Option<&Catalog>,
    progress: Option<&ProgressState>,
) -> Result<()> {
    if let Some(progress) = progress {
        progress.set_message(input.display().to_string());
    }
    let output = args.output.out.as_ref().map_or_else(
        || compute_output_path(root, input, args),
        std::clone::Clone::clone,
    );
    if output.exists() && !args.output.overwrite {
        return Err(anyhow!(
            "output already exists (use --overwrite): {}",
            output.display()
        ));
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }

    let dataset = open_dataset(input, args)?;
    let columns = ColumnSelection {
        names: args.columns.as_deref(),
        indices: args.column_indices.as_deref(),
    };
    let projection = projection_from_selection(&dataset, columns)?;
    let selection = row_selection_from_window(
        RowWindow::new(args.skip, args.max_rows),
        dataset.metadata().row_count,
    );
    let scan = ScanOptions {
        selection,
        projection: projection.as_ref(),
        parse_threads: args.execution.parse_threads,
    };

    match args.output.sink {
        SinkKind::Parquet => {
            let row_group_rows = match (
                args.output.parquet_row_group_size,
                args.output.parquet_target_bytes,
            ) {
                (Some(rows), _) => Some(rows),
                (None, Some(bytes)) => {
                    let row_len = usize::try_from(dataset.metadata().row_len)
                        .unwrap_or(0)
                        .max(1);
                    Some((bytes / row_len).max(1))
                }
                (None, None) => None,
            };
            write_parquet(
                &dataset,
                &output,
                WriteOptions {
                    row_group_rows,
                    batch_rows: row_group_rows,
                    scan,
                    catalog,
                },
            )
        }
        SinkKind::Csv => {
            let delimiter = args.output.delimiter.unwrap_or(',') as u8;
            write_csv_or_tsv(
                &dataset,
                &output,
                DelimitedWriteOptions {
                    delimiter,
                    headers: args.output.headers && !args.output.no_headers,
                    scan,
                },
            )
        }
        SinkKind::Tsv => {
            let delimiter = args.output.delimiter.unwrap_or('\t') as u8;
            write_csv_or_tsv(
                &dataset,
                &output,
                DelimitedWriteOptions {
                    delimiter,
                    headers: args.output.headers && !args.output.no_headers,
                    scan,
                },
            )
        }
    }?;

    if let Some(progress) = progress {
        progress.tick_done();
    }
    if !args.ui.quiet && !should_show_progress(args) {
        println!("{} -> {}", input.display(), output.display());
    }
    Ok(())
}

fn open_dataset(input: &Path, args: &ConvertArgs) -> Result<Dataset> {
    let open = OpenOptions::builder()
        .validation(if args.validation.strict_dates {
            ValidationMode::Strict
        } else {
            ValidationMode::Permissive
        })
        .build();
    Dataset::open_with(input, open).map_err(Into::into)
}

fn report_failures(results: &[Result<()>]) -> Result<()> {
    let failures = results.iter().filter(|result| result.is_err()).count();
    if failures > 0 {
        Err(anyhow!("completed with {failures} failures"))
    } else {
        Ok(())
    }
}

fn finish_progress(progress: Option<&ProgressState>) {
    if let Some(progress) = progress {
        progress.finish();
    }
}

fn should_show_progress(args: &ConvertArgs) -> bool {
    if args.ui.quiet {
        return false;
    }
    match args.ui.progress {
        ProgressMode::Never => false,
        ProgressMode::Always => true,
        ProgressMode::Auto => std::io::stderr().is_terminal(),
    }
}

struct ProgressState {
    multi: MultiProgress,
    overall: ProgressBar,
}

impl ProgressState {
    fn new(args: &ConvertArgs, task_count: usize) -> Option<Self> {
        if !should_show_progress(args) {
            return None;
        }
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::stderr_with_hz(20));
        let overall = multi.add(ProgressBar::new(task_count as u64));
        overall.set_style(
            ProgressStyle::with_template("{spinner} {msg} {bar} {pos}/{len} files ({eta})")
                .ok()?
                .progress_chars("=>-")
                .tick_chars("-\\|/"),
        );
        overall.set_message("Converting");
        Some(Self { multi, overall })
    }

    fn set_message(&self, message: String) {
        self.overall.set_message(message);
    }

    fn tick_done(&self) {
        self.overall.inc(1);
    }

    fn finish(&self) {
        self.overall.finish_and_clear();
        let _ = self.multi.clear();
    }
}
