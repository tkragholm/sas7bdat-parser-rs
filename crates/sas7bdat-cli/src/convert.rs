use crate::catalog::Catalog;
use crate::cli::{ConvertArgs, ProgressMode, SinkKind};
use crate::export::{
    DelimitedWriteOptions, ScanOptions, WriteOptions, write_csv_or_tsv, write_parquet,
};
use crate::friendly;
use crate::paths::{compute_output_path, discover_inputs, validate_convert_args};
use crate::selection::{
    ColumnSelection, RowWindow, projection_from_selection, resolve_column_indices,
    row_selection_from_window,
};
use crate::style::Style;
use anyhow::{Result, anyhow};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rayon::prelude::*;
use sas7bdat::{Dataset, OpenOptions, ScanProgress, ScanProgressObserver, ValidationMode};
use std::fs;
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// What one successful conversion produced, for the aggregate summary.
#[derive(Clone, Copy)]
struct ConvertOutcome {
    rows: u64,
    bytes: u64,
}

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
    // Per-file success lines only when there's no progress bar to corrupt.
    let print_each = !args.ui.quiet && progress.is_none();
    let started = Instant::now();

    let run_one = |root: &Path, input: &Path| -> (std::path::PathBuf, Result<ConvertOutcome>) {
        let result = convert_one(
            root,
            input,
            args,
            catalog.as_ref(),
            print_each,
            progress.as_ref(),
        );
        if let Some(progress) = progress.as_ref() {
            progress.tick();
            if result.is_err() {
                progress.record_failure();
            }
        }
        (input.to_path_buf(), result)
    };

    let outcomes: Vec<(std::path::PathBuf, Result<ConvertOutcome>)> =
        if let Some(jobs) = args.execution.jobs {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(jobs.max(1))
                .build()?;
            pool.install(|| {
                files
                    .par_iter()
                    .map(|(root, input)| run_one(root, input))
                    .collect()
            })
        } else if args.execution.fail_fast {
            let mut collected = Vec::new();
            for (root, input) in &files {
                let entry = run_one(root, input);
                let failed = entry.1.is_err();
                collected.push(entry);
                if failed {
                    break;
                }
            }
            collected
        } else {
            files
                .iter()
                .map(|(root, input)| run_one(root, input))
                .collect()
        };

    finish_progress(progress.as_ref());
    report(&outcomes, started.elapsed(), args, files.len() > 1)
}

/// Print grouped failures and (for multi-file runs) a final aggregate summary, then
/// return an error if anything failed.
fn report(
    outcomes: &[(std::path::PathBuf, Result<ConvertOutcome>)],
    elapsed: std::time::Duration,
    args: &ConvertArgs,
    multi: bool,
) -> Result<()> {
    let mut ok = 0usize;
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut failures: Vec<(&Path, String)> = Vec::new();
    for (path, result) in outcomes {
        match result {
            Ok(outcome) => {
                ok += 1;
                rows += outcome.rows;
                bytes += outcome.bytes;
            }
            Err(err) => failures.push((path, err.to_string())),
        }
    }

    // Failures, grouped together so they aren't lost in the scrollback. The error
    // messages already name the offending file, so we don't repeat the path here.
    if !failures.is_empty() {
        let style = Style::for_stderr();
        eprintln!("{}", style.red(&format!("Failed ({}):", failures.len())));
        for (_path, err) in &failures {
            eprintln!("  {err}");
        }
    }

    // One closing summary line for batch runs (single-file runs already printed their line).
    if multi && !args.ui.quiet {
        let style = Style::for_stdout();
        let mark = if failures.is_empty() {
            style.check()
        } else {
            style.cross()
        };
        let total = outcomes.len();
        println!(
            "{mark} {} of {} files · {} rows · {} · {}",
            crate::values::thousands(ok as u64),
            crate::values::thousands(total as u64),
            crate::values::thousands(rows),
            crate::values::human_bytes(bytes),
            human_duration(elapsed),
        );
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{}", failures_message(failures.len())))
    }
}

fn convert_one(
    root: &Path,
    input: &Path,
    args: &ConvertArgs,
    catalog: Option<&Catalog>,
    print_each: bool,
    progress: Option<&ProgressState>,
) -> Result<ConvertOutcome> {
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
    // Validate the column selection up front for a clear did-you-mean error.
    resolve_column_indices(&dataset, columns)?;
    let projection = projection_from_selection(&dataset, columns)?;
    let written_cols = projection
        .as_ref()
        .map_or_else(|| dataset.columns().len(), |proj| proj.columns().len());
    let selection = row_selection_from_window(
        RowWindow::new(args.skip, args.max_rows),
        dataset.metadata().row_count,
    );
    let (file_bar, observer) = file_progress(progress, input);
    let scan = ScanOptions {
        selection,
        projection: projection.as_ref(),
        parse_threads: args.execution.parse_threads,
        progress: observer.as_ref(),
    };

    let started = Instant::now();
    let rows = match args.output.effective_sink() {
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
                    embed_metadata: args.output.parquet_metadata,
                    compression: crate::export::resolve_compression(args.output.compression),
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
                    headers: !args.output.no_header,
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
                    headers: !args.output.no_header,
                    scan,
                },
            )
        }
    };
    if let (Some(progress), Some(bar)) = (progress, file_bar) {
        progress.remove_file_bar(&bar);
    }
    let rows = rows?;
    let elapsed = started.elapsed();
    let bytes = fs::metadata(&output).map_or(0, |meta| meta.len());

    if print_each {
        print_success(input, &output, rows, written_cols, bytes, elapsed);
    }
    Ok(ConvertOutcome { rows, bytes })
}

/// Print the styled, one-line success summary for a converted file.
fn print_success(
    input: &Path,
    output: &Path,
    rows: u64,
    cols: usize,
    size: u64,
    elapsed: std::time::Duration,
) {
    let style = Style::for_stdout();
    let detail = format!(
        "{rows} rows · {cols} cols · {} · {}",
        crate::values::human_bytes(size),
        human_duration(elapsed)
    );
    println!(
        "{} {} {} {}   {}",
        style.check(),
        input.display(),
        style.dim("\u{2192}"),
        style.cyan(&output.display().to_string()),
        style.dim(&detail),
    );
}

fn human_duration(elapsed: std::time::Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs < 1.0 {
        format!("{} ms", elapsed.as_millis())
    } else if secs < 60.0 {
        format!("{secs:.1} s")
    } else {
        let total = elapsed.as_secs();
        format!("{}m {:02}s", total / 60, total % 60)
    }
}

/// Grammatically-correct "N file(s) failed to convert".
fn failures_message(failures: usize) -> String {
    let plural = if failures == 1 { "" } else { "s" };
    format!("{failures} file{plural} failed to convert")
}

fn open_dataset(input: &Path, args: &ConvertArgs) -> Result<Dataset> {
    let open = OpenOptions::builder()
        .validation(if args.validation.strict_dates {
            ValidationMode::Strict
        } else {
            ValidationMode::Permissive
        })
        .io_backend(args.io_backend.preference())
        .build();
    friendly::open_with(input, open)
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

/// A per-file bar and the scan observer that drives it, if progress is being shown.
///
/// Bytes rather than rows: on a network share the read rate is the number worth watching, and
/// it is what tells you whether a long conversion is progressing or stalled.
fn file_progress(
    progress: Option<&ProgressState>,
    input: &Path,
) -> (Option<ProgressBar>, Option<ScanProgressObserver>) {
    let bar = progress.and_then(|progress| progress.file_bar(input));
    let observer = bar.as_ref().map(|bar| {
        let bar = bar.clone();
        Arc::new(move |snapshot: ScanProgress| bar.set_position(snapshot.raw_bytes_read))
            as ScanProgressObserver
    });
    (bar, observer)
}

/// Files converted at once above which per-file bars become noise rather than information.
const MAX_FILE_BARS: usize = 8;

struct ProgressState {
    multi: MultiProgress,
    overall: ProgressBar,
    failed: AtomicUsize,
    per_file_bars: usize,
}

impl ProgressState {
    fn new(args: &ConvertArgs, task_count: usize) -> Option<Self> {
        if !should_show_progress(args) {
            return None;
        }
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::stderr_with_hz(20));
        let overall = multi.add(ProgressBar::new(task_count as u64));
        // A stable layout: a steady counts/ETA line, with the message reserved for the
        // running failure count — never the churning current filename.
        overall.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{bar:32.cyan/blue}] {pos}/{len} files · {msg} · {elapsed} · ETA {eta}",
            )
            .ok()?
            .progress_chars("=>-")
            .tick_chars("-\\|/ "),
        );
        overall.set_message("0 failed");
        Some(Self {
            multi,
            overall,
            failed: AtomicUsize::new(0),
            per_file_bars: usize::from(args.execution.jobs.unwrap_or(1) <= MAX_FILE_BARS),
        })
    }

    /// A second bar tracking bytes read within one file, so a single multi-minute conversion
    /// shows movement instead of sitting at the same file count.
    ///
    /// Only while files are converted a few at a time: `--jobs 64` would stack 64 of these.
    fn file_bar(&self, input: &Path) -> Option<ProgressBar> {
        if self.per_file_bars == 0 {
            return None;
        }
        let total = fs::metadata(input).map_or(0, |meta| meta.len());
        let bar = self
            .multi
            .insert_after(&self.overall, ProgressBar::new(total));
        bar.set_style(
            ProgressStyle::with_template(
                "  {spinner:.green} [{bar:32.cyan/blue}] {bytes}/{total_bytes} · {binary_bytes_per_sec} · {msg}",
            )
            .ok()?
            .progress_chars("=>-")
            .tick_chars("-\\|/ "),
        );
        bar.set_message(
            input
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
        );
        Some(bar)
    }

    fn remove_file_bar(&self, bar: &ProgressBar) {
        bar.finish_and_clear();
        self.multi.remove(bar);
    }

    /// Advance the bar by one completed file (whether it succeeded or failed).
    fn tick(&self) {
        self.overall.inc(1);
    }

    /// Record a failure and refresh the running failure count shown on the bar.
    fn record_failure(&self) {
        let failed = self.failed.fetch_add(1, Ordering::Relaxed) + 1;
        self.overall.set_message(format!("{failed} failed"));
    }

    fn finish(&self) {
        self.overall.finish_and_clear();
        let _ = self.multi.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{ProgressState, failures_message, file_progress, human_duration};
    use crate::cli::{
        ConvertArgs, ExecutionOptions, OutputOptions, ProgressMode, RecursionMode, UiOptions,
        ValidationOptions,
    };
    use sas7bdat::ScanProgress;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    fn args(jobs: Option<usize>) -> ConvertArgs {
        ConvertArgs {
            inputs: vec![PathBuf::from("input.sas7bdat")],
            recursive: RecursionMode::Recursive,
            output: OutputOptions {
                out_dir: None,
                out: None,
                sink: None,
                compression: crate::cli::CompressionCodec::Zstd,
                delimiter: None,
                no_header: false,
                flatten: false,
                overwrite: false,
                parquet_row_group_size: None,
                parquet_target_bytes: None,
                parquet_metadata: false,
            },
            execution: ExecutionOptions {
                jobs,
                parse_threads: None,
                fail_fast: false,
            },
            validation: ValidationOptions {
                strict_dates: false,
            },
            io_backend: crate::cli::IoBackend::Auto,
            ui: UiOptions {
                quiet: false,
                progress: ProgressMode::Always,
            },
            skip: None,
            max_rows: None,
            columns: None,
            column_indices: None,
            catalog: None,
        }
    }

    /// A multi-minute conversion of one file has to show movement, which means the scan's byte
    /// counter has to reach the bar.
    #[test]
    fn the_file_bar_follows_bytes_read() {
        let state = ProgressState::new(&args(None), 1).expect("progress enabled");
        let (bar, observer) = file_progress(Some(&state), Path::new("Cargo.toml"));
        let bar = bar.expect("a per-file bar");
        let observer = observer.expect("an observer");

        observer(ScanProgress {
            raw_bytes_read: 4096,
            ..ScanProgress::default()
        });

        assert_eq!(bar.position(), 4096);
        state.remove_file_bar(&bar);
    }

    #[test]
    fn per_file_bars_stop_once_many_files_run_at_once() {
        let state = ProgressState::new(&args(Some(64)), 64).expect("progress enabled");
        let (bar, observer) = file_progress(Some(&state), Path::new("Cargo.toml"));
        assert!(bar.is_none(), "64 concurrent bars would be noise");
        assert!(observer.is_none());
    }

    #[test]
    fn no_progress_state_means_no_observer() {
        let (bar, observer) = file_progress(None, Path::new("Cargo.toml"));
        assert!(bar.is_none());
        assert!(observer.is_none());
    }

    #[test]
    fn human_duration_picks_a_sensible_unit() {
        assert_eq!(human_duration(Duration::from_millis(250)), "250 ms");
        assert_eq!(human_duration(Duration::from_secs_f64(3.25)), "3.2 s");
        assert_eq!(human_duration(Duration::from_secs(184)), "3m 04s");
    }

    #[test]
    fn failures_message_is_grammatical() {
        assert_eq!(failures_message(1), "1 file failed to convert");
        assert_eq!(failures_message(3), "3 files failed to convert");
    }
}
