use crate::catalog::Catalog;
use crate::cli::{ConvertArgs, ProgressMode, SinkKind};
use crate::friendly;
use crate::paths::discover_inputs;
use crate::style::Style;
use anyhow::{Result, anyhow};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rayon::prelude::*;
use sas7bdat::{ScanProgress, ScanProgressObserver};
use sas7bdat_convert::{ConvertObserver, ConvertOptions, ConvertOutcome, NoObserver};
use std::collections::HashMap;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

impl ConvertArgs {
    /// This invocation as the conversion library sees it.
    fn convert_options(&self) -> ConvertOptions {
        ConvertOptions {
            layout: self.output_layout(),
            overwrite: self.output.overwrite,
            tmp_dir: self.output.tmp_dir.clone(),
            io_backend: self.io_backend.preference(),
            parse_threads: self.execution.parse_threads,
            batch_rows: self.execution.batch_rows,
            encode_in_flight_bytes: self.execution.encode_in_flight_bytes,
            compression: self.output.compression.into(),
            parquet_row_group_rows: self.output.parquet_row_group_size,
            parquet_target_bytes: self.output.parquet_target_bytes,
            parquet_metadata: self.output.parquet_metadata,
            delimiter: self.output.delimiter.map(|c| c as u8),
            no_header: self.output.no_header,
            skip: self.skip,
            max_rows: self.max_rows,
            columns: self.columns.clone(),
            column_indices: self.column_indices.clone(),
        }
    }
}

/// Reject argument combinations the library cannot express.
///
/// These are clap-level invariants -- `--out` names one destination, `--delimiter`
/// means nothing to Parquet -- so they stayed with the argument type rather than
/// moving into `sas7bdat-convert` with the path logic.
fn validate_convert_args(args: &ConvertArgs, discovered_inputs: usize) -> Result<()> {
    if discovered_inputs == 0 {
        anyhow::bail!("no .sas7bdat inputs were found");
    }
    if args.output.out.is_some() && discovered_inputs != 1 {
        anyhow::bail!("--out can only be used with a single input");
    }
    if matches!(args.output.effective_sink(), SinkKind::Parquet) && args.output.delimiter.is_some()
    {
        anyhow::bail!("--delimiter only applies to CSV/TSV output");
    }
    Ok(())
}

/// # Errors
///
/// Returns an error if input discovery, catalog loading, conversion, or writing fails.
pub fn run_convert(args: &ConvertArgs) -> Result<()> {
    let files = discover_inputs(&args.inputs, args.recursive.into())?;
    validate_convert_args(args, files.len())?;
    let catalog = if let Some(path) = &args.catalog {
        Some(Catalog::load(path)?)
    } else {
        None
    };
    let options = args.convert_options();
    let progress = ProgressState::new(args, files.len());
    // Per-file success lines only when there's no progress bar to corrupt.
    let print_each = !args.ui.quiet && progress.is_none();
    let started = Instant::now();

    let run_one = |root: &Path, input: &Path| -> (std::path::PathBuf, Result<ConvertOutcome>) {
        let result = convert_one(
            root,
            input,
            args,
            &options,
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
    report(
        &outcomes,
        started.elapsed(),
        args,
        files.len() > 1,
        print_each,
    )
}

/// Print grouped failures and (for multi-file runs) a final aggregate summary, then
/// return an error if anything failed.
fn report(
    outcomes: &[(std::path::PathBuf, Result<ConvertOutcome>)],
    elapsed: std::time::Duration,
    args: &ConvertArgs,
    multi: bool,
    printed_each: bool,
) -> Result<()> {
    let mut ok = 0usize;
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut input_bytes = 0u64;
    let mut failures: Vec<(&Path, String)> = Vec::new();
    for (path, result) in outcomes {
        match result {
            Ok(outcome) => {
                ok += 1;
                rows += outcome.rows;
                bytes += outcome.output_bytes;
                input_bytes += outcome.input_bytes;
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

    // A closing line whenever the run didn't already account for itself per file. Without the
    // `!printed_each` half, a single file converted with a progress bar — the default on a
    // terminal, and exactly how a long benchmark run is invoked — finished with the bar erased
    // and no total printed at all: no elapsed time, no throughput.
    if (multi || !printed_each) && !args.ui.quiet {
        let style = Style::for_stdout();
        let mark = if failures.is_empty() {
            style.check()
        } else {
            style.cross()
        };
        let total = outcomes.len();
        let mut line = format!(
            "{mark} {} of {} files · {} rows · {} → {} · {}",
            crate::values::thousands(ok as u64),
            crate::values::thousands(total as u64),
            crate::values::thousands(rows),
            crate::values::human_bytes(input_bytes),
            crate::values::human_bytes(bytes),
            human_duration(elapsed),
        );
        if let Some(rate) = throughput(input_bytes, elapsed) {
            line.push_str(" · ");
            line.push_str(&rate);
        }
        println!("{line}");
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
    options: &ConvertOptions,
    catalog: Option<&Catalog>,
    print_each: bool,
    progress: Option<&ProgressState>,
) -> Result<ConvertOutcome> {
    // Existence and directory checks stay here: they produce plain-language messages
    // that the library has no business inventing, and they cost a `stat`.
    friendly::guard_convert_input(input)?;

    let observer: &dyn ConvertObserver = progress.map_or(&NoObserver, |state| state);
    let outcome = sas7bdat_convert::convert_file(
        root,
        input,
        args.output.out.as_deref(),
        options,
        catalog,
        observer,
    )
    .map_err(|err| friendly::explain_convert_failure(input, err))?;

    if print_each {
        print_success(
            input,
            &outcome.output,
            outcome.rows,
            outcome.columns,
            outcome.input_bytes,
            outcome.output_bytes,
            outcome.elapsed,
        );
    }
    Ok(outcome)
}

/// Sustained throughput over the *source* bytes, which is the figure to compare runs by.
///
/// Input rather than output: the same file compresses to a different size under a different
/// codec or dictionary policy, so an output-based rate moves when nothing about the read did.
/// `None` when there is nothing meaningful to divide.
/// Below a millisecond the rate is noise divided by noise, and it would print next to a
/// `0 ms` duration — so it is omitted rather than shown as a contradiction.
#[allow(clippy::cast_precision_loss)]
fn throughput(input_bytes: u64, elapsed: std::time::Duration) -> Option<String> {
    let seconds = elapsed.as_secs_f64();
    if input_bytes == 0 || elapsed.as_millis() == 0 {
        return None;
    }
    let mib = input_bytes as f64 / (1024.0 * 1024.0) / seconds;
    Some(if mib >= 100.0 {
        format!("{mib:.0} MiB/s")
    } else {
        format!("{mib:.1} MiB/s")
    })
}

/// Print the styled, one-line success summary for a converted file.
fn print_success(
    input: &Path,
    output: &Path,
    rows: u64,
    cols: usize,
    input_size: u64,
    size: u64,
    elapsed: std::time::Duration,
) {
    let style = Style::for_stdout();
    let mut detail = format!(
        "{rows} rows · {cols} cols · {} → {} · {}",
        crate::values::human_bytes(input_size),
        crate::values::human_bytes(size),
        human_duration(elapsed)
    );
    if let Some(rate) = throughput(input_size, elapsed) {
        detail.push_str(" · ");
        detail.push_str(&rate);
    }
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

impl ConvertObserver for ProgressState {
    fn file_started(&self, input: &Path) -> Option<ScanProgressObserver> {
        let bar = self.file_bar(input)?;
        // The bar is kept alive by the closure; `file_finished` clears it from the
        // multi-progress once the file is done.
        let position = bar.clone();
        self.track_bar(input, bar);
        Some(Arc::new(move |snapshot: ScanProgress| {
            position.set_position(snapshot.raw_bytes_read);
        }))
    }

    fn file_finished(&self, input: &Path, result: &Result<ConvertOutcome>) {
        if let Some(bar) = self.take_bar(input) {
            self.remove_file_bar(&bar);
        }
        if result.is_err() {
            self.record_failure();
        }
    }
}

/// Files converted at once above which per-file bars become noise rather than information.
const MAX_FILE_BARS: usize = 8;

struct ProgressState {
    multi: MultiProgress,
    overall: ProgressBar,
    failed: AtomicUsize,
    per_file_bars: usize,
    /// Bars in flight, keyed by input. The observer trait starts and finishes a file in
    /// two separate calls, so the bar has to outlive the first one.
    bars: Mutex<HashMap<PathBuf, ProgressBar>>,
}

impl ProgressState {
    fn track_bar(&self, input: &Path, bar: ProgressBar) {
        if let Ok(mut bars) = self.bars.lock() {
            bars.insert(input.to_path_buf(), bar);
        }
    }

    fn take_bar(&self, input: &Path) -> Option<ProgressBar> {
        self.bars.lock().ok()?.remove(input)
    }
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
            bars: Mutex::new(HashMap::new()),
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
    use super::{ProgressState, failures_message, human_duration};
    use crate::cli::{
        ConvertArgs, ExecutionOptions, OutputOptions, ProgressMode, RecursionMode, UiOptions,
    };
    use sas7bdat::ScanProgress;
    use sas7bdat_convert::ConvertObserver;
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
                tmp_dir: None,
                parquet_row_group_size: None,
                parquet_target_bytes: None,
                parquet_metadata: false,
            },
            execution: ExecutionOptions {
                jobs,
                parse_threads: None,
                batch_rows: None,
                encode_in_flight_bytes: None,
                fail_fast: false,
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
    /// counter has to reach the bar. The observer now arrives through the trait, so this also
    /// covers `ProgressState` satisfying the contract `convert_tree` calls it through.
    #[test]
    fn the_file_bar_follows_bytes_read() {
        let state = ProgressState::new(&args(None), 1).expect("progress enabled");
        let input = Path::new("Cargo.toml");
        let observer = state.file_started(input).expect("an observer");

        observer(ScanProgress {
            raw_bytes_read: 4096,
            ..ScanProgress::default()
        });

        let bar = state
            .take_bar(input)
            .expect("the bar is tracked until the file finishes");
        assert_eq!(bar.position(), 4096);
        state.remove_file_bar(&bar);
    }

    #[test]
    fn per_file_bars_stop_once_many_files_run_at_once() {
        let state = ProgressState::new(&args(Some(64)), 64).expect("progress enabled");
        assert!(
            state.file_started(Path::new("Cargo.toml")).is_none(),
            "64 concurrent bars would be noise"
        );
    }

    #[test]
    fn finishing_a_file_releases_its_bar() {
        // The bar must not outlive the file: `convert_tree` calls `file_started` and
        // `file_finished` separately, so a leak here would accumulate one bar per input.
        let state = ProgressState::new(&args(None), 1).expect("progress enabled");
        let input = Path::new("Cargo.toml");
        let _ = state.file_started(input).expect("an observer");
        state.file_finished(input, &Ok(outcome()));
        assert!(state.take_bar(input).is_none(), "the bar was released");
    }

    fn outcome() -> sas7bdat_convert::ConvertOutcome {
        sas7bdat_convert::ConvertOutcome {
            input: PathBuf::from("in.sas7bdat"),
            output: PathBuf::from("out.parquet"),
            rows: 1,
            columns: 1,
            input_bytes: 1,
            output_bytes: 1,
            elapsed: Duration::from_millis(1),
        }
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

    #[test]
    fn throughput_reports_input_mib_per_second() {
        use super::throughput;
        use std::time::Duration;

        // 2 MiB in 2 s is 1 MiB/s; sub-100 rates keep a decimal.
        assert_eq!(
            throughput(2 * 1024 * 1024, Duration::from_secs(2)).as_deref(),
            Some("1.0 MiB/s")
        );
        // At or above 100 the decimal is dropped — these are the numbers runs get compared by.
        assert_eq!(
            throughput(1024 * 1024 * 1024, Duration::from_secs(2)).as_deref(),
            Some("512 MiB/s")
        );
        // Nothing to divide, and nothing measurable to divide by.
        assert!(throughput(0, Duration::from_secs(1)).is_none());
        assert!(throughput(1024, Duration::from_micros(500)).is_none());
    }
}
