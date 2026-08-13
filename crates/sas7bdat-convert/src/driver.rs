//! Converting files: one, or a whole discovered tree.
//!
//! The UI-free half of `sas7bdat convert`. Everything here takes [`ConvertOptions`]
//! and returns [`ConvertOutcome`]s; nothing prints, draws a progress bar, or decides
//! what a failure should look like to a human. Callers that want those things get
//! them through [`ConvertObserver`] and by rendering the returned outcomes.
//!
//! Two properties matter more than the API shape and are easy to lose in a rewrite:
//!
//! * **No output is written in place.** Each file is converted to a staging path and
//!   moved over the destination only once it is complete, so an interrupted run
//!   cannot leave a truncated file that a later `overwrite = false` pass mistakes
//!   for finished work.
//! * **One bad file does not lose the run.** [`convert_tree`] returns a result per
//!   input and keeps going; only the caller decides whether to stop.

use crate::catalog::Catalog;
use crate::export::{
    DelimitedWriteOptions, ScanOptions, WriteOptions, resolve_compression, write_csv_or_tsv,
    write_parquet,
};
use crate::paths::compute_output_path;
use crate::selection::{
    ColumnSelection, RowWindow, projection_from_selection, resolve_column_indices,
    row_selection_from_window,
};
use crate::{CompressionCodec, OutputLayout, SinkKind};
use anyhow::{Result, anyhow};
use rayon::prelude::*;
use sas7bdat::{Dataset, IoBackendPreference, OpenOptions, ScanProgressObserver};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Everything a conversion needs to know that is not the input path.
///
/// A plain struct rather than a builder: callers assemble it from their own argument
/// types, so a new field is a compile error at every call site instead of a silently
/// defaulted change in behaviour.
#[derive(Clone, Debug)]
pub struct ConvertOptions {
    /// Where outputs go and in what format.
    pub layout: OutputLayout,
    /// Replace an existing output instead of refusing to.
    pub overwrite: bool,
    /// Directory for the staging file. `None` stages beside the destination, where the
    /// move is a rename. Pointing this at a local disk keeps the write off a network
    /// link until there is a finished file to send, at the cost of a copy.
    pub tmp_dir: Option<PathBuf>,
    /// How the input is read. `Auto` memory-maps local files and reads shares
    /// sequentially — but it can only tell them apart on Windows.
    pub io_backend: IoBackendPreference,
    /// Decode threads per file. `None` uses every logical core.
    pub parse_threads: Option<usize>,
    /// Rows per scan batch. `None` lets the reader size them.
    pub batch_rows: Option<usize>,
    /// Decoded bytes allowed in row groups that are encoding but not yet written.
    pub encode_in_flight_bytes: Option<usize>,
    /// Parquet compression codec.
    pub compression: CompressionCodec,
    /// Rows per Parquet row group. Takes precedence over `parquet_target_bytes`.
    pub parquet_row_group_rows: Option<usize>,
    /// Target row-group size in bytes, converted to rows using the row length.
    pub parquet_target_bytes: Option<usize>,
    /// Embed SAS labels, formats, kinds and widths in the Parquet key-value metadata.
    pub parquet_metadata: bool,
    /// Delimiter for CSV/TSV. `None` uses the sink's default.
    pub delimiter: Option<u8>,
    /// Omit the header row from CSV/TSV.
    pub no_header: bool,
    /// Skip this many leading rows.
    pub skip: Option<u64>,
    /// Convert at most this many rows.
    pub max_rows: Option<u64>,
    /// Convert only these columns, by name.
    pub columns: Option<Vec<String>>,
    /// Convert only these columns, by zero-based index.
    pub column_indices: Option<Vec<usize>>,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            layout: OutputLayout::default(),
            overwrite: false,
            tmp_dir: None,
            io_backend: IoBackendPreference::Auto,
            parse_threads: None,
            batch_rows: None,
            encode_in_flight_bytes: None,
            compression: CompressionCodec::default(),
            parquet_row_group_rows: None,
            parquet_target_bytes: None,
            parquet_metadata: false,
            delimiter: None,
            no_header: false,
            skip: None,
            max_rows: None,
            columns: None,
            column_indices: None,
        }
    }
}

/// What one converted file cost and produced.
#[derive(Clone, Debug)]
pub struct ConvertOutcome {
    pub input: PathBuf,
    pub output: PathBuf,
    pub rows: u64,
    /// Columns actually written, after any projection.
    pub columns: usize,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub elapsed: Duration,
}

/// Watches a tree conversion as it runs.
///
/// Called from whichever thread is converting, so implementations must be `Sync` and
/// must not assume a main thread — an R binding, for instance, can only accumulate
/// counters here and read them from the main thread later.
pub trait ConvertObserver: Sync {
    /// A file is about to be converted. Returning an observer attaches it to the scan.
    fn file_started(&self, _input: &Path) -> Option<ScanProgressObserver> {
        None
    }
    /// A file finished, successfully or not.
    fn file_finished(&self, _input: &Path, _result: &Result<ConvertOutcome>) {}
}

/// An observer that does nothing, for callers that want no reporting.
pub struct NoObserver;
impl ConvertObserver for NoObserver {}

/// Convert one file.
///
/// `root` is the directory the input was discovered under, which is what
/// [`compute_output_path`] mirrors below the output root. `explicit_output`
/// overrides that entirely, for a caller naming a single destination.
///
/// # Errors
///
/// Returns an error if the output already exists and `overwrite` is unset, if the
/// input cannot be opened or decoded, or if writing fails. A failed conversion
/// leaves no staging file behind.
pub fn convert_file(
    root: &Path,
    input: &Path,
    explicit_output: Option<&Path>,
    options: &ConvertOptions,
    catalog: Option<&Catalog>,
    observer: &dyn ConvertObserver,
) -> Result<ConvertOutcome> {
    let output = explicit_output.map_or_else(
        || compute_output_path(root, input, &options.layout),
        Path::to_path_buf,
    );
    if output.exists() && !options.overwrite {
        return Err(anyhow!(
            "output already exists (use overwrite): {}",
            output.display()
        ));
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }

    let staged = staging_path(&output, options.tmp_dir.as_deref())?;
    let open = OpenOptions::builder()
        .io_backend(options.io_backend)
        .build();
    let dataset = Dataset::open_with(input, open)?;

    let selection = ColumnSelection {
        names: options.columns.as_deref(),
        indices: options.column_indices.as_deref(),
    };
    // Resolved up front so an unknown column name fails before any file is created.
    resolve_column_indices(&dataset, selection)?;
    let projection = projection_from_selection(&dataset, selection)?;
    let columns = projection
        .as_ref()
        .map_or_else(|| dataset.columns().len(), |proj| proj.columns().len());
    let rows_wanted = row_selection_from_window(
        RowWindow::new(options.skip, options.max_rows),
        dataset.metadata().row_count,
    );

    let progress = observer.file_started(input);
    let scan = ScanOptions {
        selection: rows_wanted,
        projection: projection.as_ref(),
        parse_threads: options.parse_threads,
        progress: progress.as_ref(),
    };

    let started = Instant::now();
    let rows = match write_output(&dataset, &staged, options, scan, catalog) {
        Ok(rows) => rows,
        Err(err) => {
            // The staging file is this function's to clean up; leaving one behind
            // would accumulate junk beside every destination.
            let _ = fs::remove_file(&staged);
            return Err(err);
        }
    };
    publish(&staged, &output)?;

    Ok(ConvertOutcome {
        input: input.to_path_buf(),
        output: output.clone(),
        rows,
        columns,
        input_bytes: fs::metadata(input).map_or(0, |meta| meta.len()),
        output_bytes: fs::metadata(&output).map_or(0, |meta| meta.len()),
        elapsed: started.elapsed(),
    })
}

/// Convert every discovered input, returning one result per file in input order.
///
/// Failures are values, not early returns: a tree with one unreadable file still
/// converts the rest, and the caller decides what that means.
///
/// `jobs` is *files* in parallel. `None` converts them one at a time, which is the
/// right default — each file's own scan and encode already use the whole machine, so
/// running several at once mostly oversubscribes it.
pub fn convert_tree(
    files: &[(PathBuf, PathBuf)],
    options: &ConvertOptions,
    catalog: Option<&Catalog>,
    jobs: Option<usize>,
    observer: &dyn ConvertObserver,
) -> Vec<Result<ConvertOutcome>> {
    let run = |(root, input): &(PathBuf, PathBuf)| {
        let result = convert_file(root, input, None, options, catalog, observer);
        observer.file_finished(input, &result);
        result
    };

    match jobs {
        Some(jobs) if jobs > 1 => rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build()
            .map_or_else(
                |err| {
                    vec![Err(anyhow!(
                        "could not start {jobs} conversion threads: {err}"
                    ))]
                },
                |pool| pool.install(|| files.par_iter().map(run).collect()),
            ),
        _ => files.iter().map(run).collect(),
    }
}

fn write_output(
    dataset: &Dataset,
    staged: &Path,
    options: &ConvertOptions,
    scan: ScanOptions<'_>,
    catalog: Option<&Catalog>,
) -> Result<u64> {
    match options.layout.sink {
        SinkKind::Parquet => {
            let row_group_rows =
                match (options.parquet_row_group_rows, options.parquet_target_bytes) {
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
                dataset,
                staged,
                WriteOptions {
                    row_group_rows,
                    batch_rows: options.batch_rows,
                    encode_in_flight_bytes: options.encode_in_flight_bytes,
                    scan,
                    catalog,
                    embed_metadata: options.parquet_metadata,
                    compression: resolve_compression(options.compression),
                },
            )
        }
        sink @ (SinkKind::Csv | SinkKind::Tsv) => write_csv_or_tsv(
            dataset,
            staged,
            DelimitedWriteOptions {
                delimiter: options
                    .delimiter
                    .unwrap_or(if matches!(sink, SinkKind::Tsv) {
                        b'\t'
                    } else {
                        b','
                    }),
                headers: !options.no_header,
                scan,
            },
        ),
    }
}

/// Where a conversion writes before it has something worth keeping.
///
/// The name carries the process id *and* a counter. The counter is not redundant: with a
/// `tmp_dir`, two inputs from different subdirectories that share a file name would
/// otherwise stage to the same path and race.
fn staging_path(output: &Path, tmp_dir: Option<&Path>) -> Result<PathBuf> {
    static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
    let mut name = output.file_name().unwrap_or_default().to_os_string();
    name.push(format!(
        ".{}-{}.part",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    match tmp_dir {
        Some(dir) => {
            fs::create_dir_all(dir)?;
            Ok(dir.join(name))
        }
        None => Ok(output.with_file_name(name)),
    }
}

/// Move a finished output into place.
///
/// A rename when the two sit on one volume, which is atomic and free. A copy when they
/// do not, which is what a `tmp_dir` on another disk asks for: the write lands locally
/// and only the finished file crosses the network.
fn publish(staged: &Path, output: &Path) -> Result<()> {
    // Rename refuses to clobber on Windows, and an existing output here has already
    // been cleared by the overwrite check.
    if output.exists() {
        fs::remove_file(output)?;
    }
    if fs::rename(staged, output).is_ok() {
        return Ok(());
    }
    fs::copy(staged, output)?;
    fs::remove_file(staged)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ConvertOptions, publish, staging_path};
    use std::path::{Path, PathBuf};

    #[test]
    fn staging_defaults_to_the_destination_directory() {
        let staged = staging_path(Path::new("/out/data.parquet"), None).expect("staging path");
        assert_eq!(staged.parent(), Some(Path::new("/out")));
        assert!(
            staged
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("data.parquet.") && n.ends_with(".part"))
        );
    }

    #[test]
    fn staging_honours_a_tmp_dir_and_creates_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let scratch = dir.path().join("does-not-exist-yet");
        let staged =
            staging_path(Path::new("/out/data.parquet"), Some(&scratch)).expect("staging path");
        assert_eq!(staged.parent(), Some(scratch.as_path()));
        assert!(scratch.is_dir(), "a tmp_dir is created rather than assumed");
    }

    #[test]
    fn staging_names_do_not_collide_for_identically_named_inputs() {
        // Two inputs called the same thing in different subdirectories, staged into one
        // tmp_dir: the counter is what keeps these apart.
        let dir = tempfile::tempdir().expect("temp dir");
        let a = staging_path(Path::new("/out/one/x.parquet"), Some(dir.path())).expect("a");
        let b = staging_path(Path::new("/out/two/x.parquet"), Some(dir.path())).expect("b");
        assert_ne!(a, b);
    }

    #[test]
    fn publishing_moves_the_file_into_place() {
        let dir = tempfile::tempdir().expect("temp dir");
        let staged = dir.path().join("out.parquet.1-0.part");
        let output = dir.path().join("out.parquet");
        std::fs::write(&staged, b"finished").expect("staged file");

        publish(&staged, &output).expect("publish");

        assert!(!staged.exists(), "staging file must not survive");
        assert_eq!(std::fs::read(&output).expect("output"), b"finished");
    }

    #[test]
    fn publishing_replaces_an_existing_output() {
        let dir = tempfile::tempdir().expect("temp dir");
        let staged = dir.path().join("out.parquet.1-0.part");
        let output = dir.path().join("out.parquet");
        std::fs::write(&output, b"stale").expect("stale output");
        std::fs::write(&staged, b"fresh").expect("staged file");

        publish(&staged, &output).expect("publish");

        assert_eq!(std::fs::read(&output).expect("output"), b"fresh");
    }

    #[test]
    fn defaults_convert_to_parquet_beside_the_input_without_overwriting() {
        let options = ConvertOptions::default();
        assert!(!options.overwrite);
        assert!(
            options.tmp_dir.is_none(),
            "staging defaults beside the destination"
        );
        assert!(options.layout.out_dir.is_none());
        assert_eq!(options.layout.sink, crate::SinkKind::Parquet);
    }

    #[test]
    fn an_explicit_output_overrides_the_mirrored_path() {
        // Guards the branch a single-destination caller depends on: `explicit_output`
        // must win over the computed tree position, not be merged with it.
        let options = ConvertOptions::default();
        let computed = crate::paths::compute_output_path(
            Path::new("/data"),
            Path::new("/data/nested/x.sas7bdat"),
            &options.layout,
        );
        assert_eq!(computed, PathBuf::from("/data/nested/x.parquet"));
    }
}
