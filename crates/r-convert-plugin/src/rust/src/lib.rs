//! R binding for `sas7bdat-convert`.
//!
//! Converts `.sas7bdat` files to Parquet, CSV or TSV without the data entering R.
//! That is the whole point of the package: the trees this is aimed at contain files
//! far larger than the session's memory, so R only ever sees one row per input in
//! the returned summary.
//!
//! Two things shape the implementation more than the API does:
//!
//! * **Failures are rows, not errors.** A tree with one unreadable file still
//!   converts the rest, and the caller decides what a failure means. Only argument
//!   mistakes throw.
//! * **A long run is interruptible.** Ctrl-C is polled between files. The polling
//!   is deliberately indirect — see [`Interrupts`] — because R's own interrupt
//!   check unwinds by `longjmp`, which would skip every Rust destructor between
//!   here and the top of the call.

use extendr_api::prelude::*;
use sas7bdat::IoBackendPreference;
use sas7bdat_convert::paths::discover_inputs;
use sas7bdat_convert::{
    CompressionCodec, ConvertOptions, NoObserver, OutputLayout, RecursionMode, SinkKind,
    convert_file,
};
use std::path::PathBuf;

/// Makes Ctrl-C observable without letting R unwind through Rust.
///
/// `R_CheckUserInterrupt` handles an interrupt by `longjmp`-ing straight to R's
/// top level. Called from here that would tear through live `Vec`s, open files and
/// a half-written staging file without running a single destructor. Suspending
/// interrupts turns Ctrl-C into a flag instead: R records it in
/// `R_interrupts_pending` and leaves the stack alone, so the loop can notice it,
/// finish tidily and return what it has.
///
/// The guard restores the previous state on drop, including on panic, so an R
/// session is never left unable to interrupt.
struct Interrupts {
    previous: libR_sys::Rboolean,
}

impl Interrupts {
    fn suspend() -> Self {
        // SAFETY: single-threaded R main thread; these are plain globals in R's
        // C API and this is the documented way to defer interrupt handling.
        unsafe {
            let previous = libR_sys::R_interrupts_suspended;
            libR_sys::R_interrupts_suspended = libR_sys::Rboolean::TRUE;
            Self { previous }
        }
    }

    /// True once the user has asked to stop.
    fn requested(&self) -> bool {
        // SAFETY: as above — a read of an `int` R sets from its signal handler.
        unsafe { libR_sys::R_interrupts_pending != 0 }
    }
}

impl Drop for Interrupts {
    fn drop(&mut self) {
        // SAFETY: as above. Restoring rather than clearing, so nesting is sound.
        unsafe {
            libR_sys::R_interrupts_suspended = self.previous;
        }
    }
}

fn parse_sink(sink: &str) -> std::result::Result<SinkKind, String> {
    match sink {
        "parquet" => Ok(SinkKind::Parquet),
        "csv" => Ok(SinkKind::Csv),
        "tsv" => Ok(SinkKind::Tsv),
        other => Err(format!(
            "unknown sink {other:?}; expected parquet, csv, or tsv"
        )),
    }
}

fn parse_compression(codec: &str) -> std::result::Result<CompressionCodec, String> {
    match codec {
        "zstd" => Ok(CompressionCodec::Zstd),
        "lz4" => Ok(CompressionCodec::Lz4),
        "snappy" => Ok(CompressionCodec::Snappy),
        "none" => Ok(CompressionCodec::None),
        other => Err(format!(
            "unknown compression {other:?}; expected zstd, lz4, snappy, or none"
        )),
    }
}

fn parse_backend(backend: &str) -> std::result::Result<IoBackendPreference, String> {
    backend
        .parse()
        .map_err(|err| format!("io_backend: {err}"))
}

/// One row of the returned summary, before it becomes columns.
struct Row {
    input: String,
    output: String,
    rows: f64,
    columns: i32,
    input_bytes: f64,
    output_bytes: f64,
    seconds: f64,
    status: &'static str,
    error: Option<String>,
}

#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
fn convert_impl(
    input: Vec<String>,
    output: Option<String>,
    recursive: bool,
    flatten: bool,
    overwrite: bool,
    sink: &str,
    compression: &str,
    io_backend: &str,
    threads: Option<usize>,
    tmp_dir: Option<String>,
) -> std::result::Result<Robj, String> {
    let options = ConvertOptions {
        layout: OutputLayout {
            out_dir: output.map(PathBuf::from),
            flatten,
            sink: parse_sink(sink)?,
        },
        overwrite,
        tmp_dir: tmp_dir.map(PathBuf::from),
        io_backend: parse_backend(io_backend)?,
        parse_threads: threads,
        compression: parse_compression(compression)?,
        ..ConvertOptions::default()
    };

    let inputs: Vec<PathBuf> = input.into_iter().map(PathBuf::from).collect();
    let recursion = if recursive {
        RecursionMode::Recursive
    } else {
        RecursionMode::Never
    };
    let files = discover_inputs(&inputs, recursion).map_err(|err| err.to_string())?;

    // Sequential on purpose. Each file's own scan and encode already use the whole
    // machine, so converting several at once mostly oversubscribes it — and running
    // on this thread is what makes the interrupt check below meaningful.
    let interrupts = Interrupts::suspend();
    let mut rows: Vec<Row> = Vec::with_capacity(files.len());
    let mut interrupted = false;

    for (root, file) in &files {
        if interrupts.requested() {
            interrupted = true;
            break;
        }
        let result = convert_file(root, file, None, &options, None, &NoObserver);
        rows.push(match result {
            Ok(outcome) => Row {
                input: outcome.input.display().to_string(),
                output: outcome.output.display().to_string(),
                rows: outcome.rows as f64,
                columns: i32::try_from(outcome.columns).unwrap_or(i32::MAX),
                input_bytes: outcome.input_bytes as f64,
                output_bytes: outcome.output_bytes as f64,
                seconds: outcome.elapsed.as_secs_f64(),
                status: "ok",
                error: None,
            },
            Err(err) => Row {
                input: file.display().to_string(),
                output: String::new(),
                rows: 0.0,
                columns: 0,
                input_bytes: std::fs::metadata(file).map_or(0.0, |m| m.len() as f64),
                output_bytes: 0.0,
                seconds: 0.0,
                status: "error",
                // `{err:#}` renders the whole anyhow chain, so a failure deep in the
                // writer still says what it was rather than just "conversion failed".
                error: Some(format!("{err:#}")),
            },
        });
    }
    drop(interrupts);

    Ok(build_frame(&rows, interrupted, files.len()))
}

/// Assemble the per-file summary as a bare `data.frame`.
fn build_frame(rows: &[Row], interrupted: bool, discovered: usize) -> Robj {
    let n = rows.len();
    let column = |values: Vec<f64>| -> Robj { Doubles::from_values(values).into() };

    let mut inputs = Strings::new(n);
    let mut outputs = Strings::new(n);
    let mut statuses = Strings::new(n);
    let mut errors = Strings::new(n);
    for (i, row) in rows.iter().enumerate() {
        inputs.set_elt(i, Rstr::from(row.input.as_str()));
        outputs.set_elt(i, Rstr::from(row.output.as_str()));
        statuses.set_elt(i, Rstr::from(row.status));
        match &row.error {
            Some(message) => errors.set_elt(i, Rstr::from(message.as_str())),
            // NA rather than "", so `is.na(error)` is the test for success.
            None => errors.set_elt(i, <Rstr>::na()),
        }
    }

    let mut frame: Robj = List::from_values([
        Robj::from(inputs),
        Robj::from(outputs),
        column(rows.iter().map(|r| r.rows).collect()),
        Integers::from_values(rows.iter().map(|r| Rint::from(r.columns))).into(),
        column(rows.iter().map(|r| r.input_bytes).collect()),
        column(rows.iter().map(|r| r.output_bytes).collect()),
        column(rows.iter().map(|r| r.seconds).collect()),
        Robj::from(statuses),
        Robj::from(errors),
    ])
    .into();

    frame
        .set_names([
            "input",
            "output",
            "rows",
            "columns",
            "input_bytes",
            "output_bytes",
            "seconds",
            "status",
            "error",
        ])
        .unwrap();
    let row_names = Integers::from_values([Rint::na(), Rint::from(-(n as i32))]);
    frame.set_attrib("row.names", row_names).unwrap();
    frame.set_class(&["data.frame"]).unwrap();

    // Carried as attributes rather than columns: they describe the run, not a file.
    // `interrupted` is how a caller tells "nothing left to do" from "stopped early".
    frame.set_attrib("interrupted", interrupted).unwrap();
    frame
        .set_attrib("discovered", i32::try_from(discovered).unwrap_or(i32::MAX))
        .unwrap();
    frame
}

/// Convert `.sas7bdat` files to Parquet, CSV or TSV.
#[extendr]
#[allow(clippy::too_many_arguments)]
fn convert_sas_impl(
    input: Vec<String>,
    output: Option<String>,
    recursive: bool,
    flatten: bool,
    overwrite: bool,
    sink: &str,
    compression: &str,
    io_backend: &str,
    threads: Option<i32>,
    tmp_dir: Option<String>,
) -> Robj {
    let threads = match threads {
        None => None,
        Some(n) if n >= 1 => Some(n as usize),
        Some(n) => throw_r_error(format!("threads must be >= 1, got {n}")),
    };
    match convert_impl(
        input,
        output,
        recursive,
        flatten,
        overwrite,
        sink,
        compression,
        io_backend,
        threads,
        tmp_dir,
    ) {
        Ok(frame) => frame,
        Err(message) => throw_r_error(message),
    }
}

extendr_module! {
    mod fastsasconvert;
    fn convert_sas_impl;
}
