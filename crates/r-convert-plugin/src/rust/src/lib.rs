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
use sas7bdat_convert::columns::{
    ColumnPlan, ConstantValue, NameCase, StringType, TimeType, TimeUnit,
};
use sas7bdat_convert::paths::discover_inputs;
use sas7bdat_convert::{
    convert_file, CompressionCodec, ConvertOptions, NoObserver, OutputLayout, RecursionMode,
    SinkKind,
};
use std::path::PathBuf;

// R's interrupt flags, from `Rinterface.h`. extendr-ffi binds R's API only, and these
// are outside it, so they are declared here with the types R gives them.
unsafe extern "C" {
    static mut R_interrupts_suspended: extendr_ffi::Rboolean;
    static mut R_interrupts_pending: std::os::raw::c_int;
}

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
    previous: extendr_ffi::Rboolean,
}

impl Interrupts {
    fn suspend() -> Self {
        // SAFETY: single-threaded R main thread; these are plain globals in R's
        // C API and this is the documented way to defer interrupt handling.
        unsafe {
            let previous = R_interrupts_suspended;
            R_interrupts_suspended = extendr_ffi::Rboolean::TRUE;
            Self { previous }
        }
    }

    /// True once the user has asked to stop.
    fn requested(&self) -> bool {
        // SAFETY: as above — a read of an `int` R sets from its signal handler.
        unsafe { R_interrupts_pending != 0 }
    }
}

impl Drop for Interrupts {
    fn drop(&mut self) {
        // SAFETY: as above. Restoring rather than clearing, so nesting is sound.
        unsafe {
            R_interrupts_suspended = self.previous;
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
    backend.parse().map_err(|err| format!("io_backend: {err}"))
}

fn parse_unit(unit: &str, arg: &str) -> std::result::Result<TimeUnit, String> {
    match unit {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        other => Err(format!("unknown {arg} {other:?}; expected s, ms, us or ns")),
    }
}

/// The written columns' names and types, from `sas_columns()`'s fields.
#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
fn column_plan(
    case: &str,
    rename_from: Vec<String>,
    rename_to: Vec<String>,
    add_names: Vec<String>,
    add_values: Vec<String>,
    source_column: Option<String>,
    strings: &str,
    timestamps: Option<String>,
    times: &str,
    time_unit: Option<String>,
) -> std::result::Result<ColumnPlan, String> {
    let case = match case {
        "asis" => NameCase::AsIs,
        "lower" => NameCase::Lower,
        "upper" => NameCase::Upper,
        other => {
            return Err(format!(
                "unknown names {other:?}; expected asis, lower or upper"
            ))
        }
    };
    if rename_from.len() != rename_to.len() || add_names.len() != add_values.len() {
        return Err("rename and add must be named character vectors".to_string());
    }
    let mut constants: Vec<(String, ConstantValue)> = add_names
        .into_iter()
        .zip(add_values)
        .map(|(name, value)| (name, ConstantValue::Text(value)))
        .collect();
    if let Some(name) = source_column {
        constants.push((name, ConstantValue::InputPath));
    }
    let strings = match strings {
        "string" => StringType::Utf8,
        "large_string" => StringType::LargeUtf8,
        other => {
            return Err(format!(
                "unknown strings {other:?}; expected string or large_string"
            ))
        }
    };
    let timestamps = timestamps
        .as_deref()
        .map(|unit| parse_unit(unit, "timestamps"))
        .transpose()?;
    let unit = time_unit
        .as_deref()
        .map(|unit| parse_unit(unit, "time_unit"))
        .transpose()?;
    let times = match times {
        "time_of_day" => TimeType::TimeOfDay(unit.unwrap_or(TimeUnit::Microsecond)),
        "duration" => TimeType::Duration(unit.unwrap_or(TimeUnit::Nanosecond)),
        other => {
            return Err(format!(
                "unknown times {other:?}; expected duration or time_of_day"
            ));
        }
    };
    Ok(ColumnPlan {
        case,
        rename: rename_from.into_iter().zip(rename_to).collect(),
        constants,
        strings,
        timestamps,
        times,
    })
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
    encoding: Option<String>,
    strict: bool,
    labels: bool,
    output_files: Option<Vec<String>>,
    column_plan: ColumnPlan,
) -> std::result::Result<Robj, String> {
    // An unknown encoding would fail every file the same way; say so once instead.
    if let Some(label) = &encoding {
        sas7bdat::OpenOptions::builder()
            .encoding(label)
            .map_err(|err| format!("encoding: {err}"))?;
    }
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
        encoding,
        strict,
        parquet_metadata: labels,
        column_plan,
        ..ConvertOptions::default()
    };

    let inputs: Vec<PathBuf> = input.into_iter().map(PathBuf::from).collect();
    let recursion = if recursive {
        RecursionMode::Recursive
    } else {
        RecursionMode::Never
    };
    // With `output_files`, each input is one file and names its own destination, in
    // the order given. Discovery would sort and could expand, which loses the pairing.
    let (files, outputs): (Vec<(PathBuf, PathBuf)>, Vec<Option<PathBuf>>) = match output_files {
        None => {
            let files = discover_inputs(&inputs, recursion).map_err(|err| err.to_string())?;
            let outputs = vec![None; files.len()];
            (files, outputs)
        }
        Some(outputs) => {
            if outputs.len() != inputs.len() {
                return Err(format!(
                    "output_files has {} paths for {} inputs",
                    outputs.len(),
                    inputs.len()
                ));
            }
            let mut files = Vec::with_capacity(inputs.len());
            for input in &inputs {
                if !input.is_file() {
                    return Err(format!(
                        "with output_files, every input must be a file: {}",
                        input.display()
                    ));
                }
                let found = discover_inputs(std::slice::from_ref(input), RecursionMode::Never)
                    .map_err(|err| err.to_string())?;
                files.extend(found);
            }
            (
                files,
                outputs
                    .into_iter()
                    .map(|path| Some(PathBuf::from(path)))
                    .collect(),
            )
        }
    };

    // Sequential on purpose. Each file's own scan and encode already use the whole
    // machine, so converting several at once mostly oversubscribes it — and running
    // on this thread is what makes the interrupt check below meaningful.
    let interrupts = Interrupts::suspend();
    let mut rows: Vec<Row> = Vec::with_capacity(files.len());
    let mut interrupted = false;

    for ((root, file), explicit) in files.iter().zip(&outputs) {
        if interrupts.requested() {
            interrupted = true;
            break;
        }
        let result = convert_file(root, file, explicit.as_deref(), &options, None, &NoObserver);
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
    encoding: Option<String>,
    strict: bool,
    labels: bool,
    output_files: Option<Vec<String>>,
    names: &str,
    rename_from: Vec<String>,
    rename_to: Vec<String>,
    add_names: Vec<String>,
    add_values: Vec<String>,
    source_column: Option<String>,
    strings: &str,
    timestamps: Option<String>,
    times: &str,
    time_unit: Option<String>,
) -> Robj {
    let plan = match column_plan(
        names,
        rename_from,
        rename_to,
        add_names,
        add_values,
        source_column,
        strings,
        timestamps,
        times,
        time_unit,
    ) {
        Ok(plan) => plan,
        Err(message) => throw_r_error(message),
    };
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
        encoding,
        strict,
        labels,
        output_files,
        plan,
    ) {
        Ok(frame) => frame,
        Err(message) => throw_r_error(message),
    }
}

extendr_module! {
    mod fastsasconvert;
    fn convert_sas_impl;
}
