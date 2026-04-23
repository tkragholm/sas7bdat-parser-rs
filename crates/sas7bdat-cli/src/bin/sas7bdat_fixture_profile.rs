#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use sas7bdat_cli::{exit_code_with_init, init_profiler_runtime, next_parsed, next_value};
use sas7bdat_simd::{
    BatchHint, Dataset, DecodeMode, IoBackendPreference, OpenOptions, ProjectionPreset,
    ScanStatsSummary, build_projection, summarize_scan_stats,
};
use serde::Serialize;
use std::{env, path::PathBuf, process::ExitCode, time::Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProfileMode {
    RawRows,
    TypedRows,
    TypedLosslessRows,
    TypedBatches,
    TypedLosslessBatches,
}

impl ProfileMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "raw_rows" => Some(Self::RawRows),
            "typed_rows" => Some(Self::TypedRows),
            "typed_lossless_rows" => Some(Self::TypedLosslessRows),
            "typed_batches" => Some(Self::TypedBatches),
            "typed_lossless_batches" => Some(Self::TypedLosslessBatches),
            _ => None,
        }
    }

    const fn decode_mode(self) -> DecodeMode {
        match self {
            Self::RawRows | Self::TypedRows | Self::TypedBatches => DecodeMode::Typed,
            Self::TypedLosslessRows | Self::TypedLosslessBatches => DecodeMode::TypedLossless,
        }
    }

    const fn is_batch(self) -> bool {
        matches!(self, Self::TypedBatches | Self::TypedLosslessBatches)
    }
}

#[derive(Debug, Serialize)]
struct ProfileOutput {
    fixture: String,
    mode: String,
    projection: String,
    io_backend: String,
    limit: Option<u64>,
    repeat: usize,
    elapsed_ns_total: u128,
    elapsed_ns_avg: u128,
    rows_per_second: f64,
    bytes_per_second: f64,
    stats_last: ScanStatsSummary,
}

const fn mode_name(mode: ProfileMode) -> &'static str {
    match mode {
        ProfileMode::RawRows => "raw_rows",
        ProfileMode::TypedRows => "typed_rows",
        ProfileMode::TypedLosslessRows => "typed_lossless_rows",
        ProfileMode::TypedBatches => "typed_batches",
        ProfileMode::TypedLosslessBatches => "typed_lossless_batches",
    }
}

fn main() -> ExitCode {
    exit_code_with_init(init_profiler_runtime, run)
}

fn run() -> std::result::Result<(), String> {
    let mut args = env::args_os().skip(1);
    let mut fixture: Option<PathBuf> = None;
    let mut mode = ProfileMode::TypedRows;
    let mut projection = ProjectionPreset::Full;
    let mut repeat = 1usize;
    let mut limit: Option<u64> = None;
    let mut batch_rows = 256usize;
    let mut io_backend = IoBackendPreference::Auto;

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--fixture" => fixture = Some(PathBuf::from(next_value(&mut args, "--fixture")?)),
            "--mode" => {
                let value = next_value(&mut args, "--mode")?;
                mode = ProfileMode::parse(&value)
                    .ok_or_else(|| format!("invalid --mode value: {value}"))?;
            }
            "--projection" => {
                let value = next_value(&mut args, "--projection")?;
                projection = ProjectionPreset::parse(&value)
                    .ok_or_else(|| format!("invalid --projection value: {value}"))?;
            }
            "--repeat" => {
                repeat = next_parsed(&mut args, "--repeat")?;
            }
            "--limit" => {
                let parsed: u64 = next_parsed(&mut args, "--limit")?;
                limit = (parsed != 0).then_some(parsed);
            }
            "--batch-rows" => {
                batch_rows = next_parsed(&mut args, "--batch-rows")?;
            }
            "--io-backend" => {
                let value = next_value(&mut args, "--io-backend")?;
                io_backend = parse_io_backend(&value)
                    .ok_or_else(|| format!("invalid --io-backend value: {value}"))?;
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            value => return Err(format!("unexpected argument: {value}")),
        }
    }

    let fixture = fixture.ok_or_else(|| "missing required --fixture".to_owned())?;
    let ds = Dataset::open_with(
        &fixture,
        OpenOptions::builder().io_backend(io_backend).build(),
    )
    .map_err(|err| err.to_string())?;
    let projection_obj = build_projection(&ds, projection);

    let mut elapsed_total = 0u128;
    let mut stats_last = ScanStatsSummary::default();
    for _ in 0..repeat {
        let mut scan = ds.scan().with_decode_mode(mode.decode_mode());
        if let Some(projection) = projection_obj.as_ref() {
            scan = scan.with_projection(projection);
        }
        if let Some(limit) = limit {
            scan = scan.limit(limit);
        }
        if mode.is_batch() {
            scan = scan.with_batch_hint(BatchHint::Rows(batch_rows));
        }

        let start = Instant::now();
        let stats = match mode {
            ProfileMode::RawRows => scan
                .with_decode_mode(DecodeMode::Raw)
                .visit_raw_rows(|_| Ok(std::ops::ControlFlow::Continue(()))),
            ProfileMode::TypedRows | ProfileMode::TypedLosslessRows => {
                scan.visit_rows(|_| Ok(std::ops::ControlFlow::Continue(())))
            }
            ProfileMode::TypedBatches | ProfileMode::TypedLosslessBatches => {
                scan.visit_batches(|_| Ok(std::ops::ControlFlow::Continue(())))
            }
        }
        .map_err(|err| err.to_string())?;
        elapsed_total += start.elapsed().as_nanos();
        stats_last = summarize_scan_stats(&stats);
    }

    let elapsed_avg = elapsed_total / repeat as u128;
    let seconds = elapsed_avg as f64 / 1_000_000_000.0;
    let rows_per_second = if seconds > 0.0 {
        stats_last.rows_emitted as f64 / seconds
    } else {
        0.0
    };
    let bytes_per_second = if seconds > 0.0 {
        stats_last.raw_bytes_read as f64 / seconds
    } else {
        0.0
    };

    let output = ProfileOutput {
        fixture: fixture.display().to_string(),
        mode: mode_name(mode).to_owned(),
        projection: match projection {
            ProjectionPreset::Full => "full",
            ProjectionPreset::Numeric => "numeric",
            ProjectionPreset::Strings => "strings",
            ProjectionPreset::Mixed => "mixed",
        }
        .to_owned(),
        io_backend: match io_backend {
            IoBackendPreference::Auto => "auto",
            IoBackendPreference::MmapPreferred => "mmap-preferred",
            IoBackendPreference::BufferedPreferred => "buffered-preferred",
            IoBackendPreference::BufferedOnly => "buffered-only",
        }
        .to_owned(),
        limit,
        repeat,
        elapsed_ns_total: elapsed_total,
        elapsed_ns_avg: elapsed_avg,
        rows_per_second,
        bytes_per_second,
        stats_last,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&output).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn parse_io_backend(value: &str) -> Option<IoBackendPreference> {
    match value {
        "auto" => Some(IoBackendPreference::Auto),
        "mmap-preferred" => Some(IoBackendPreference::MmapPreferred),
        "buffered-preferred" => Some(IoBackendPreference::BufferedPreferred),
        "buffered-only" => Some(IoBackendPreference::BufferedOnly),
        _ => None,
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p sas7bdat-cli --bin sas7bdat-fixture-profile -- --fixture PATH --mode raw_rows|typed_rows|typed_lossless_rows|typed_batches|typed_lossless_batches [--projection full|numeric|strings|mixed] [--repeat N] [--limit N] [--batch-rows N] [--io-backend auto|mmap-preferred|buffered-preferred|buffered-only]"
    );
}
