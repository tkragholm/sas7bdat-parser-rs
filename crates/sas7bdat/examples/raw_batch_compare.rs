use std::{convert::TryFrom, env, path::PathBuf, process::ExitCode, time::Instant};

use sas7bdat::{BatchHint, Dataset};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
fn run() -> Result<(), String> {
    let mut args = env::args_os().skip(1);
    let mut fixture: Option<PathBuf> = None;
    let mut columns: Vec<String> = Vec::new();
    let mut repeat = 1usize;
    let mut batch_rows = 4096usize;
    let mut limit: Option<usize> = None;
    let mut owned = false;

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--fixture" => fixture = Some(PathBuf::from(next_value(&mut args, "--fixture")?)),
            "--columns" => {
                let value = next_value(&mut args, "--columns")?;
                columns = parse_columns(&value);
            }
            "--repeat" => {
                let value = next_value(&mut args, "--repeat")?;
                repeat = value
                    .parse()
                    .map_err(|_| format!("invalid --repeat value: {value}"))?;
            }
            "--batch-rows" => {
                let value = next_value(&mut args, "--batch-rows")?;
                batch_rows = value
                    .parse()
                    .map_err(|_| format!("invalid --batch-rows value: {value}"))?;
            }
            "--limit" => {
                let value = next_value(&mut args, "--limit")?;
                let parsed: usize = value
                    .parse()
                    .map_err(|_| format!("invalid --limit value: {value}"))?;
                limit = (parsed != 0).then_some(parsed);
            }
            "--owned" => owned = true,
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            value => return Err(format!("unexpected argument: {value}")),
        }
    }

    let fixture = fixture.ok_or_else(|| "missing required --fixture".to_owned())?;
    let ds = Dataset::open(&fixture).map_err(|err| err.to_string())?;
    let projection = build_projection(&ds, &columns)?;

    let mut elapsed_total = 0u128;
    let mut rows_last = 0usize;
    let mut batches_last = 0usize;

    let prime_start = Instant::now();
    {
        let mut scan = ds.scan();
        if let Some(projection) = projection.as_ref() {
            scan = scan.with_projection(projection);
        }
        if let Some(limit) = limit {
            scan =
                scan.limit(u64::try_from(limit).map_err(|_| "row limit exceeds u64".to_owned())?);
        }
        scan = scan.with_batch_hint(BatchHint::Rows(batch_rows));

        if owned {
            scan.visit_owned_batches(|batch| {
                batches_last += 1;
                rows_last += batch.row_count;
                std::hint::black_box(batch.row_count);
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .map_err(|err| err.to_string())?;
        } else {
            scan.visit_batches(|batch| {
                batches_last += 1;
                rows_last += batch.row_count;
                std::hint::black_box(batch.row_count);
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .map_err(|err| err.to_string())?;
        }
    }
    let priming_ns = prime_start.elapsed().as_nanos();

    for _ in 0..repeat {
        let mut scan = ds.scan();
        if let Some(projection) = projection.as_ref() {
            scan = scan.with_projection(projection);
        }
        if let Some(limit) = limit {
            scan =
                scan.limit(u64::try_from(limit).map_err(|_| "row limit exceeds u64".to_owned())?);
        }
        scan = scan.with_batch_hint(BatchHint::Rows(batch_rows));

        let start = Instant::now();
        let mut rows = 0usize;
        let mut batches = 0usize;
        if owned {
            scan.visit_owned_batches(|batch| {
                batches += 1;
                rows += batch.row_count;
                std::hint::black_box(batch.row_count);
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .map_err(|err| err.to_string())?;
        } else {
            scan.visit_batches(|batch| {
                batches += 1;
                rows += batch.row_count;
                std::hint::black_box(batch.row_count);
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .map_err(|err| err.to_string())?;
        }
        elapsed_total += start.elapsed().as_nanos();
        rows_last = rows;
        batches_last = batches;
    }

    let elapsed_avg = elapsed_total / repeat as u128;
    let seconds = elapsed_avg as f64 / 1_000_000_000.0;
    let rows_per_second = if seconds > 0.0 {
        rows_last as f64 / seconds
    } else {
        0.0
    };
    let batches_per_second = if seconds > 0.0 {
        batches_last as f64 / seconds
    } else {
        0.0
    };

    println!(
        "{}",
        serde_json::json!({
            "fixture": fixture.display().to_string(),
            "columns": columns,
            "repeat": repeat,
            "batch_rows": batch_rows,
            "limit": limit,
            "owned": owned,
            "priming_ns": priming_ns,
            "elapsed_ns_total": elapsed_total,
            "elapsed_ns_avg": elapsed_avg,
            "rows_last": rows_last,
            "batches_last": batches_last,
            "rows_per_second": rows_per_second,
            "batches_per_second": batches_per_second,
        })
    );
    Ok(())
}

fn build_projection(
    ds: &Dataset,
    columns: &[String],
) -> Result<Option<sas7bdat::Projection>, String> {
    if columns.is_empty() {
        return Ok(None);
    }
    ds.projection()
        .columns(columns.to_vec())
        .build()
        .map(Some)
        .map_err(|err| err.to_string())
}

fn parse_columns(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn next_value(
    args: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> Result<String, String> {
    let Some(value) = args.next() else {
        return Err(format!("missing value after {flag}"));
    };
    Ok(value.to_string_lossy().into_owned())
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --example raw_batch_compare -- --fixture PATH [--columns COL1,COL2] [--repeat N] [--batch-rows N] [--limit N] [--owned]"
    );
}
