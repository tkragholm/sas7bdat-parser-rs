use super::{
    FixtureCatalog, IoBackendPreference, OutputFormat, ProfileMode, ProjectionPreset,
    ScanRunOptions, collect_fixture_entries, discover_fixture_paths, display_roots,
    load_failed_paths, print_usage, summarize_catalog,
};
use sas7bdat_cli::{next_parsed, next_value};
use std::{env, path::PathBuf};

#[derive(Debug)]
struct CliConfig {
    help: bool,
    sample_rows: usize,
    out: Option<PathBuf>,
    failed_from: Option<PathBuf>,
    summary_only: bool,
    format: OutputFormat,
    format_explicit: bool,
    scan_mode: Option<ProfileMode>,
    scan_projection: ProjectionPreset,
    scan_batch_rows: usize,
    scan_io_backend: IoBackendPreference,
    scan_limit: Option<u64>,
    inputs: Vec<PathBuf>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            sample_rows: 256,
            help: false,
            out: None,
            failed_from: None,
            summary_only: false,
            format: OutputFormat::Json,
            format_explicit: false,
            scan_mode: None,
            scan_projection: ProjectionPreset::Full,
            scan_batch_rows: 256,
            scan_io_backend: IoBackendPreference::Auto,
            scan_limit: None,
            inputs: Vec::new(),
        }
    }
}

pub fn run() -> std::result::Result<(), String> {
    let config = parse_args()?;
    execute(&config)
}

fn parse_args() -> std::result::Result<CliConfig, String> {
    let mut args = env::args_os().skip(1);
    let mut config = CliConfig::default();

    while let Some(arg) = args.next() {
        let flag = arg.to_string_lossy().into_owned();
        if handle_flag(&flag, &mut args, &mut config)? {
            continue;
        }
        config.inputs.push(PathBuf::from(arg));
    }

    Ok(config)
}

fn handle_flag(
    flag: &str,
    args: &mut impl Iterator<Item = std::ffi::OsString>,
    config: &mut CliConfig,
) -> std::result::Result<bool, String> {
    if handle_common_flag(flag, args, config)? {
        return Ok(true);
    }
    if handle_scan_flag(flag, args, config)? {
        return Ok(true);
    }
    match flag {
        "--help" | "-h" => {
            config.help = true;
            Ok(true)
        }
        value if value.starts_with("--") => Err(format!("unexpected argument: {value}")),
        _ => Ok(false),
    }
}

fn handle_common_flag(
    flag: &str,
    args: &mut impl Iterator<Item = std::ffi::OsString>,
    config: &mut CliConfig,
) -> std::result::Result<bool, String> {
    match flag {
        "--sample-rows" => {
            config.sample_rows = next_parsed(args, flag)?;
            Ok(true)
        }
        "--out" => {
            config.out = Some(PathBuf::from(next_value(args, flag)?));
            Ok(true)
        }
        "--failed-from" => {
            config.failed_from = Some(PathBuf::from(next_value(args, flag)?));
            Ok(true)
        }
        "--format" => {
            config.format = OutputFormat::parse(&next_value(args, flag)?)?;
            config.format_explicit = true;
            Ok(true)
        }
        "--summary-only" => {
            config.summary_only = true;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn handle_scan_flag(
    flag: &str,
    args: &mut impl Iterator<Item = std::ffi::OsString>,
    config: &mut CliConfig,
) -> std::result::Result<bool, String> {
    match flag {
        "--scan-mode" | "--mode" => {
            let value = next_value(args, flag)?;
            config.scan_mode =
                Some(ProfileMode::parse(&value).ok_or_else(|| format!("invalid {flag} value"))?);
            Ok(true)
        }
        "--scan-projection" | "--projection" => {
            let value = next_value(args, flag)?;
            config.scan_projection =
                ProjectionPreset::parse(&value).ok_or_else(|| format!("invalid {flag} value"))?;
            Ok(true)
        }
        "--scan-batch-rows" | "--batch-rows" => {
            config.scan_batch_rows = next_parsed(args, flag)?;
            Ok(true)
        }
        "--scan-io-backend" | "--io-backend" => {
            let value = next_value(args, flag)?;
            config.scan_io_backend = value.parse().map_err(|_| format!("invalid {flag} value"))?;
            Ok(true)
        }
        "--scan-limit" | "--limit" => {
            let parsed = next_parsed(args, flag)?;
            config.scan_limit = (parsed != 0).then_some(parsed);
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn execute(config: &CliConfig) -> std::result::Result<(), String> {
    if config.help {
        print_usage();
        return Ok(());
    }

    if config.inputs.is_empty() && config.failed_from.is_none() {
        return Err("usage requires at least one input path".to_owned());
    }

    let roots = display_roots(&config.inputs, config.failed_from.as_deref());
    let paths = if let Some(failed_from) = config.failed_from.as_deref() {
        load_failed_paths(failed_from, &config.inputs)?
    } else {
        discover_fixture_paths(&config.inputs).map_err(|err| err.to_string())?
    };
    if paths.is_empty() {
        return Err("no files selected for profiling".to_owned());
    }

    if let Some(mode) = config.scan_mode {
        return execute_scan(config, mode, &paths, &roots);
    }

    execute_structural(config, &paths, &roots)
}

fn execute_scan(
    config: &CliConfig,
    mode: ProfileMode,
    paths: &[PathBuf],
    roots: &[String],
) -> std::result::Result<(), String> {
    if config.format_explicit && config.format != OutputFormat::Csv {
        return Err("scan profiling currently supports only --format csv".to_owned());
    }
    if config.summary_only {
        return Err("--summary-only is not supported with --scan-mode".to_owned());
    }

    super::write_scan_profile(
        paths,
        roots,
        config.out.clone(),
        ScanRunOptions {
            mode,
            projection: config.scan_projection,
            batch_rows: config.scan_batch_rows,
            io_backend: config.scan_io_backend,
            limit: config.scan_limit,
        },
    )
}

fn execute_structural(
    config: &CliConfig,
    paths: &[PathBuf],
    roots: &[String],
) -> std::result::Result<(), String> {
    if config.format_explicit && config.format != OutputFormat::Csv {
        return Err("corpus profiling currently supports only --format csv".to_owned());
    }

    let catalog = FixtureCatalog {
        roots: roots.to_owned(),
        sample_rows: config.sample_rows,
        fixtures: collect_fixture_entries(paths, config.sample_rows),
    };
    let summary = summarize_catalog(&catalog);
    match config.format {
        OutputFormat::Json => super::write_json(
            &catalog,
            roots,
            config.sample_rows,
            &summary,
            config.summary_only,
            config.out.clone(),
        ),
        OutputFormat::Csv => super::write_csv(
            &catalog,
            roots,
            config.sample_rows,
            &summary,
            config.summary_only,
            config.out.clone(),
        ),
    }
}
