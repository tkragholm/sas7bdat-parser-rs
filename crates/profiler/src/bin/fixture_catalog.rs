use sas7bdat_profiler::init_profiler_runtime;
use sas7bdat_simd::{build_catalog, discover_fixture_paths};
use std::{env, fs, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    init_profiler_runtime();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> std::result::Result<(), String> {
    let mut args = env::args_os().skip(1);
    let mut sample_rows = 256usize;
    let mut out: Option<PathBuf> = None;
    let mut inputs = Vec::new();

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--sample-rows" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --sample-rows".to_owned());
                };
                sample_rows = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| "invalid --sample-rows value".to_owned())?;
            }
            "--out" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --out".to_owned());
                };
                out = Some(PathBuf::from(value));
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            value => inputs.push(PathBuf::from(value)),
        }
    }

    if inputs.is_empty() {
        inputs.push(PathBuf::from("fixtures/raw_data"));
        inputs.push(PathBuf::from("fixtures/ahs2013n.sas7bdat"));
    }

    let roots: Vec<String> = inputs
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    let paths = discover_fixture_paths(&inputs).map_err(|err| err.to_string())?;
    let mut catalog = build_catalog(&paths, sample_rows);
    catalog.roots = roots;
    let json = serde_json::to_string_pretty(&catalog).map_err(|err| err.to_string())?;
    if let Some(path) = out {
        fs::write(path, json).map_err(|err| err.to_string())?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p sas7bdat-profiler --bin fixture_catalog -- [paths...] [--sample-rows N] [--out PATH]"
    );
}
