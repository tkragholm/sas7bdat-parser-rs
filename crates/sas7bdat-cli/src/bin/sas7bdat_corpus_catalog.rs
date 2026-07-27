use sas7bdat::{build_catalog, discover_fixture_paths};
use sas7bdat_cli::{exit_code, next_parsed, next_value};
use std::{env, fs, path::PathBuf, process::ExitCode};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    exit_code(run())
}

fn run() -> std::result::Result<(), String> {
    let mut args = env::args_os().skip(1);
    let mut sample_rows = 256usize;
    let mut out: Option<PathBuf> = None;
    let mut inputs = Vec::new();

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--sample-rows" => {
                sample_rows = next_parsed(&mut args, "--sample-rows")?;
            }
            "--out" => {
                out = Some(PathBuf::from(next_value(&mut args, "--out")?));
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
        "usage: cargo run -p sas7bdat-cli --bin sas7bdat-corpus-catalog -- [paths...] [--sample-rows N] [--out PATH]"
    );
}
