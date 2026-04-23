use clap::Parser;
use sas7bdat_cli::{ConvertArgs, convert, exit_code};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    exit_code(run())
}

fn run() -> anyhow::Result<()> {
    let args = ConvertArgs::parse();
    convert::run_convert(&args)
}
