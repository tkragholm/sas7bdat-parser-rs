use clap::Parser;
use sas7bdat_cli::{ConvertCli, convert, exit_code};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    exit_code(run())
}

fn run() -> anyhow::Result<()> {
    let cli = ConvertCli::parse();
    convert::run_convert(&cli.args)
}
