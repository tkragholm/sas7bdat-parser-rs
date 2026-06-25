use clap::Parser;
use sas7bdat_cli::{InspectCli, exit_code, inspect};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    exit_code(run())
}

fn run() -> anyhow::Result<()> {
    let cli = InspectCli::parse();
    inspect::run_inspect(&cli.args)
}
