use clap::Parser;
use sas7bdat_cli::{InspectArgs, exit_code, inspect};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    exit_code(run())
}

fn run() -> anyhow::Result<()> {
    let args = InspectArgs::parse();
    inspect::run_inspect(&args)
}
