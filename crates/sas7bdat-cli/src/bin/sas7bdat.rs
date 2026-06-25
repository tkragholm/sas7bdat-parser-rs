use clap::Parser;
use sas7bdat_cli::{Cli, Command, completions, convert, exit_code, head, inspect};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    exit_code(run())
}

fn run() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Convert(args) => convert::run_convert(&args),
        Command::Info(args) => inspect::run_inspect(&args),
        Command::Head(args) => head::run_head(&args),
        Command::Completions(args) => {
            completions::run(&args);
            Ok(())
        }
    }
}
