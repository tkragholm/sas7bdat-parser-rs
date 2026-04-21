pub mod catalog;
pub mod cli;
pub mod convert;
pub mod export;
pub mod inspect;
pub mod inspect_report;
pub mod paths;
pub mod runtime;
pub mod selection;

use anyhow::Result;
use clap::Parser;

pub use runtime::init_profiler_runtime;

/// # Errors
///
/// Returns an error if the selected command fails.
pub fn run() -> Result<()> {
    let cli = cli::Cli::parse();
    match cli.command {
        cli::Commands::Convert(args) => convert::run_convert(&args),
        cli::Commands::Inspect(args) => inspect::run_inspect(&args),
    }
}
