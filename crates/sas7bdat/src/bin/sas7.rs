use clap::Parser;
#[path = "sas7/cli.rs"]
mod cli;
#[path = "sas7/convert.rs"]
mod convert;
#[path = "sas7/inspect.rs"]
mod inspect;
#[path = "sas7/paths.rs"]
mod paths;
#[path = "sas7/projection.rs"]
mod projection;
use cli::Cli;
use convert::run_convert;
use inspect::{InspectArgs, run_inspect};

type AnyError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> Result<(), AnyError> {
    let cli = Cli::parse();

    if let Some(path) = cli.inspect {
        if !cli.convert.inputs.is_empty() {
            return Err("`--inspect` cannot be combined with conversion inputs".into());
        }
        let args = InspectArgs {
            input: path,
            json: cli.inspect_json,
        };
        run_inspect(&args)
    } else {
        run_convert(&cli.convert)
    }
}
