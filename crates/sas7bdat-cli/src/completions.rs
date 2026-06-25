//! `sas7bdat completions <shell>` — emit a shell completion script to stdout.

use crate::cli::{Cli, CompletionsArgs};
use clap::CommandFactory;
use std::io;

/// Write a completion script for the requested shell to stdout.
pub fn run(args: &CompletionsArgs) {
    let mut command = Cli::command();
    let name = command.get_name().to_owned();
    clap_complete::generate(args.shell, &mut command, name, &mut io::stdout());
}
