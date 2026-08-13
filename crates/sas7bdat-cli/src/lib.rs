// Conversion lives in `sas7bdat-convert` so the R package can reach it too; these
// re-exports keep the CLI's own module paths working.
pub use sas7bdat_convert::{catalog, export, parquet_pipeline, paths, sas_metadata, selection};

pub mod bin_support;
pub mod cli;
pub mod completions;
pub mod convert;
pub mod friendly;
pub mod head;
pub mod inspect;
pub mod inspect_report;
pub mod style;
pub mod values;

pub use bin_support::exit_code;
#[cfg(feature = "dev-tools")]
pub use bin_support::{next_parsed, next_value};
pub use cli::{Cli, Command, ConvertArgs, ConvertCli, HeadArgs, InspectArgs, InspectCli};
