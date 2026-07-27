#[cfg(feature = "dev-tools")]
use std::{ffi::OsString, str::FromStr};
use std::{fmt::Display, process::ExitCode};

/// Convert a `Result` into a process exit code with consistent stderr output.
///
/// Printed with `{:#}` so an `anyhow` error shows its whole context chain on one line —
/// `failed to read catalog x.sas7bcat: not a SAS catalog: ...` rather than just the
/// outermost context, which on its own rarely says what actually went wrong.
pub fn exit_code<E>(result: Result<(), E>) -> ExitCode
where
    E: Display,
{
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message:#}");
            ExitCode::FAILURE
        }
    }
}

/// Read the next flag value from an iterator.
///
/// Used by the hand-rolled argument parsing in the `dev-tools` binaries; the
/// user-facing commands go through clap.
///
/// # Errors
///
/// Returns an error if the flag has no following value.
#[cfg(feature = "dev-tools")]
pub fn next_value(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<String, String> {
    let Some(value) = args.next() else {
        return Err(format!("missing value after {flag}"));
    };
    Ok(value.to_string_lossy().into_owned())
}

/// Read and parse the next flag value from an iterator.
///
/// # Errors
///
/// Returns an error if the flag has no following value or if parsing fails.
#[cfg(feature = "dev-tools")]
pub fn next_parsed<T>(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    let value = next_value(args, flag)?;
    value
        .parse()
        .map_err(|err| format!("invalid {flag} value: {err}"))
}
