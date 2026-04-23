use std::{ffi::OsString, fmt::Display, process::ExitCode, str::FromStr};

/// Convert a `Result` into a process exit code with consistent stderr output.
pub fn exit_code<E>(result: Result<(), E>) -> ExitCode
where
    E: Display,
{
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

/// Run a binary after performing one-time initialization.
pub fn exit_code_with_init<E, F, I>(init: I, run: F) -> ExitCode
where
    E: Display,
    F: FnOnce() -> Result<(), E>,
    I: FnOnce(),
{
    init();
    exit_code(run())
}

/// Read the next flag value from an iterator.
///
/// # Errors
///
/// Returns an error if the flag has no following value.
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
