//! Friendly, actionable wrappers around dataset opening so non-technical users get
//! "File not found" / "doesn't look like a SAS7BDAT file" instead of parser internals.

use anyhow::{Result, bail};
use sas7bdat::{Dataset, OpenOptions};
use std::path::Path;

/// Open a dataset, mapping low-level failures to a plain-language message.
///
/// # Errors
///
/// Returns an error if the path is missing or the file isn't a readable SAS7BDAT file.
pub fn open(path: &Path) -> Result<Dataset> {
    guard_exists(path)?;
    Dataset::open(path).map_err(|err| not_a_sas_file(path, &err))
}

/// Like [`open`], but with explicit [`OpenOptions`] (used by `convert`).
///
/// # Errors
///
/// Returns an error if the path is missing or the file isn't a readable SAS7BDAT file.
pub fn open_with(path: &Path, options: OpenOptions) -> Result<Dataset> {
    guard_exists(path)?;
    Dataset::open_with(path, options).map_err(|err| not_a_sas_file(path, &err))
}

/// Open options for `io_backend`, with the declared encoding replaced by `encoding`.
///
/// # Errors
///
/// Returns an error if `encoding` is not a recognised encoding label.
pub fn open_options(
    io_backend: sas7bdat::IoBackendPreference,
    encoding: Option<&str>,
) -> Result<OpenOptions> {
    let builder = OpenOptions::builder().io_backend(io_backend);
    Ok(match encoding {
        None => builder,
        Some(label) => builder
            .encoding(label)
            .map_err(|err| anyhow::anyhow!("--encoding: {err}"))?,
    }
    .build())
}

fn guard_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        bail!("File not found: {}", path.display());
    }
    if path.is_dir() {
        bail!("Expected a file but got a directory: {}", path.display());
    }
    Ok(())
}

/// Say whose problem a failure is: the file system, a part of the format this reader
/// does not handle, a bug here, or the file itself.
fn not_a_sas_file(path: &Path, err: &sas7bdat::Error) -> anyhow::Error {
    let path = path.display();
    match err {
        sas7bdat::Error::Io(io) => anyhow::anyhow!("Could not read '{path}': {}", io.message),
        sas7bdat::Error::Unsupported(_) => anyhow::anyhow!(
            "'{path}' uses a part of the SAS7BDAT format this reader does not support ({err})"
        ),
        sas7bdat::Error::Decode(_) => anyhow::anyhow!(
            "'{path}' has text that does not decode in its encoding ({err}); \
             --encoding names another, and leaving out --strict replaces such bytes"
        ),
        sas7bdat::Error::Internal(_) => {
            anyhow::anyhow!("Reading '{path}' hit a bug in this reader ({err})")
        }
        _ => anyhow::anyhow!("'{path}' doesn't look like a valid SAS7BDAT file ({err})"),
    }
}

/// Reject a convert input before the library touches it.
///
/// Only the checks that produce a plain-language message a parser could not phrase:
/// a missing path and a directory. Everything else is the conversion's to report.
///
/// # Errors
///
/// Returns an error if the path does not exist or is a directory.
pub fn guard_convert_input(path: &Path) -> Result<()> {
    guard_exists(path)
}

/// Re-word a conversion failure that turned out to be an unreadable input.
///
/// `sas7bdat-convert` returns the parser's own error, which is precise but assumes the
/// reader knows the format. When the failure came from the parser at all, say the
/// plain thing instead; anything else (a full disk, a permission error) is passed
/// through untouched because its own message is already the useful one.
#[must_use]
pub fn explain_convert_failure(path: &Path, err: anyhow::Error) -> anyhow::Error {
    match err.downcast_ref::<sas7bdat::Error>() {
        Some(sas_err) => not_a_sas_file(path, sas_err),
        None => err,
    }
}
