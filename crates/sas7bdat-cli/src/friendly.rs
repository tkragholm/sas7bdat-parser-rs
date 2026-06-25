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

fn guard_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        bail!("File not found: {}", path.display());
    }
    if path.is_dir() {
        bail!("Expected a file but got a directory: {}", path.display());
    }
    Ok(())
}

fn not_a_sas_file(path: &Path, err: &sas7bdat::Error) -> anyhow::Error {
    anyhow::anyhow!(
        "'{}' doesn't look like a valid SAS7BDAT file ({err})",
        path.display()
    )
}
