#![allow(clippy::pedantic)]
use std::path::Path;

use datatest_stable::Result;

#[path = "fixtures_snapshot_util/mod.rs"]
mod fixtures_snapshot_util;
#[path = "reference.rs"]
mod reference;

use fixtures_snapshot_util::{absolute_path, collect_snapshot, should_skip};
use reference::{
    compare_snapshots, load_reference_snapshot, normalized_relative_path, reference_snapshot_path,
};

fn verify_fixture(path: &Path) -> Result<()> {
    if std::env::var_os("SAS7BDAT_PYREADSTAT_SNAPSHOTS").is_none() {
        return Ok(());
    }

    let absolute = absolute_path(path);
    if should_skip(&absolute) {
        return Ok(());
    }

    match load_reference_snapshot("pyreadstat", &absolute) {
        Ok(Some(reference_snapshot)) => {
            let snapshot = collect_snapshot(&absolute);
            compare_snapshots("pyreadstat", &absolute, &snapshot, &reference_snapshot);
        }
        Ok(None) => {
            panic!(
                "missing pyreadstat reference snapshot for {} (expected at {})",
                normalized_relative_path(&absolute),
                reference_snapshot_path("pyreadstat", &absolute).display()
            );
        }
        Err(err) => {
            panic!(
                "failed to load pyreadstat reference snapshot for {}: {err}",
                normalized_relative_path(&absolute)
            );
        }
    }

    Ok(())
}

datatest_stable::harness! {{
    test = verify_fixture,
    root = "fixtures/raw_data",
    pattern = r"(?i)^.*\.sas7bdat$"
}}
