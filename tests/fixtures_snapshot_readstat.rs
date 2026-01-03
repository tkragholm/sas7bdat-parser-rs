#![allow(clippy::pedantic)]
use std::path::Path;

use datatest_stable::Result;

#[path = "fixtures_snapshot_util/mod.rs"]
mod fixtures_snapshot_util;
#[path = "reference.rs"]
mod reference;

use fixtures_snapshot_util::{
    absolute_path, collect_readstat_snapshot, collect_snapshot, readstat_available, should_skip,
};
use reference::compare_snapshots;

fn verify_fixture(path: &Path) -> Result<()> {
    if !readstat_available() {
        return Ok(());
    }

    let absolute = absolute_path(path);
    if should_skip(&absolute) {
        return Ok(());
    }

    let snapshot = collect_snapshot(&absolute);
    let reference_snapshot = collect_readstat_snapshot(&absolute);
    compare_snapshots("readstat-cli", &absolute, &snapshot, &reference_snapshot);

    Ok(())
}

datatest_stable::harness! {{
    test = verify_fixture,
    root = "fixtures/raw_data",
    pattern = r"(?i)^.*\.sas7bdat$"
}}
