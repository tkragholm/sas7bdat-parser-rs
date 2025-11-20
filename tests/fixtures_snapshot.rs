#![allow(clippy::pedantic)]
use std::path::Path;

use datatest_stable::Result;
use insta::assert_json_snapshot;

#[path = "fixtures_snapshot_util/mod.rs"]
mod fixtures_snapshot_util;
#[path = "reference.rs"]
mod reference;
use fixtures_snapshot_util::{absolute_path, collect_snapshot, should_skip, snapshot_name};

fn verify_fixture(path: &Path) -> Result<()> {
    let absolute = absolute_path(path);
    if should_skip(&absolute) {
        return Ok(());
    }

    let snapshot = collect_snapshot(&absolute);
    let name = snapshot_name(&absolute);
    assert_json_snapshot!(name, snapshot);
    Ok(())
}

datatest_stable::harness! {{
    test = verify_fixture,
    root = "fixtures/raw_data",
    pattern = r"(?i)^.*\.sas7bdat$"
}}
