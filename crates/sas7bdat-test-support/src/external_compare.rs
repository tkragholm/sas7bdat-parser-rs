use crate::{
    external_tools::SnapshotDir,
    fixtures_snapshot_util::{absolute_path, collect_snapshot, should_skip},
    reference::{
        compare_snapshots, load_reference_snapshot_from, normalized_relative_path,
        reference_snapshot_path_in,
    },
};
use datatest_stable::Result;
use std::path::Path;

pub fn verify_external_snapshot(
    path: &Path,
    env_var: &str,
    parser: &str,
    snapshots: fn() -> Option<&'static SnapshotDir>,
) -> Result<()> {
    if std::env::var_os(env_var).is_none() {
        return Ok(());
    }

    let snapshots = match snapshots() {
        Some(snapshots) => snapshots,
        None => return Ok(()),
    };

    let absolute = absolute_path(path);
    if should_skip(&absolute) {
        return Ok(());
    }

    let snapshot = collect_snapshot(&absolute);
    match load_reference_snapshot_from(&snapshots.base, parser, &absolute) {
        Ok(Some(reference_snapshot)) => {
            compare_snapshots(parser, &absolute, &snapshot, &reference_snapshot);
        }
        Ok(None) => {
            panic!(
                "missing {parser} reference snapshot for {} (expected at {})",
                normalized_relative_path(&absolute),
                reference_snapshot_path_in(&snapshots.base, parser, &absolute).display()
            );
        }
        Err(err) => {
            panic!(
                "failed to load {parser} reference snapshot for {}: {err}",
                normalized_relative_path(&absolute)
            );
        }
    }

    Ok(())
}
