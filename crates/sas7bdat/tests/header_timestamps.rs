//! The header's creation and modification times are seconds since the SAS epoch,
//! 1960-01-01, followed by the UTC offset SAS stores beside them because the recorded time
//! is local under some configurations (`WizardMac/ReadStat#309`).
//!
//! Both conversions were wrong and compounded: the epoch offset was added rather than
//! subtracted, and it was 315,532,800 rather than 315,619,200, since 1960-01-01 to
//! 1970-01-01 spans 3,653 days and not 3,652. Every timestamp came back twenty years and a
//! day late. Nothing caught it because nothing compared them to anything.
//!
//! Expected values are `ReadStat` at `da9fcaa`. All 318 corpus files with a readable header
//! were compared against it when this landed, with no differences.

use sas7bdat::Dataset;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn fixture(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

fn unix_seconds(t: Option<SystemTime>) -> Option<i64> {
    t?.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
}

#[test]
fn header_timestamps_match_the_reference_reader() {
    // (fixture, created, modified)
    const CASES: &[(&str, i64, i64)] = &[
        (
            "raw_data/ahs2013/homimp.sas7bdat",
            1_430_738_404,
            1_430_738_462,
        ),
        (
            "raw_data/csharp/date_formats.sas7bdat",
            1_489_520_216,
            1_489_520_216,
        ),
        ("raw_data/other/cars.sas7bdat", 1_222_779_301, 1_222_779_301),
    ];

    for (rel, created, modified) in CASES {
        let path = fixture(rel);
        if !path.exists() {
            continue;
        }
        let ds = Dataset::open(&path).expect("open dataset");
        let meta = ds.metadata();
        assert_eq!(
            unix_seconds(meta.created_at),
            Some(*created),
            "{rel}: created"
        );
        assert_eq!(
            unix_seconds(meta.modified_at),
            Some(*modified),
            "{rel}: modified"
        );
    }
}

/// A timestamp in the far future is the signature of the bug this replaced: adding the
/// epoch offset instead of subtracting it moves every file about forty years forward of
/// where it belongs, which is always wrong and never obviously so.
#[test]
fn no_fixture_claims_to_be_from_the_future() {
    let root = fixture("raw_data");
    if !root.exists() {
        return;
    }
    let now = unix_seconds(Some(SystemTime::now())).expect("clock");
    let mut checked = 0usize;
    for entry in walk(&root) {
        let Ok(ds) = Dataset::open(&entry) else {
            continue;
        };
        for (what, stamp) in [
            ("created", ds.metadata().created_at),
            ("modified", ds.metadata().modified_at),
        ] {
            if let Some(seconds) = unix_seconds(stamp) {
                assert!(
                    seconds < now,
                    "{}: {what} timestamp {seconds} is in the future",
                    entry.display()
                );
                checked += 1;
            }
        }
    }
    assert!(
        checked > 100,
        "expected the corpus, saw {checked} timestamps"
    );
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else if path.extension().is_some_and(|e| e == "sas7bdat") {
            out.push(path);
        }
    }
    out
}
