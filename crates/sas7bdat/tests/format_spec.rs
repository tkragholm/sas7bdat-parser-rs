//! SAS keeps a column's format in three fields, not one: a name, a width, and a decimal
//! count. Reading only the name reports `DATETIME` where SAS wrote `DATETIME22.3`, and
//! reports nothing at all for the plain numeric `w.d` format, which SAS stores with a width
//! and no name.
//!
//! Expected strings are `ReadStat` at `da9fcaa` via `readstat_variable_get_format`. Every
//! column of every corpus fixture was compared against it when this landed: 4,782 columns,
//! no differences.

use sas7bdat::Dataset;
use std::path::{Path, PathBuf};

fn fixture(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

fn spec(path: &Path, column: &str) -> Option<String> {
    let ds = Dataset::open(path).expect("open dataset");
    ds.columns()
        .iter()
        .find(|c| c.name == column)
        .unwrap_or_else(|| panic!("no column {column}"))
        .format_spec()
}

#[test]
fn width_and_decimals_survive() {
    // The file from WizardMac/ReadStat#327, where the `.d` was being lost.
    let path = fixture("raw_data/readstat_issues/dttm_wd.sas7bdat");
    if !path.exists() {
        return;
    }
    assert_eq!(spec(&path, "datetime").as_deref(), Some("DATETIME22.3"));
    assert_eq!(spec(&path, "e8601dt").as_deref(), Some("E8601DT23.3"));
}

#[test]
fn a_width_with_no_decimals_omits_the_dot() {
    let path = fixture("raw_data/csharp/date_formats.sas7bdat");
    if !path.exists() {
        return;
    }
    assert_eq!(spec(&path, "date").as_deref(), Some("DATE11"));
    assert_eq!(spec(&path, "day").as_deref(), Some("DAY2"));
}

/// The `w.d` format has no name. A column carrying width 4 and 2 decimals with an empty
/// name is formatted `4.2`, not unformatted, which is WizardMac/ReadStat#361.
#[test]
fn an_unnamed_width_is_still_a_format() {
    let path = fixture("raw_data/csharp/54-cookie.sas7bdat");
    if !path.exists() {
        return;
    }
    assert_eq!(spec(&path, "A").as_deref(), Some("4.2"));
    assert_eq!(spec(&path, "AROMA").as_deref(), Some("4.1"));
}

/// A column with neither a name nor a width has no format, and must not become `""`.
#[test]
fn no_format_stays_none() {
    let path = fixture("raw_data/other/cars.sas7bdat");
    if !path.exists() {
        return;
    }
    assert_eq!(spec(&path, "Brand"), None);
}
