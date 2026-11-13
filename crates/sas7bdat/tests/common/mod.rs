//! Where the integration tests find their SAS files.
//!
//! The full corpus is untracked, under `fixtures/raw_data/` at the workspace root (see
//! `fixtures/README.md`). A subset is tracked in `tests/fixtures/corpus/` and listed in its
//! `MANIFEST.tsv`, so that continuous integration runs the tests that use those files.
//! [`fixture`] takes a path in the corpus and falls back to the tracked copy.
//!
//! A file the manifest lists but the repository does not hold is a panic, not a skip: the
//! tests that use it would otherwise pass without running.
//!
//! `SAS7BDAT_TRACKED_ONLY=1` ignores the local corpus, which shows locally what continuous
//! integration runs.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

fn crate_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// The directory of tracked test files, with `MANIFEST.tsv` in it.
pub fn tracked_root() -> PathBuf {
    crate_dir().join("tests/fixtures/corpus")
}

/// The paths `MANIFEST.tsv` lists, relative to [`tracked_root`].
pub fn manifest_paths() -> Vec<String> {
    let text = std::fs::read_to_string(tracked_root().join("MANIFEST.tsv")).expect("MANIFEST.tsv");
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("path\t") && !line.is_empty())
        .filter_map(|line| line.split('\t').next().map(str::to_owned))
        .collect()
}

/// The tracked copy of a corpus path, if the repository keeps one.
fn tracked_copy(rel: &str) -> Option<PathBuf> {
    let (dir, name) = rel.strip_prefix("raw_data/")?.split_once('/')?;
    match dir {
        "pandas" => Some(tracked_root().join("pandas").join(name)),
        // The corpus keeps these under the C# reader that redistributes them. They are
        // Parso's test files, and are tracked under that name.
        "csharp" => Some(tracked_root().join("parso").join(name)),
        // pyreadstat's files, tracked with the R package.
        "readstat" => Some(crate_dir().join("../r-plugin/inst/extdata").join(name)),
        _ => None,
    }
}

/// `fixtures/<rel>` from the local corpus, or else its tracked copy.
///
/// When neither exists the corpus path is returned, so a test's own `exists()` check
/// skips it as before.
///
/// # Panics
///
/// If `rel` is a file the manifest lists and the tracked copy is missing.
pub fn fixture(rel: &str) -> PathBuf {
    let local = crate_dir().join("../../fixtures").join(rel);
    let tracked_only = std::env::var_os("SAS7BDAT_TRACKED_ONLY").is_some();
    if local.exists() && !tracked_only {
        return local;
    }
    if let Some(copy) = tracked_copy(rel) {
        if copy.exists() {
            return copy;
        }
        if let Ok(listed) = copy.strip_prefix(tracked_root()) {
            let listed = listed.to_string_lossy().replace('\\', "/");
            assert!(
                !manifest_paths().contains(&listed),
                "{listed} is listed in MANIFEST.tsv but is not in {}",
                tracked_root().display()
            );
        }
    }
    if tracked_only {
        // A path that does not exist, so the caller skips as continuous integration would.
        return tracked_root().join("not-tracked").join(rel);
    }
    local
}
