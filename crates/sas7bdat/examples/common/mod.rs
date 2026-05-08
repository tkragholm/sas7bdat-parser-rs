#![allow(dead_code)]

use sas7bdat::discover_fixture_paths;
use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

pub fn discover_target_roots(fixtures_root: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let Ok(entries) = fs::read_dir(fixtures_root) else {
        return roots;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name == "raw_data" {
            continue;
        }
        roots.push(path);
    }
    roots.sort();
    roots
}

pub fn discover_target_paths(min_size_bytes: u64) -> Vec<PathBuf> {
    let fixtures_root = fixture_root();
    let roots = discover_target_roots(&fixtures_root);

    let mut files = discover_fixture_paths(&roots).unwrap_or_default();
    files.sort();
    files.retain(|path| fs::metadata(path).is_ok_and(|meta| meta.len() >= min_size_bytes));
    files
}

pub fn next_arg(
    args: &mut impl Iterator<Item = std::ffi::OsString>,
    help: &str,
) -> Result<String, String> {
    args.next()
        .map(|s| s.to_string_lossy().into_owned())
        .ok_or_else(|| help.to_owned())
}
