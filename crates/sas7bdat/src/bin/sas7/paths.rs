use crate::cli::{ConvertArgs, SinkKind};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub fn discover_inputs(inputs: &[PathBuf]) -> Vec<(PathBuf, PathBuf)> {
    let mut files = Vec::new();
    for input in inputs {
        if input.is_dir() {
            for entry in WalkDir::new(input)
                .follow_links(false)
                .into_iter()
                .filter_map(Result::ok)
            {
                let path = entry.path();
                if path.is_file() && is_sas7bdat(path) {
                    files.push((input.clone(), path.to_path_buf()));
                }
            }
        } else if input.is_file() {
            if is_sas7bdat(input) {
                let root = input
                    .parent()
                    .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
                files.push((root, input.clone()));
            }
        } else {
            // Non-existent paths are ignored; shell globbing typically expands patterns.
        }
    }
    files.sort();
    files.dedup();
    files
}

fn is_sas7bdat(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("sas7bdat"))
}

pub fn compute_output_path_unchecked(root: &Path, input: &Path, args: &ConvertArgs) -> PathBuf {
    use std::ffi::OsStr;
    let new_ext = match args.output.sink {
        SinkKind::Parquet => "parquet",
        SinkKind::Csv => "csv",
        SinkKind::Tsv => "tsv",
    };
    args.output.out_dir.as_ref().map_or_else(
        || input.with_extension(new_ext),
        |dir| {
            if args.output.flatten {
                let fname = input.file_name().unwrap_or_else(|| OsStr::new("output"));
                let renamed = PathBuf::from(fname).with_extension(new_ext);
                return dir.join(renamed);
            }

            let rel = input.strip_prefix(root).unwrap_or(input);
            let mut renamed = rel.to_path_buf();
            let file = renamed
                .file_name()
                .map_or_else(|| PathBuf::from("output"), PathBuf::from);
            renamed.set_file_name(file.with_extension(new_ext));
            dir.join(renamed)
        },
    )
}
