use crate::cli::{ConvertArgs, RecursionMode, SinkKind};
use anyhow::{Result, bail};
use glob::glob;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// # Errors
///
/// Returns an error if an input glob is invalid.
pub fn discover_inputs(
    inputs: &[PathBuf],
    recursive: RecursionMode,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut files = Vec::new();
    for input in inputs {
        if let Some(pattern) = glob_pattern(input) {
            for entry in glob(pattern)? {
                let path = entry?;
                add_path(&mut files, &path, recursive)?;
            }
            continue;
        }

        if input.is_dir() {
            if matches!(recursive, RecursionMode::Recursive) {
                for entry in WalkDir::new(input) {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() && is_sas7bdat(path) {
                        files.push((input.clone(), path.to_path_buf()));
                    }
                }
            } else {
                for entry in std::fs::read_dir(input)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() && is_sas7bdat(&path) {
                        files.push((input.clone(), path));
                    }
                }
            }
        } else if input.is_file() {
            // An explicitly-named file: report a clear error rather than silently skipping,
            // so a typo or wrong extension doesn't surface as "no inputs were found".
            if !is_sas7bdat(input) {
                bail!("Not a .sas7bdat file: {}", input.display());
            }
            let root = input
                .parent()
                .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
            files.push((root, input.clone()));
        } else {
            bail!("File not found: {}", input.display());
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

#[must_use]
pub fn compute_output_path(root: &Path, input: &Path, args: &ConvertArgs) -> PathBuf {
    let extension = sink_extension(args.output.effective_sink());
    args.output.out_dir.as_ref().map_or_else(
        || input.with_extension(extension),
        |dir| {
            if args.output.flatten {
                let file_name = input.file_name().unwrap_or_else(|| OsStr::new("output"));
                return dir.join(PathBuf::from(file_name).with_extension(extension));
            }

            let rel = input.strip_prefix(root).unwrap_or(input);
            let mut renamed = rel.to_path_buf();
            let file = renamed
                .file_name()
                .map_or_else(|| PathBuf::from("output"), PathBuf::from);
            renamed.set_file_name(file.with_extension(extension));
            dir.join(renamed)
        },
    )
}

fn is_sas7bdat(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("sas7bdat"))
}

fn glob_pattern(input: &Path) -> Option<&str> {
    let pattern = input.to_str()?;
    if pattern.contains('*') || pattern.contains('?') || pattern.contains('[') {
        Some(pattern)
    } else {
        None
    }
}

fn add_path(
    files: &mut Vec<(PathBuf, PathBuf)>,
    path: &Path,
    recursive: RecursionMode,
) -> Result<()> {
    if path.is_dir() {
        if matches!(recursive, RecursionMode::Recursive) {
            for entry in WalkDir::new(path) {
                let entry = entry?;
                let entry_path = entry.path();
                if entry_path.is_file() && is_sas7bdat(entry_path) {
                    files.push((path.to_path_buf(), entry_path.to_path_buf()));
                }
            }
        } else {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let entry_path = entry.path();
                if entry_path.is_file() && is_sas7bdat(&entry_path) {
                    files.push((path.to_path_buf(), entry_path));
                }
            }
        }
    } else if path.is_file() && is_sas7bdat(path) {
        let root = path
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        files.push((root, path.to_path_buf()));
    }
    Ok(())
}

const fn sink_extension(sink: SinkKind) -> &'static str {
    match sink {
        SinkKind::Parquet => "parquet",
        SinkKind::Csv => "csv",
        SinkKind::Tsv => "tsv",
    }
}

/// # Errors
///
/// Returns an error if the resolved inputs are invalid for the requested output mode.
pub fn validate_convert_args(args: &ConvertArgs, discovered_inputs: usize) -> Result<()> {
    if discovered_inputs == 0 {
        bail!("no .sas7bdat inputs were found");
    }
    if args.output.out.is_some() && discovered_inputs != 1 {
        bail!("--out can only be used with a single input");
    }
    if matches!(args.output.effective_sink(), SinkKind::Parquet) && args.output.delimiter.is_some()
    {
        bail!("--delimiter only applies to CSV/TSV output");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{
        ConvertArgs, ExecutionOptions, OutputOptions, RecursionMode, UiOptions, ValidationOptions,
    };
    use crate::selection::{RowWindow, row_selection_from_window};

    fn args(sink: SinkKind, out_dir: Option<&str>, flatten: bool) -> ConvertArgs {
        ConvertArgs {
            inputs: vec![PathBuf::from("input.sas7bdat")],
            recursive: RecursionMode::Recursive,
            output: OutputOptions {
                out_dir: out_dir.map(PathBuf::from),
                out: None,
                sink: Some(sink),
                delimiter: None,
                no_header: false,
                flatten,
                overwrite: false,
                parquet_row_group_size: None,
                parquet_target_bytes: None,
                parquet_metadata: false,
            },
            execution: ExecutionOptions {
                jobs: None,
                parse_threads: None,
                fail_fast: false,
            },
            validation: ValidationOptions {
                strict_dates: false,
            },
            ui: UiOptions {
                quiet: false,
                progress: crate::cli::ProgressMode::Auto,
            },
            skip: None,
            max_rows: None,
            columns: None,
            column_indices: None,
            catalog: None,
        }
    }

    #[test]
    fn compute_output_path_flattens_when_requested() {
        let args = args(SinkKind::Parquet, Some("/tmp/out"), true);
        let output = compute_output_path(
            Path::new("/data"),
            Path::new("/data/nested/example.sas7bdat"),
            &args,
        );
        assert_eq!(output, PathBuf::from("/tmp/out/example.parquet"));
    }

    #[test]
    fn compute_output_path_preserves_relative_tree() {
        let args = args(SinkKind::Csv, Some("/tmp/out"), false);
        let output = compute_output_path(
            Path::new("/data"),
            Path::new("/data/nested/example.sas7bdat"),
            &args,
        );
        assert_eq!(output, PathBuf::from("/tmp/out/nested/example.csv"));
    }

    #[test]
    fn build_row_selection_handles_skip_and_limit() {
        let selection =
            row_selection_from_window(RowWindow::new(Some(10), Some(5)), 1_000).expect("selection");
        assert!(matches!(
            selection,
            sas7bdat::RowSelection::Range { start, end }
                if start.0 == 10 && end.0 == 15
        ));
    }

    #[test]
    fn discover_inputs_supports_globs() {
        let temp_dir = std::env::temp_dir().join("sas7bdat-cli-glob-test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).expect("temp dir");
        let path = temp_dir.join("example.sas7bdat");
        std::fs::write(&path, b"test").expect("fixture file");

        let pattern = temp_dir.join("*.sas7bdat");
        let inputs = vec![pattern];
        let files = discover_inputs(&inputs, RecursionMode::Recursive).expect("glob discovery");
        assert!(files.iter().any(|(_, input)| input == &path));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
