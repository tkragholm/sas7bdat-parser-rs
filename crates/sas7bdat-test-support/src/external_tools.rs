use crate::common;
use std::{path::PathBuf, process::Command, sync::OnceLock};
use tempfile::TempDir;

pub struct SnapshotDir {
    _temp: TempDir,
    pub base: PathBuf,
}

fn python_bin() -> String {
    std::env::var("PYTHON_BIN").unwrap_or_else(|_| "python3".to_string())
}

fn rscript_bin() -> String {
    std::env::var("RSCRIPT_BIN")
        .or_else(|_| std::env::var("R_BIN"))
        .unwrap_or_else(|_| "Rscript".to_string())
}

fn command_available(bin: &str, args: &[&str]) -> bool {
    Command::new(bin)
        .args(args)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn run_checked(mut cmd: Command, label: &str) {
    let output = cmd.output().unwrap_or_else(|err| {
        panic!("failed to run {label}: {err}");
    });
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "{label} failed: status {:?}\nstdout: {}\nstderr: {}",
            output.status, stdout, stderr
        );
    }
}

pub fn python_snapshots() -> Option<&'static SnapshotDir> {
    static SNAPSHOTS: OnceLock<Option<SnapshotDir>> = OnceLock::new();
    SNAPSHOTS
        .get_or_init(|| {
            let pandas = std::env::var_os("SAS7BDAT_VERIFY_PANDAS").is_some();
            let pyreadstat = std::env::var_os("SAS7BDAT_VERIFY_PYREADSTAT").is_some();
            if !pandas && !pyreadstat {
                return None;
            }

            let python = python_bin();
            if !command_available(&python, &["--version"]) {
                panic!("python executable not available: {}", python);
            }

            let temp = TempDir::new().expect("create temp dir for python snapshots");
            let script = common::repo_root()
                .join("scripts")
                .join("snapshots")
                .join("generate_pandas_dumps.py");

            let mut cmd = Command::new(&python);
            cmd.arg(script)
                .arg("--output-dir")
                .arg(temp.path())
                .arg("--parsers")
                .arg("pandas")
                .arg("pyreadstat")
                .current_dir(common::repo_root());
            run_checked(cmd, "python snapshot generator");

            let base = temp.path().to_path_buf();
            Some(SnapshotDir { _temp: temp, base })
        })
        .as_ref()
}

pub fn haven_snapshots() -> Option<&'static SnapshotDir> {
    static SNAPSHOTS: OnceLock<Option<SnapshotDir>> = OnceLock::new();
    SNAPSHOTS
        .get_or_init(|| {
            std::env::var_os("SAS7BDAT_VERIFY_HAVEN")?;

            let rscript = rscript_bin();
            if !command_available(&rscript, &["--version"]) {
                panic!("Rscript executable not available: {}", rscript);
            }

            let temp = TempDir::new().expect("create temp dir for haven snapshots");
            let script = common::repo_root()
                .join("scripts")
                .join("snapshots")
                .join("generate_haven_dumps.R");

            let mut cmd = Command::new(&rscript);
            cmd.arg(script)
                .arg("--output-dir")
                .arg(temp.path())
                .current_dir(common::repo_root());
            run_checked(cmd, "haven snapshot generator");

            let base = temp.path().to_path_buf();
            Some(SnapshotDir { _temp: temp, base })
        })
        .as_ref()
}
