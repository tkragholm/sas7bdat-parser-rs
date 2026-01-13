use datatest_stable::Result;
use sas7bdat_test_support::{
    common,
    fixtures_snapshot_util::{
        absolute_path, collect_csharp_snapshot, collect_snapshot, csharp_available, should_skip,
    },
    reference::{compare_snapshots, normalized_relative_path},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Error as IoError,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

struct CsharpReport {
    unsupported_charset: BTreeSet<String>,
    mismatches: BTreeMap<String, String>,
}

impl CsharpReport {
    const fn new() -> Self {
        Self {
            unsupported_charset: BTreeSet::new(),
            mismatches: BTreeMap::new(),
        }
    }

    fn record_unsupported(&mut self, fixture: String) {
        self.unsupported_charset.insert(fixture);
    }

    fn record_mismatch(&mut self, fixture: String, error: String) {
        self.mismatches.entry(fixture).or_insert(error);
    }
}

static CSHARP_REPORT: OnceLock<Mutex<CsharpReport>> = OnceLock::new();

fn report() -> &'static Mutex<CsharpReport> {
    CSHARP_REPORT.get_or_init(|| Mutex::new(CsharpReport::new()))
}

struct ReportGuard;
impl Drop for ReportGuard {
    fn drop(&mut self) {
        let Ok(report) = report().lock() else {
            return;
        };
        if !report.unsupported_charset.is_empty() || !report.mismatches.is_empty() {
            eprintln!("csharp comparison summary:");
            if !report.unsupported_charset.is_empty() {
                eprintln!(
                    "  unsupported character set ({}):",
                    report.unsupported_charset.len()
                );
                for fixture in &report.unsupported_charset {
                    eprintln!("    - {fixture}");
                }
            }
            if !report.mismatches.is_empty() {
                eprintln!("  mismatches ({}):", report.mismatches.len());
                for (fixture, error) in &report.mismatches {
                    eprintln!("    - {fixture}: {error}");
                }
            }
        }

        if let Some(path) = report_path(
            "SAS7BDAT_CSHARP_REPORT",
            "csharp.json",
            "SAS7BDAT_VERIFY_CSHARP",
        ) {
            let payload = serde_json::json!({
                "unsupported_charset": report.unsupported_charset,
                "mismatches": report.mismatches,
            });
            if let Err(err) = write_report(path, &payload) {
                eprintln!("failed to write csharp report: {err}");
            }
        }
    }
}

fn write_report(path: PathBuf, payload: &serde_json::Value) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(&payload).unwrap())
}

fn report_path(env_var: &str, filename: &str, verify_var: &str) -> Option<PathBuf> {
    if let Ok(path) = std::env::var(env_var)
        && !path.trim().is_empty()
    {
        return Some(PathBuf::from(path));
    }
    if std::env::var_os(verify_var).is_some() {
        return Some(
            common::repo_root()
                .join("target")
                .join("sas7bdat-reports")
                .join(filename),
        );
    }
    None
}

static _REPORT_GUARD: ReportGuard = ReportGuard;

struct PerTestReportGuard;
impl Drop for PerTestReportGuard {
    fn drop(&mut self) {
        let Ok(report) = report().lock() else {
            return;
        };
        if let Some(path) = report_path(
            "SAS7BDAT_CSHARP_REPORT",
            "csharp.json",
            "SAS7BDAT_VERIFY_CSHARP",
        ) {
            let payload = serde_json::json!({
                "unsupported_charset": report.unsupported_charset,
                "mismatches": report.mismatches,
            });
            if let Err(err) = write_report(path, &payload) {
                eprintln!("failed to write csharp report: {err}");
            }
        }
    }
}

fn verify_fixture(path: &Path) -> Result<()> {
    let _ = &_REPORT_GUARD;
    if std::env::var_os("SAS7BDAT_VERIFY_CSHARP").is_none() {
        return Ok(());
    }

    if !csharp_available() {
        return Ok(());
    }
    let _per_test_report = PerTestReportGuard;

    let absolute = absolute_path(path);
    if should_skip(&absolute) {
        return Ok(());
    }
    let normalized = normalized_relative_path(&absolute);

    let snapshot = collect_snapshot(&absolute);
    let Some(reference_snapshot) = collect_csharp_snapshot(&absolute) else {
        if let Ok(mut report) = report().lock() {
            report.record_unsupported(normalized);
        }
        return Ok(());
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        compare_snapshots("csharp", &absolute, &snapshot, &reference_snapshot);
    }));
    if let Err(payload) = result {
        let message = panic_message(&payload);
        if let Ok(mut report) = report().lock() {
            report.record_mismatch(normalized, message.clone());
        }
        if std::env::var_os("SAS7BDAT_CSHARP_FAIL_ON_MISMATCH").is_some() {
            return Err(Box::new(IoError::other(message)));
        }
        return Ok(());
    }

    Ok(())
}

datatest_stable::harness! {{
    test = verify_fixture,
    root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/raw_data"),
    pattern = r"(?i)^.*\.sas7bdat$"
}}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload.downcast_ref::<&str>().map_or_else(
        || {
            payload
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "unknown panic".to_string())
        },
        ToString::to_string,
    )
}
