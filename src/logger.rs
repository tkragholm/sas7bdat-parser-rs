use std::fs::File;
use std::io::{Result as IoResult, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

static LOG_FILE: OnceLock<Arc<Mutex<File>>> = OnceLock::new();

/// Configures a log file for warnings/errors emitted by the converter.
///
/// # Errors
///
/// Returns an error if the log file cannot be created.
pub fn set_log_file(path: &Path) -> IoResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    // Ignore error if already set; keep the first writer.
    let _ = LOG_FILE.set(Arc::new(Mutex::new(file)));
    Ok(())
}

pub fn log_warn(message: &str) {
    eprintln!("{message}");
    if let Some(writer) = LOG_FILE.get()
        && let Ok(mut file) = writer.lock() {
            let _ = writeln!(file, "warning: {message}");
        }
}

pub fn log_error(message: &str) {
    eprintln!("{message}");
    if let Some(writer) = LOG_FILE.get()
        && let Ok(mut file) = writer.lock() {
            let _ = writeln!(file, "error: {message}");
        }
}
